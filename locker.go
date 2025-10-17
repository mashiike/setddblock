// Package setddblock provides a distributed lock mechanism using DynamoDB.
package setddblock

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Songmu/flextime"
	"github.com/google/uuid"
)

// DynamoDBLocker implements the sync.Locker interface and provides a Lock mechanism using DynamoDB.
type DynamoDBLocker struct {
	mu                sync.Mutex
	lastError         error
	tableName         string
	itemID            string
	noPanic           bool
	delay             bool
	svc               *dynamoDBService
	logger            *slog.Logger
	leaseDuration     time.Duration
	expireGracePeriod time.Duration
	unlockSignal      chan struct{}
	locked            bool
	wg                sync.WaitGroup
	defaultCtx        context.Context
}

// New returns *DynamoDBLocker
func New(urlStr string, optFns ...func(*Options)) (*DynamoDBLocker, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "ddb" && u.Scheme != "dynamodb" {
		return nil, errors.New("scheme is required ddb or dynamodb")
	}
	if u.Host == "" {
		return nil, errors.New("table_name is required: ddb://<table_name>/<item_id>")
	}
	if u.Path == "" {
		return nil, errors.New("table_name is required: ddb://<table_name>/<item_id>")
	}
	tableName := u.Host
	itemID := strings.TrimPrefix(u.Path, "/")
	opts := newOptions()
	for _, optFn := range optFns {
		optFn(opts)
	}
	if opts.LeaseDuration > 10*time.Minute {
		return nil, errors.New("lease duration is so long, please set under 10 minute")
	}
	if opts.LeaseDuration < 100*time.Millisecond {
		return nil, errors.New("lease duration is so short, please set over 100 milli second")
	}
	svc, err := newDynamoDBService(opts)
	if err != nil {
		return nil, err
	}
	return &DynamoDBLocker{
		logger:            opts.Logger,
		noPanic:           opts.NoPanic,
		delay:             opts.Delay,
		tableName:         tableName,
		itemID:            itemID,
		svc:               svc,
		leaseDuration:     opts.LeaseDuration,
		expireGracePeriod: opts.ExpireGracePeriod,
		defaultCtx:        opts.ctx,
	}, nil
}

func (l *DynamoDBLocker) generateRevision() (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (l *DynamoDBLocker) acquireLock(ctx context.Context, input *lockInput) (*lockOutput, error) {
	lockResult, err := l.svc.AcquireLock(ctx, input)
	if err != nil {
		return nil, err
	}
	l.logger.DebugContext(ctx, "acquire lock result", slog.Any("result", lockResult))
	if !lockResult.LockGranted && l.expireGracePeriod > 0 {
		// Check if the existing lock has expired
		if flextime.Since(lockResult.ExpireTime) > l.expireGracePeriod {
			l.logger.WarnContext(ctx, "existing lock has expired, try to force acquire lock", slog.Time("expire_time", lockResult.ExpireTime))
			input.PrevRevision = &lockResult.Revision
			input.Revision, err = l.generateRevision()
			if err != nil {
				return nil, err
			}
			l.logger.DebugContext(ctx, "try force acquire lock", slog.Any("input", input))
			lockResult, err = l.svc.AcquireLock(ctx, input)
			if err != nil {
				return nil, err
			}
			l.logger.DebugContext(ctx, "force acquire lock result", slog.Any("result", lockResult))
		}
	}
	return lockResult, nil
}

// LockWithErr try get lock.
// The return value of bool indicates whether Lock has been released. If true, it is Lock Granted.
func (l *DynamoDBLocker) LockWithErr(ctx context.Context) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.DebugContext(ctx, "start LockWithErr")
	if l.locked {
		return true, errors.New("aleady lock granted")
	}
	exists, err := l.svc.LockTableExists(ctx, l.tableName)
	if err != nil {
		return false, err
	}
	l.logger.DebugContext(ctx, "check lock table exists", slog.Bool("exists", exists))
	if !exists {
		if err := l.svc.CreateLockTable(ctx, l.tableName); err != nil {
			return false, err
		}
	}
	rev, err := l.generateRevision()
	if err != nil {
		return false, err
	}

	l.logger.DebugContext(ctx, "try acquire lock")
	input := &lockInput{
		TableName:     l.tableName,
		ItemID:        l.itemID,
		LeaseDuration: l.leaseDuration,
		Revision:      rev,
	}
	lockResult, err := l.acquireLock(ctx, input)
	if err != nil {
		return false, err
	}
	if !lockResult.LockGranted && !l.delay {
		return false, nil
	}
	for !lockResult.LockGranted {
		sleepTime := flextime.Until(lockResult.NextHeartbeatLimit)
		l.logger.DebugContext(ctx, "wait for next acquire lock until", slog.Time("next_heartbeat_limit", lockResult.NextHeartbeatLimit), slog.Duration("sleep_time", sleepTime))
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(sleepTime):
		}
		input.PrevRevision = &lockResult.Revision
		input.Revision, err = l.generateRevision()
		if err != nil {
			return false, err
		}
		l.logger.DebugContext(ctx, "retry acquire lock until", slog.Any("input", input))
		lockResult, err = l.acquireLock(ctx, input)
		if err != nil {
			return false, err
		}
		l.logger.DebugContext(ctx, "now revision", slog.String("revision", lockResult.Revision))
	}
	l.logger.DebugContext(ctx, "success lock granted")
	l.locked = true
	l.unlockSignal = make(chan struct{})
	l.wg = sync.WaitGroup{}
	l.wg.Add(1)
	go func() {
		defer func() {
			if lockResult != nil {
				input.PrevRevision = &lockResult.Revision
				if err := l.svc.ReleaseLock(context.Background(), input); err != nil {
					l.logger.Warn("release lock failed in background heartbeat", slog.String("revision", lockResult.Revision), slog.String("detail", err.Error()))
				}
			} else {
				l.logger.Warn("lock result is nil in background heartbeat", slog.String("detail", l.lastError.Error()))
			}

			l.logger.Debug("finish background heartbeat")
			l.wg.Done()
		}()
		nextHeartbeatTime := lockResult.NextHeartbeatLimit.Add(-time.Duration(float64(lockResult.LeaseDuration) * 0.2))
		for {
			sleepTime := flextime.Until(nextHeartbeatTime)
			l.logger.Debug("wait for next heartbeat time until", slog.Time("next_heartbeat_time", nextHeartbeatTime), slog.Duration("sleep_time", sleepTime))
			select {
			case <-ctx.Done():
				return
			case <-l.unlockSignal:
				return
			case <-time.After(sleepTime):
			}
			l.logger.Debug("try send heartbeat")
			input.PrevRevision = &lockResult.Revision
			input.Revision, err = l.generateRevision()
			if err != nil {
				l.lastError = err
				l.logger.Error("generate revision failed in heartbeat", slog.String("detail", err.Error()))
				continue
			}
			lockResult, err = l.svc.SendHeartbeat(ctx, input)
			if err != nil {
				l.lastError = err
				l.logger.Error("send heartbeat failed", slog.String("detail", err.Error()))
				continue
			}
			nextHeartbeatTime = lockResult.NextHeartbeatLimit.Add(-time.Duration(float64(lockResult.LeaseDuration) * 0.2))
		}
	}()
	l.logger.Debug("end LockWithErr")
	return true, nil
}

// Lock for implements sync.Locker
func (l *DynamoDBLocker) Lock() {
	lockGranted, err := l.LockWithErr(l.defaultCtx)
	if err != nil {
		l.bailout(err)
	}
	if !lockGranted {
		l.bailout(errors.New("lock was not granted"))
	}
}

// UnlockWithErr unlocks. Delete DynamoDB items
func (l *DynamoDBLocker) UnlockWithErr(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.DebugContext(ctx, "start UnlockWithErr")
	if !l.locked {
		return errors.New("not lock granted")
	}
	close(l.unlockSignal)
	l.locked = false
	l.wg.Wait()
	l.logger.DebugContext(ctx, "end UnlockWithErr")
	return nil
}

// Unlock for implements sync.Locker
func (l *DynamoDBLocker) Unlock() {
	if err := l.UnlockWithErr(l.defaultCtx); err != nil {
		l.bailout(err)
	}
}

func (l *DynamoDBLocker) LastErr() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastError
}

func (l *DynamoDBLocker) ClearLastErr() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastError = nil
}

type bailoutErr struct {
	err error
}

func (l *DynamoDBLocker) bailout(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastError = err
	if !l.noPanic {
		panic(bailoutErr{err: err})
	}
}

// Recover for Lock() and Unlock() panic
func Recover(e interface{}) error {
	if e != nil {
		b, ok := e.(bailoutErr)
		if !ok {
			panic(e)
		}
		return b.err
	}
	return nil
}
