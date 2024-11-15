package setddblock

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// DynamoDBLocker implements the sync.Locker interface and provides a Lock mechanism using DynamoDB.
type DynamoDBLocker struct {
	mu            sync.Mutex
	lastError     error
	tableName     string
	itemID        string
	noPanic       bool
	delay         bool
	svc           *dynamoDBService
	logger        Logger
	leaseDuration time.Duration
	unlockSignal  chan struct{}
	locked        bool
	wg            sync.WaitGroup
	defaultCtx    context.Context
}

// GetLockDetails retrieves the lock details for the current item.
func (l *DynamoDBLocker) GetLockDetails(ctx context.Context) (*LockDetails, error) {
	return l.svc.GetLockDetails(ctx, l.tableName, l.itemID)
}

// ItemID returns the item ID of the lock.
func (l *DynamoDBLocker) ItemID() string {
	return l.itemID
}

// TableName returns the table name of the lock.
func (l *DynamoDBLocker) TableName() string {
	return l.tableName
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
		logger:        opts.Logger,
		noPanic:       opts.NoPanic,
		delay:         opts.Delay,
		tableName:     tableName,
		itemID:        itemID,
		svc:           svc,
		leaseDuration: opts.LeaseDuration,
		defaultCtx:    opts.ctx,
	}, nil
}

func (l *DynamoDBLocker) generateRevision() (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

// LockWithErr try get lock.
// The return value of bool indicates whether Lock has been released. If true, it is Lock Granted.
func (l *DynamoDBLocker) LockWithErr(ctx context.Context) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Println("[debug][setddblock] start - LockWithErr")
	if l.locked {
		return true, errors.New("aleady lock granted")
	}
	exists, err := l.svc.LockTableExists(ctx, l.tableName)
	if err != nil {
		return false, err
	}
	l.logger.Printf("[debug][setddblock] lock table exists = %v", exists)
	if !exists {
		if err := l.svc.CreateLockTable(ctx, l.tableName); err != nil {
			return false, err
		}
	}
	rev, err := l.generateRevision()
	if err != nil {
		return false, err
	}

	l.logger.Println("[debug][setddblock] try - acquire lock")
	input := &lockInput{
		TableName:     l.tableName,
		ItemID:        l.itemID,
		LeaseDuration: l.leaseDuration,
		Revision:      rev,
	}
	lockResult, err := l.svc.AcquireLock(ctx, input)
	if err != nil {
		return false, err
	}
	if lockResult == nil {
		// Lock is considered expired due to TTL
		l.logger.Printf("[debug][setddblock] lock expired due to TTL for item_id=%s, table_name=%s, current_time=%s", l.itemID, l.tableName, time.Now().Format(time.RFC3339))
		return false, nil
	}
	if lockResult == nil {
		l.logger.Printf("[debug][setddblock] acquire lock result is nil for table_name=%s, item_id=%s", l.tableName, l.itemID)
		return false, nil
	}
	if !lockResult.LockGranted && !l.delay {
		return false, nil
	}
	for !lockResult.LockGranted {
		sleepTime := time.Until(lockResult.NextHeartbeatLimit)
		l.logger.Printf("[debug][setddblock] wait for next acquire lock until %s (%s)", lockResult.NextHeartbeatLimit, sleepTime)
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
		lockResult, err = l.svc.AcquireLock(ctx, input)
		if err != nil {
			return false, err
		}
		l.logger.Printf("[debug][setddblock] now revision %s", lockResult.Revision)
	}
	l.logger.Println("[debug][setddblock] success - lock granted")
	l.locked = true
	l.unlockSignal = make(chan struct{})
	l.wg = sync.WaitGroup{}
	l.wg.Add(1)
	go func() {
		defer func() {
			if lockResult != nil {
				input.PrevRevision = &lockResult.Revision
				if err := l.svc.ReleaseLock(context.Background(), input); err != nil {
					l.logger.Printf("[warn][setddblock] release lock failed: %s", err)
				}
			} else {
				l.logger.Printf("[warn][setddblock] lock result is nil last error: %s", l.lastError)
			}

			l.logger.Printf("[debug][setddblock] finish background heartbeat for item_id=%s, table_name=%s at %s", l.itemID, l.tableName, time.Now().Format(time.RFC3339))
			l.wg.Done()
		}()
		nextHeartbeatTime := lockResult.NextHeartbeatLimit.Add(-time.Duration(float64(lockResult.LeaseDuration) * 0.2))
		for {
			sleepTime := time.Until(nextHeartbeatTime)
			l.logger.Printf("[debug][setddblock] wait for next heartbeat time for item_id=%s, table_name=%s until %s (%s) at %s", l.itemID, l.tableName, nextHeartbeatTime, sleepTime, time.Now().Format(time.RFC3339))
			select {
			case <-ctx.Done():
				return
			case <-l.unlockSignal:
				return
			case <-time.After(sleepTime):
			}
			l.logger.Println("[debug][setddblock] try send heartbeat")
			input.PrevRevision = &lockResult.Revision
			input.Revision, err = l.generateRevision()
			if err != nil {
				l.lastError = err
				l.logger.Println("[error][setddblock] generate revision failed in heartbeat: %s", err)
				continue
			}
			lockResult, err = l.svc.SendHeartbeat(ctx, input)
			if err != nil {
				l.lastError = err
				l.logger.Println("[error][setddblock] send heartbeat failed: %s", err)
				continue
			}
			nextHeartbeatTime = lockResult.NextHeartbeatLimit.Add(-time.Duration(float64(lockResult.LeaseDuration) * 0.2))
		}
	}()
	l.logger.Println("[debug][setddblock] end -LockWithErr")
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
func (l *DynamoDBLocker) UnlockWithErr(_ context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Println("[debug][setddblock] start - UnlockWithErr")
	if !l.locked {
		return errors.New("not lock granted")
	}
	close(l.unlockSignal)
	l.locked = false
	l.wg.Wait()
	l.logger.Println("[debug][setddblock] end - UnlockWithErr")
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
