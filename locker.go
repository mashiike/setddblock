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

//DynamoDB Locker implements the sync.Locker interface and provides a Lock mechanism using DynamoDB.
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
}

//New returns *DynamoDBLocker
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
	}, nil
}

func (l *DynamoDBLocker) generateRevision() (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

//LockWithErr try get lock.
func (l *DynamoDBLocker) LockWithErr(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Println("[debug][setddblock] start LockWithErr")
	if l.locked {
		return errors.New("aleady lock granted")
	}
	exists, err := l.svc.LockTableExists(ctx, l.tableName)
	if err != nil {
		return err
	}
	l.logger.Printf("[debug][setddblock] lock table exists = %v", exists)
	if !exists {
		if err := l.svc.CreateLockTable(ctx, l.tableName); err != nil {
			return err
		}
		if err := l.svc.WaitLockTableActive(ctx, l.tableName); err != nil {
			return err
		}
	}
	rev, err := l.generateRevision()
	if err != nil {
		return err
	}

	l.logger.Println("[debug][setddblock] try aruire lock")
	input := &lockInput{
		TableName:     l.tableName,
		ItemID:        l.itemID,
		LeaseDuration: l.leaseDuration,
		Revision:      rev,
	}
	lcokResult, err := l.svc.AquireLock(ctx, input)
	if err != nil {
		return err
	}
	l.logger.Println("[debug][setddblock] aquire lock reqult", lcokResult)
	if !lcokResult.LockGranted && !l.delay {
		return errors.New("can not get lock")
	}
	for !lcokResult.LockGranted {
		sleepTime := time.Until(lcokResult.NextHartbeatLimit)
		l.logger.Printf("[debug][setddblock] wait for next aquire lock until %s (%s)", lcokResult.NextHartbeatLimit, sleepTime)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepTime):
		}
		input.PravRevision = &lcokResult.Revision
		input.Revision, err = l.generateRevision()
		if err != nil {
			return err
		}
		l.logger.Printf("[debug][setddblock] retry aquire lock until %s", input)
		lcokResult, err = l.svc.AquireLock(ctx, input)
		if err != nil {
			return err
		}
		l.logger.Printf("[debug][setddblock] now revision %s", lcokResult.Revision)
	}
	l.logger.Println("[debug][setddblock] success lock granted")
	l.locked = true
	l.unlockSignal = make(chan struct{})
	l.wg = sync.WaitGroup{}
	l.wg.Add(1)
	go func() {
		defer func() {
			if lcokResult != nil {
				input.PravRevision = &lcokResult.Revision
				if err := l.svc.ReleaseLock(ctx, input); err != nil {
					l.logger.Printf("[warn][setddblock] release lock failed: %s", err)
				}
			} else {
				l.logger.Printf("[warn][setddblock] lock result is nil last error: %s", l.lastError)
			}

			l.logger.Println("[debug][setddblock] finish background hartbeet")
			l.wg.Done()
		}()
		nextHartbeatTime := lcokResult.NextHartbeatLimit.Add(-time.Duration(float64(lcokResult.LeaseDuration) * 0.2))
		for {
			sleepTime := time.Until(nextHartbeatTime)
			l.logger.Printf("[debug][setddblock] wait for next hartbeet time until %s (%s)", nextHartbeatTime, sleepTime)
			select {
			case <-ctx.Done():
				return
			case <-l.unlockSignal:
				return
			case <-time.After(sleepTime):
			}
			l.logger.Println("[debug][setddblock] try send hartbeet")
			input.PravRevision = &lcokResult.Revision
			input.Revision, err = l.generateRevision()
			if err != nil {
				l.lastError = err
				l.logger.Println("[error][setddblock] generate revision failed in hartbeat: %s", err)
				continue
			}
			lcokResult, err = l.svc.SendHartbeat(ctx, input)
			if err != nil {
				l.lastError = err
				l.logger.Println("[error][setddblock] send hartbeat failed: %s", err)
				continue
			}
			nextHartbeatTime = lcokResult.NextHartbeatLimit.Add(-time.Duration(float64(lcokResult.LeaseDuration) * 0.2))
		}
	}()
	l.logger.Println("[debug][setddblock] end LockWithErr")
	return nil
}

//Lock for implements sync.Locker
func (l *DynamoDBLocker) Lock() {
	if err := l.LockWithErr(context.Background()); err != nil {
		l.bailout(err)
	}
}

//UnlockWithErr unlocks. Delete DynamoDB items
func (l *DynamoDBLocker) UnlockWithErr(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Println("[debug][setddblock] start UnlockWithErr")
	if !l.locked {
		return errors.New("not lock granted")
	}
	close(l.unlockSignal)
	l.locked = false
	l.wg.Wait()
	l.logger.Println("[debug][setddblock] end UnlockWithErr")
	return nil
}

//Unlock for implements sync.Locker
func (l *DynamoDBLocker) Unlock() {
	if err := l.UnlockWithErr(context.Background()); err != nil {
		l.bailout(err)
	}
}

func (l *DynamoDBLocker) LastError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastError
}

func (l *DynamoDBLocker) ClearLastError() {
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

//Recover for Lock() and Unlock() panic
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
