package ddblock

import (
	"context"
	"errors"
	"log"
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
	if u.Scheme != "ddb" {
		return nil, errors.New("scheme is not ddb")
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

func (l *DynamoDBLocker) LockWithErr(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Println("[debug][ddblock] start LockWithErr")
	if l.locked {
		return errors.New("aleady lock granted")
	}
	exists, err := l.svc.LockTableExists(ctx, l.tableName)
	if err != nil {
		return err
	}
	l.logger.Printf("[debug][ddblock] lock table exists = %v", exists)
	if !exists {
		if err := l.svc.CreateLockTable(ctx, l.tableName); err != nil {
			return err
		}
	}
	rev, err := l.generateRevision()
	if err != nil {
		return err
	}

	l.logger.Println("[debug][ddblock] try aruire lock")
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
	l.logger.Println("[debug][ddblock] aquire lock reqult", lcokResult)
	if !lcokResult.LockGranted && !l.delay {
		return errors.New("can not get lock")
	}
	for !lcokResult.LockGranted {
		sleepTime := time.Until(lcokResult.NextHartbeatLimit)
		l.logger.Printf("[debug][ddblock] wait for next aquire lock until %s (%s)", lcokResult.NextHartbeatLimit, sleepTime)
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
		l.logger.Printf("[debug][ddblock] retry aquire lock until %s", input)
		lcokResult, err = l.svc.AquireLock(ctx, input)
		if err != nil {
			return err
		}
		l.logger.Printf("[debug][ddblock] now revision %s", lcokResult.Revision)
	}
	l.logger.Println("[debug][ddblock] success lock granted")
	l.locked = true
	l.unlockSignal = make(chan struct{})
	l.wg = sync.WaitGroup{}
	l.wg.Add(1)
	go func() {
		defer func() {
			if lcokResult != nil {
				input.PravRevision = &lcokResult.Revision
				if err := l.svc.ReleaseLock(ctx, input); err != nil {
					l.logger.Printf("[warn][ddblock] release lock failed: %s", err)
				}
			} else {
				l.logger.Printf("[warn][ddblock] lock result is nil last error: %s", l.lastError)
			}

			l.logger.Println("[debug][ddblock] finish background hartbeet")
			l.wg.Done()
		}()
		nextHartbeatTime := lcokResult.NextHartbeatLimit.Add(-time.Duration(float64(lcokResult.LeaseDuration) * 0.2))
		for {
			sleepTime := time.Until(nextHartbeatTime)
			l.logger.Printf("[debug][ddblock] wait for next hartbeet time until %s (%s)", nextHartbeatTime, sleepTime)
			select {
			case <-ctx.Done():
				return
			case <-l.unlockSignal:
				return
			case <-time.After(sleepTime):
			}
			l.logger.Println("[debug][ddblock] try send hartbeet")
			input.PravRevision = &lcokResult.Revision
			input.Revision, err = l.generateRevision()
			if err != nil {
				l.lastError = err
				l.logger.Println("[error][ddblock] generate revision failed in hartbeat: %s", err)
				continue
			}
			lcokResult, err = l.svc.SendHartbeat(ctx, input)
			if err != nil {
				l.lastError = err
				l.logger.Println("[error][ddblock] send hartbeat failed: %s", err)
				continue
			}
			nextHartbeatTime = lcokResult.NextHartbeatLimit.Add(-time.Duration(float64(lcokResult.LeaseDuration) * 0.2))
		}
	}()
	l.logger.Println("[debug][ddblock] end LockWithErr")
	return nil
}

func (l *DynamoDBLocker) Lock() {
	if l.noPanic {
		defer func() {
			Recover(recover())
		}()
	}
	if err := l.LockWithErr(context.Background()); err != nil {
		l.bailout(err)
	}
}

func (l *DynamoDBLocker) UnlockWithErr(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.Println("[debug][ddblock] start UnlockWithErr")
	if !l.locked {
		return errors.New("not lock granted")
	}
	close(l.unlockSignal)
	l.locked = false
	l.wg.Wait()
	l.logger.Println("[debug][ddblock] end UnlockWithErr")
	return nil
}

func (l *DynamoDBLocker) Unlock() {
	if l.noPanic {
		defer func() {
			Recover(recover())
		}()
	}
	if err := l.UnlockWithErr(context.Background()); err != nil {
		l.bailout(err)
	}
}

func (l *DynamoDBLocker) LastError() error {
	return l.lastError
}

type bailoutErr struct {
	err error
}

func (l *DynamoDBLocker) bailout(err error) {
	l.lastError = err
	panic(bailoutErr{err: err})
}

func Recover(e interface{}) error {
	log.Println(e)
	if e != nil {
		b, ok := e.(bailoutErr)
		if !ok {
			panic(e)
		}
		return b.err
	}
	return nil
}
