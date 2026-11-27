package setddblock

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeDynamoDB is a minimal DynamoDB JSON API server for unit tests.
// It holds a single lock item and lets tests inject UpdateItem failures.
type fakeDynamoDB struct {
	mu sync.Mutex
	// updateItemErrType, if non-empty, is returned as the error __type for UpdateItem.
	updateItemErrType string
	// updateItemFailures is the number of UpdateItem calls that fail before succeeding again.
	// A negative value means fail forever.
	updateItemFailures int
	updateItemCalls    int
	prevRevisions      []string
	deleteItemCalls    int
}

func (f *fakeDynamoDB) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target := r.Header.Get("X-Amz-Target")
	op := target[strings.LastIndex(target, ".")+1:]
	body, _ := io.ReadAll(r.Body)
	var req map[string]any
	_ = json.Unmarshal(body, &req)

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	f.mu.Lock()
	defer f.mu.Unlock()
	switch op {
	case "DescribeTable":
		_, _ = io.WriteString(w, `{"Table":{"TableName":"test","TableStatus":"ACTIVE"}}`)
	case "PutItem":
		_, _ = io.WriteString(w, `{}`)
	case "UpdateItem":
		f.updateItemCalls++
		if values, ok := req["ExpressionAttributeValues"].(map[string]any); ok {
			if prev, ok := values[":PrevRevision"].(map[string]any); ok {
				if s, ok := prev["S"].(string); ok {
					f.prevRevisions = append(f.prevRevisions, s)
				}
			}
		}
		if f.updateItemErrType != "" && f.updateItemFailures != 0 {
			if f.updateItemFailures > 0 {
				f.updateItemFailures--
			}
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"__type":"com.amazonaws.dynamodb.v20120810#`+f.updateItemErrType+`","message":"injected"}`)
			return
		}
		_, _ = io.WriteString(w, `{}`)
	case "DeleteItem":
		f.deleteItemCalls++
		_, _ = io.WriteString(w, `{}`)
	default:
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"__type":"com.amazonaws.dynamodb.v20120810#UnknownOperationException","message":"`+op+`"}`)
	}
}

func (f *fakeDynamoDB) stats() (updateCalls int, prevRevisions []string, deleteCalls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updateItemCalls, append([]string(nil), f.prevRevisions...), f.deleteItemCalls
}

func newFakeLocker(t *testing.T, fake *fakeDynamoDB) *DynamoDBLocker {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "dummy")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "dummy")
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	orig := retryPolicy
	retryPolicy.MaxCount = 2
	retryPolicy.MaxDelay = 10 * time.Millisecond
	t.Cleanup(func() { retryPolicy = orig })

	l, err := New("ddb://test/item",
		WithEndpoint(srv.URL),
		WithRegion("ap-northeast-1"),
		WithLeaseDuration(100*time.Millisecond),
		WithNoPanic(),
	)
	require.NoError(t, err)
	return l
}

// When the lock is taken over by another process, the heartbeat must not panic.
// It should record the error and stop sending heartbeats.
func TestHeartbeatLockLostDoesNotPanic(t *testing.T) {
	fake := &fakeDynamoDB{
		updateItemErrType:  "ConditionalCheckFailedException",
		updateItemFailures: -1,
	}
	l := newFakeLocker(t, fake)

	granted, err := l.LockWithErr(t.Context())
	require.NoError(t, err)
	require.True(t, granted)

	require.Eventually(t, func() bool { return l.LastErr() != nil }, 2*time.Second, 10*time.Millisecond)
	require.ErrorIs(t, l.LastErr(), ErrLockLost)
	select {
	case <-l.LockLost():
	case <-time.After(time.Second):
		t.Fatal("LockLost channel should be closed")
	}

	// heartbeat stops after the lock is lost
	calls, _, _ := fake.stats()
	time.Sleep(300 * time.Millisecond)
	callsAfter, _, _ := fake.stats()
	require.Equal(t, calls, callsAfter, "heartbeat should stop after the lock is lost")

	require.NoError(t, l.UnlockWithErr(t.Context()))
}

// A transient heartbeat failure must not panic, and the next heartbeat
// must continue from the last successful revision.
func TestHeartbeatTransientFailureKeepsLock(t *testing.T) {
	fake := &fakeDynamoDB{
		updateItemErrType:  "InjectedTransientException",
		updateItemFailures: 2, // fails one SendHeartbeat call (MaxCount=2), then recovers
	}
	l := newFakeLocker(t, fake)

	granted, err := l.LockWithErr(t.Context())
	require.NoError(t, err)
	require.True(t, granted)

	require.Eventually(t, func() bool {
		calls, _, _ := fake.stats()
		return calls >= 4
	}, 2*time.Second, 10*time.Millisecond)
	require.Error(t, l.LastErr(), "transient failure should be recorded")
	select {
	case <-l.LockLost():
		t.Fatal("LockLost channel should not be closed on a transient failure")
	default:
	}

	_, prevRevisions, _ := fake.stats()
	// the first two (failed) attempts and the retry after them use the same previous revision
	require.Equal(t, prevRevisions[0], prevRevisions[1])
	require.Equal(t, prevRevisions[0], prevRevisions[2])
	require.NotEqual(t, prevRevisions[2], prevRevisions[3])

	require.NoError(t, l.UnlockWithErr(t.Context()))
	_, _, deleteCalls := fake.stats()
	require.Equal(t, 1, deleteCalls, "lock should be released on unlock")
}
