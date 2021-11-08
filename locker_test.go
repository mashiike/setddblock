package ddblock_test

import (
	"bytes"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fatih/color"
	"github.com/fujiwara/logutils"
	"github.com/mashiike/ddblock"
	"github.com/stretchr/testify/require"
)

func TestDDBLock(t *testing.T) {
	endpoint := checkDDBLocalEndpoint(t)
	defer func() {
		err := ddblock.Recover(recover())
		require.NoError(t, err)
	}()
	var buf bytes.Buffer
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"debug", "warn", "error"},
		MinLevel: "warn",
		ModifierFuncs: []logutils.ModifierFunc{
			logutils.Color(color.FgHiBlack),
			logutils.Color(color.FgYellow),
			logutils.Color(color.FgRed, color.Bold),
		},
		Writer: &buf,
	}
	logger := log.New(filter, "", log.LstdFlags|log.Lmsgprefix)

	var wgStart, wgEnd sync.WaitGroup
	wgStart.Add(1)
	var total1, total2 int
	var lastTime1, lastTime2 time.Time
	workerNum := 10
	countMax := 10
	f1 := func(workerID int, l sync.Locker) {
		l.Lock()
		defer l.Unlock()
		t.Logf("f1 wroker_id = %d start", workerID)
		for i := 0; i < countMax; i++ {
			total1 += 1
			time.Sleep(10 * time.Millisecond)
		}
		lastTime1 = time.Now()
		t.Logf("f1 wroker_id = %d finish", workerID)
	}
	f2 := func(workerID int, l sync.Locker) {
		l.Lock()
		defer l.Unlock()
		t.Logf("f2 wroker_id = %d start", workerID)

		for i := 0; i < countMax; i++ {
			total2 += 1
			time.Sleep(20 * time.Millisecond)
		}
		lastTime2 = time.Now()

		t.Logf("f2 wroker_id = %d finish", workerID)
	}
	for i := 0; i < workerNum; i++ {
		wgEnd.Add(2)
		go func(workerID int) {
			defer wgEnd.Done()
			locker, err := ddblock.New(
				"ddb://test/item1",
				ddblock.WithDelay(true),
				ddblock.WithEndpoint(endpoint),
				ddblock.WithLeaseDuration(500*time.Millisecond),
				ddblock.WithLogger(logger),
			)
			require.NoError(t, err)
			wgStart.Wait()
			f1(workerID, locker)
		}(i + 1)
		go func(workerID int) {
			defer wgEnd.Done()
			locker, err := ddblock.New(
				"ddb://test/item2",
				ddblock.WithDelay(true),
				ddblock.WithEndpoint(endpoint),
				ddblock.WithLeaseDuration(50*time.Millisecond),
				ddblock.WithLogger(logger),
			)
			require.NoError(t, err)
			wgStart.Wait()
			f2(workerID, locker)
		}(i + 1)
	}
	wgStart.Done()
	wgEnd.Wait()
	t.Log(buf.String())
	require.EqualValues(t, workerNum*countMax, total1)
	require.EqualValues(t, workerNum*countMax, total2)
	t.Logf("f1 last = %s", lastTime1)
	t.Logf("f2 last = %s", lastTime2)
	require.True(t, lastTime1.After(lastTime2))
	require.False(t, strings.Contains(buf.String(), "[error]"))
}
func checkDDBLocalEndpoint(t *testing.T) string {
	t.Helper()
	if endpoint := os.Getenv("DYNAMODB_LOCAL_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	t.Log("ddb local endpoint not set. this test skip")
	t.SkipNow()
	return ""
}
