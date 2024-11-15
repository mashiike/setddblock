// locker-ttl_test.go

package setddblock_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
  "log"


	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/mashiike/setddblock"
	"github.com/stretchr/testify/require"
  "github.com/fujiwara/logutils"

)

/*
TestTTLExpirationLock aims to verify that a DynamoDB-based lock expires as expected based on its TTL.
The test follows these steps:
1. Acquire an initial lock with a defined TTL (5 seconds).
   - This lock is created using `DynamoDBLocker` and is intentionally left "unreleased" by killing the process to simulate a process crash.
2. Check the lock's `TTL` and `Revision` attributes directly in DynamoDB.
   - We use the AWS SDK to confirm the lock's TTL and verify that DynamoDB has recorded it.
3. Continuously attempt to acquire the same lock before the TTL expires.
   - Each acquisition attempt should fail until the TTL expires, confirming the lock is held until DynamoDB releases it.
4. Once the TTL expires, validate that the lock can now be reacquired.
   - This confirms that the time it took to reacquire the lock matches or exceeds the expected TTL, showing that the lock was released due to TTL expiration.
*/

func getItemDetails(client *dynamodb.Client, tableName, itemID string) (int64, string, error) {
	result, err := client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"ID": &types.AttributeValueMemberS{Value: itemID},
		},
	})
	if err != nil {
		return 0, "", fmt.Errorf("failed to query DynamoDB: %w", err)
	}

	var ttl int64
	if ttlAttr, ok := result.Item["ttl"].(*types.AttributeValueMemberN); ok {
		ttl, err = strconv.ParseInt(ttlAttr.Value, 10, 64)
		if err != nil {
			return 0, "", fmt.Errorf("failed to parse TTL attribute: %w", err)
		}
	} else {
		return 0, "", fmt.Errorf("TTL attribute is missing or has an unexpected type")
	}

	revision := ""
	if revisionAttr, ok := result.Item["Revision"].(*types.AttributeValueMemberS); ok {
		revision = revisionAttr.Value
	} else {
		return 0, "", fmt.Errorf("Revision attribute is missing or has an unexpected type")
	}

	return ttl, revision, nil
}


func setupDynamoDBClient(t *testing.T) *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithEndpointResolverWithOptions(
		aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == dynamodb.ServiceID {
				return aws.Endpoint{URL: dynamoDBURL}, nil
			}
			return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
		}),
	))
	require.NoError(t, err, "Failed to load AWS SDK config")
	return dynamodb.NewFromConfig(cfg)
}



// toggle --debug style logging for setddblock
// var enableDebug = false
var enableDebug = true



func tryAcquireLock(t *testing.T, logger *log.Logger, retryCount int) (bool, time.Time) {
    options := []func(*setddblock.Options){
        setddblock.WithEndpoint(dynamoDBURL),
        setddblock.WithLeaseDuration(5 * time.Second),
        setddblock.WithDelay(false),
        setddblock.WithNoPanic(),
    }
    if enableDebug {
        options = append(options, setddblock.WithLogger(logger))
    }
    locker, err := setddblock.New(
        fmt.Sprintf("ddb://%s/%s", lockTableName, lockItemID),
        options...,
    )
    require.NoError(t, err, "Failed to create locker for retry")

    locker.Lock()

    // I'm on the fence here - use 1 second precision or actual.
    // the original ttl isn't a subsecond timestamp, so when we calculate it, it's not "real"
    if locker.LastErr() == nil {
        return true, time.Now() // Capture time of acquisition
				// Capture acquisition time with whole-second precision
		    // return true, time.Now().Truncate(time.Second)

    }
    return false, time.Time{}
		// Capture acquisition time with whole-second precision
    // return false, time.Now().Truncate(time.Second)
}



const (
	leaseDuration   = 10 * time.Second
	retryInterval   = 1 * time.Second
	maxRetries      = 100
	dynamoDBURL     = "http://localhost:8000"
	lockItemID      = "lock_item_id"
	lockTableName   = "test"
)

func setupLogger() *log.Logger {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"debug", "warn", "error"},
		MinLevel: "warn",
		Writer:   os.Stdout,
	}
	if enableDebug {
		filter.MinLevel = "debug"
	}
	logger.SetOutput(filter)
	return logger
}

func acquireInitialLock(logger *log.Logger) {
	locker, err := setddblock.New(
		fmt.Sprintf("ddb://%s/%s", lockTableName, lockItemID),
		setddblock.WithEndpoint(dynamoDBURL),
		setddblock.WithLeaseDuration(leaseDuration),
		setddblock.WithDelay(false),
		setddblock.WithNoPanic(),
	)
	if enableDebug {
		locker, err = setddblock.New(
			fmt.Sprintf("ddb://%s/%s", lockTableName, lockItemID),
			setddblock.WithEndpoint(dynamoDBURL),
			setddblock.WithLeaseDuration(leaseDuration),
			setddblock.WithDelay(false),
			setddblock.WithNoPanic(),
			setddblock.WithLogger(logger),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create locker: %v\n", err)
			os.Exit(1)
		}
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create locker: %v\n", err)
		os.Exit(1)
	}
	locker.Lock()
	fmt.Printf("[%s] Initial lock acquired; simulating lock hold indefinitely.\n", time.Now().Format(time.RFC3339))



	select {} // Keep the process alive to simulate a lock hold
}

// Test function with process forking and cleanup
func TestTTLExpirationLock(t *testing.T) {

	var retryCount int
  var actualAcquiredTime time.Time // Declare `actualAcquiredTime` at the top

	// Use the debug variable to control logging level.
	// This is set to false by default but can be toggled for more verbose output.
	logger := setupLogger()

	// Load AWS SDK DynamoDB client configuration
	client := setupDynamoDBClient(t)

	// Step 1: Check if we are in the main process or the forked process
	if os.Getenv("FORKED") == "1" {
		acquireInitialLock(logger)
		return
	}

	// Step 2: Fork the process to acquire and hold the initial lock
	t.Logf("[%s] Forking process to acquire initial lock.", time.Now().Format(time.RFC3339))
	cmd := exec.Command(os.Args[0], "-test.run=TestTTLExpirationLock")
	cmd.Env = append(os.Environ(), "FORKED=1")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start(), "Failed to fork process for lock acquisition")

	// Allow the forked process time to acquire the lock
	t.Logf("[%s] Waiting for forked process to acquire lock...", time.Now().Format(time.RFC3339))
	time.Sleep(3 * time.Second)

	// Step 3: Kill the forked process to simulate a crash
	t.Logf("[%s] Killing forked process to simulate crash.", time.Now().Format(time.RFC3339))
	require.NoError(t, cmd.Process.Kill(), "Failed to kill forked process")

	// Confirm process termination
	processState, err := cmd.Process.Wait()
	if err != nil {
		t.Fatalf("Failed to confirm process termination: %v", err)
	}
	t.Logf("[%s] Forked process terminated with status: %v", time.Now().Format(time.RFC3339), processState)

	// Step 4: Log initial lock's TTL and revision from DynamoDB
	initialTTL, initialRevision, err := getItemDetails(client, lockTableName, lockItemID)
	require.NoError(t, err, "Failed to get item details")
	expireTime := time.Unix(initialTTL, 0)

	t.Logf("[%s] Initial: REVISION=%s, TTL=%d Now=%d Expires: %s",
		time.Now().Format(time.RFC3339), initialRevision, initialTTL, time.Now().Unix(), expireTime.Format(time.RFC3339))

	// Start retry loop
	for retryCount < maxRetries {
		retryCount++
		t.Logf("[%s] [Retry #%d] Attempting lock acquisition.", time.Now().Format(time.RFC3339), retryCount)

    // Capture lock status and acquisition time
    lockAcquired, acquiredTime := tryAcquireLock(t, logger, retryCount)

    if lockAcquired {
        actualAcquiredTime = acquiredTime // Capture the exact acquisition time
        t.Logf("[%s] Lock finally acquired at %d, original TTL: %d", acquiredTime.Format(time.RFC3339), acquiredTime.Unix(), expireTime.Unix())
        break
    }

		// Check TTL to ensure it's stable and not being updated
		currentTTL, currentRevision, err := getItemDetails(client, lockTableName, lockItemID)
		if err == nil {
			t.Logf("[%s] [Retry #%d] REVISION=%s, TTL=%d Now=%d Expires: %s",
				time.Now().Format(time.RFC3339), retryCount, currentRevision, currentTTL,
				time.Now().Unix(), time.Unix(currentTTL, 0).Format(time.RFC3339))

		} else {
			t.Logf("[Retry #%d] Failed to retrieve item details: %v", retryCount, err)
		}

		time.Sleep(retryInterval)
	}

	// Log duration between TTL expiration and successful lock acquisition
	timeAfterTTL := actualAcquiredTime.Sub(expireTime)
	t.Logf("[%s] Time between TTL expiration and lock acquisition: %v", time.Now().Format(time.RFC3339), timeAfterTTL)
	require.LessOrEqual(t, timeAfterTTL.Seconds(), 3.0, "Time between TTL expiration and lock acquisition should not exceed 3 seconds")
	require.GreaterOrEqual(t, actualAcquiredTime.Unix(), initialTTL, "Lock should only be acquired after TTL expiration")

}
