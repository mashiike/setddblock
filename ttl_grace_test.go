package setddblock_test

import (
	"context"
	"testing"
	"time"

	"strconv"

	"github.com/Songmu/flextime"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/mashiike/setddblock"
	"github.com/stretchr/testify/require"
)

func ensureTableWithTTL(t *testing.T, client *dynamodb.Client, tableName string) {
	t.Helper()
	// Describe
	_, err := client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
	if err == nil {
		return
	}
	// Create
	_, err = client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("ID"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("ID"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)
	// Wait a moment for table to become active (simple sleep is fine here)
	time.Sleep(500 * time.Millisecond)
	// Enable TTL on attribute `ttl`
	_, err = client.UpdateTimeToLive(context.Background(), &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(tableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("ttl"),
			Enabled:       aws.Bool(true),
		},
	})
	require.NoError(t, err)
}

func newDynamoClient(t *testing.T, endpoint string) *dynamodb.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background())
	require.NoError(t, err)
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// Test that when ExpireGracePeriod is set and current time exceeds TTL+grace,
// we forcibly reacquire the lock on first attempt.
func TestForceAcquireAfterTTLWithGrace(t *testing.T) {
	endpoint := checkDDBLocalEndpoint(t)
	client := newDynamoClient(t, endpoint)
	tableName := "test"
	itemID := "ttl_grace_item"

	ensureTableWithTTL(t, client, tableName)

	// Fix base time
	base := time.Now().Truncate(time.Second)
	restore := flextime.Fix(base)
	t.Cleanup(func() { restore() })

	// Simulate an existing lock item with TTL in the future (base+20s)
	lease := 10 * time.Second
	ttl := base.Add(20 * time.Second).Unix()
	_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"ID":            &types.AttributeValueMemberS{Value: itemID},
			"LeaseDuration": &types.AttributeValueMemberN{Value: "10000"}, // 10s in ms
			"Revision":      &types.AttributeValueMemberS{Value: "rev-1"},
			"ttl":           &types.AttributeValueMemberN{Value: strconv.FormatInt(ttl, 10)},
		},
		ConditionExpression: aws.String("attribute_not_exists(ID)"),
	})
	require.NoError(t, err)

	// Advance time beyond TTL+grace (grace=5s => base+25s)
	flextime.Fix(base.Add(30 * time.Second))

	// Attempt to acquire with grace configured; should force acquire immediately.
	locker, err := setddblock.New(
		"ddb://"+tableName+"/"+itemID,
		setddblock.WithEndpoint(endpoint),
		setddblock.WithLeaseDuration(lease),
		setddblock.WithExpireGracePeriod(5*time.Second),
		setddblock.WithNoPanic(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	granted, err := locker.LockWithErr(ctx)
	require.NoError(t, err)
	require.True(t, granted, "lock should be forcibly reacquired after TTL+grace")
	_ = locker.UnlockWithErr(context.Background())
}

// Without ExpireGracePeriod and with Delay(false), lock should NOT be granted
// even if current time is past TTL (we don't force acquire without grace).
func TestNoForceAcquireWithoutGrace(t *testing.T) {
	endpoint := checkDDBLocalEndpoint(t)
	client := newDynamoClient(t, endpoint)
	tableName := "test"
	itemID := "ttl_nograce_item"

	ensureTableWithTTL(t, client, tableName)

	base := time.Now().Truncate(time.Second)
	restore := flextime.Fix(base)
	t.Cleanup(func() { restore() })

	// Existing lock with TTL at base+20s
	ttl := base.Add(20 * time.Second).Unix()
	_, err := client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"ID":            &types.AttributeValueMemberS{Value: itemID},
			"LeaseDuration": &types.AttributeValueMemberN{Value: "10000"},
			"Revision":      &types.AttributeValueMemberS{Value: "rev-1"},
			"ttl":           &types.AttributeValueMemberN{Value: strconv.FormatInt(ttl, 10)},
		},
		ConditionExpression: aws.String("attribute_not_exists(ID)"),
	})
	require.NoError(t, err)

	// Move time past TTL+5s
	flextime.Fix(base.Add(30 * time.Second))

	locker, err := setddblock.New(
		"ddb://"+tableName+"/"+itemID,
		setddblock.WithEndpoint(endpoint),
		setddblock.WithLeaseDuration(10*time.Second),
		setddblock.WithDelay(false), // do not wait; return immediately when not granted
		setddblock.WithNoPanic(),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	granted, err := locker.LockWithErr(ctx)
	require.NoError(t, err)
	require.False(t, granted, "without grace, should not force acquire immediately")
}
