package setddblock

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	retry "github.com/shogo82148/go-retry"
)

type dynamoDBService struct {
	client *dynamodb.Client
	logger *slog.Logger
}

func newDynamoDBService(opts *Options) (*dynamoDBService, error) {
	if opts.Region == "" {
		opts.Region = os.Getenv("AWS_DEFAULT_REGION")
		if opts.Region == "" {
			opts.Region = os.Getenv("AWS_REGION")
		}
	}
	awsOpts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithRegion(opts.Region),
	}
	dynamoDBOpts := []func(*dynamodb.Options){}
	if opts.Endpoint != "" {
		dynamoDBOpts = append(dynamoDBOpts, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		})
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(opts.ctx, awsOpts...)
	if err != nil {
		return nil, err
	}
	return &dynamoDBService{
		client: dynamodb.NewFromConfig(awsCfg, dynamoDBOpts...),
		logger: opts.Logger,
	}, nil
}

var checkTableRetryPolicy = retry.Policy{
	MinDelay: 200 * time.Millisecond,
	MaxDelay: 2 * time.Second,
	MaxCount: 10,
}

func (svc *dynamoDBService) waitLockTableActive(ctx context.Context, tableName string) error {
	retrier := checkTableRetryPolicy.Start(ctx)
	var err error
	var exists bool
	for retrier.Continue() {
		exists, err = svc.LockTableExists(ctx, tableName)
		if err == nil && exists {
			return nil
		}
		svc.logger.DebugContext(ctx, "retry lock table exists untile table active", slog.String("table_name", tableName))
	}
	if err == nil {
		return fmt.Errorf("table not active")
	}
	return fmt.Errorf("table not active: %w", err)
}

func (svc *dynamoDBService) LockTableExists(ctx context.Context, tableName string) (bool, error) {
	table, err := svc.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	})
	if err != nil {
		if strings.Contains(err.Error(), "ResourceNotFoundException") {
			return false, nil
		}
		return false, err
	}
	svc.logger.DebugContext(ctx, "table status", slog.String("table_name", tableName), slog.String("status", string(table.Table.TableStatus)))
	if table.Table.TableStatus == types.TableStatusActive || table.Table.TableStatus == types.TableStatusUpdating {
		return true, nil
	}
	return false, nil
}

func (svc *dynamoDBService) CreateLockTable(ctx context.Context, tableName string) error {
	svc.logger.DebugContext(ctx, "try create table", slog.String("table_name", tableName))
	output, err := svc.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &tableName,
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("ID"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("ID"),
				KeyType:       types.KeyTypeHash,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		if strings.Contains(err.Error(), "ResourceInUseException") {
			if err := svc.waitLockTableActive(ctx, tableName); err != nil {
				return err
			}
			return nil
		}
		return err
	}
	svc.logger.DebugContext(ctx, "success create table", slog.String("table_arn", *output.TableDescription.TableArn))
	if err := svc.waitLockTableActive(ctx, tableName); err != nil {
		return err
	}
	svc.logger.DebugContext(ctx, "try update time to live", slog.String("table_name", tableName))
	_, err = svc.client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: &tableName,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("ttl"),
			Enabled:       aws.Bool(true),
		},
	})
	if err != nil {
		return err
	}
	svc.logger.DebugContext(ctx, "success update time to live", slog.String("table_name", tableName))
	return nil
}

type lockInput struct {
	TableName     string
	ItemID        string
	Revision      string
	PrevRevision  *string
	LeaseDuration time.Duration
}

func (parms *lockInput) String() string {
	prevRevision := "<nil>"
	if parms.PrevRevision != nil {
		prevRevision = *parms.PrevRevision
	}
	return fmt.Sprintf(
		"item_id=%s, lease_duration=%s, revision=%s, prev_revision=%s",
		parms.ItemID,
		parms.LeaseDuration,
		parms.Revision,
		prevRevision,
	)
}

func (parms *lockInput) caluTime() (time.Time, time.Time) {
	nextHeartbeatLimit := time.Now().Add(parms.LeaseDuration)
	ttl := nextHeartbeatLimit.Add(parms.LeaseDuration / 2).Truncate(time.Second).Add(time.Second)
	return nextHeartbeatLimit, ttl
}

func (parms *lockInput) Item() (map[string]types.AttributeValue, time.Time) {
	nextHeartbeatLimit, ttl := parms.caluTime()
	return map[string]types.AttributeValue{
		"ID": &types.AttributeValueMemberS{
			Value: parms.ItemID,
		},
		"LeaseDuration": &types.AttributeValueMemberN{
			Value: strconv.FormatInt(parms.LeaseDuration.Milliseconds(), 10),
		},
		"Revision": &types.AttributeValueMemberS{
			Value: parms.Revision,
		},
		"ttl": &types.AttributeValueMemberN{
			Value: strconv.FormatInt(ttl.Unix(), 10),
		},
	}, nextHeartbeatLimit
}

type lockOutput struct {
	LockGranted        bool
	LeaseDuration      time.Duration
	NextHeartbeatLimit time.Time
	Revision           string
}

var (
	errMaybeRaceDeleted = errors.New("maybe race")
)

func (output *lockOutput) String() string {
	return fmt.Sprintf(
		"lock_granted=%v, lease_duration=%s, revision=%s, next_heartbeat_limit=%s",
		output.LockGranted,
		output.LeaseDuration,
		output.Revision,
		output.NextHeartbeatLimit,
	)
}

func (svc *dynamoDBService) AquireLock(ctx context.Context, parms *lockInput) (*lockOutput, error) {
	svc.logger.DebugContext(ctx, "AquireLock", slog.Any("params", parms))
	var ret *lockOutput
	var err error
	if parms.PrevRevision == nil {
		ret, err = svc.putItemForLock(ctx, parms)
	} else {
		ret, err = svc.updateItemForLock(ctx, parms)
	}
	if err == nil {
		return ret, nil
	}
	if err != errMaybeRaceDeleted {
		return nil, err
	}
	retrier := retryPolicy.Start(ctx)
	for retrier.Continue() {
		svc.logger.DebugContext(ctx, "race retry put item or get item")
		ret, err = svc.putItemForLock(ctx, parms)
		if err != errMaybeRaceDeleted {
			return ret, err
		}
	}
	return nil, err
}

func (svc *dynamoDBService) putItemForLock(ctx context.Context, parms *lockInput) (*lockOutput, error) {
	item, nextHeartbeatLimit := parms.Item()
	svc.logger.DebugContext(ctx, "try put item to ddb")
	_, err := svc.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           &parms.TableName,
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(ID)"),
	})
	if err == nil {
		svc.logger.DebugContext(ctx, "lock granted")
		return &lockOutput{
			LockGranted:        true,
			LeaseDuration:      parms.LeaseDuration,
			NextHeartbeatLimit: nextHeartbeatLimit.Truncate(time.Millisecond),
			Revision:           parms.Revision,
		}, nil
	}
	if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
		svc.logger.DebugContext(ctx, "not lock granted")
		return svc.getItemForLock(ctx, parms)
	}
	return nil, err
}

func (svc *dynamoDBService) getItemForLock(ctx context.Context, parms *lockInput) (*lockOutput, error) {
	svc.logger.DebugContext(ctx, "try get item", slog.String("table_name", parms.TableName), slog.String("item_id", parms.ItemID))
	output, err := svc.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &parms.TableName,
		Key: map[string]types.AttributeValue{
			"ID": &types.AttributeValueMemberS{
				Value: parms.ItemID,
			},
		},

		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	svc.logger.DebugContext(ctx, "get item success")
	n, ok := readAttributeValueMemberN(output.Item, "LeaseDuration")
	if !ok {
		return nil, errMaybeRaceDeleted
	}
	leaseDuration := time.Duration(n) * time.Millisecond
	revision, ok := readAttributeValueMemberS(output.Item, "Revision")
	if !ok || revision == "" {
		return nil, errMaybeRaceDeleted
	}

	return &lockOutput{
		LockGranted:        false,
		LeaseDuration:      leaseDuration,
		Revision:           revision,
		NextHeartbeatLimit: time.Now().Add(leaseDuration).Truncate(time.Millisecond),
	}, nil
}

func readAttributeValueMemberN(item map[string]types.AttributeValue, key string) (int64, bool) {
	v, ok := item[key]
	if !ok {
		return 0, false
	}
	n, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseInt(n.Value, 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

func readAttributeValueMemberS(item map[string]types.AttributeValue, key string) (string, bool) {
	v, ok := item[key]
	if !ok {
		return "", false
	}
	s, ok := v.(*types.AttributeValueMemberS)
	if !ok {
		return "", false
	}
	return s.Value, true
}

func (svc *dynamoDBService) updateItemForLock(ctx context.Context, parms *lockInput) (*lockOutput, error) {
	svc.logger.DebugContext(ctx, "try update item to ddb")
	ret, err := svc.updateItem(ctx, parms)
	if err == nil {
		svc.logger.DebugContext(ctx, "success update item to ddb")
		svc.logger.DebugContext(ctx, "lock granted")
		return ret, nil
	}
	if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
		svc.logger.DebugContext(ctx, "not lock granted")
		return svc.getItemForLock(ctx, parms)
	}
	return nil, err
}

func (svc *dynamoDBService) updateItem(ctx context.Context, parms *lockInput) (*lockOutput, error) {
	item, nextHeartbeatLimit := parms.Item()
	_, err := svc.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &parms.TableName,
		Key: map[string]types.AttributeValue{
			"ID": &types.AttributeValueMemberS{
				Value: parms.ItemID,
			},
		},
		UpdateExpression:    aws.String("SET #LeaseDuration=:LeaseDuration,#Revision=:Revision,#ttl=:ttl"),
		ConditionExpression: aws.String("attribute_not_exists(ID) OR Revision=:PrevRevision"),
		ExpressionAttributeNames: map[string]string{
			"#LeaseDuration": "LeaseDuration",
			"#Revision":      "Revision",
			"#ttl":           "ttl",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":LeaseDuration": item["LeaseDuration"],
			":Revision":      item["Revision"],
			":ttl":           item["ttl"],
			":PrevRevision": &types.AttributeValueMemberS{
				Value: *parms.PrevRevision,
			},
		},
	})
	if err == nil {
		return &lockOutput{
			LockGranted:        true,
			LeaseDuration:      parms.LeaseDuration,
			NextHeartbeatLimit: nextHeartbeatLimit.Truncate(time.Millisecond),
			Revision:           parms.Revision,
		}, nil
	}
	return nil, err
}

var retryPolicy = retry.Policy{
	MinDelay: 10 * time.Millisecond,
	MaxDelay: 500 * time.Millisecond,
	MaxCount: 10,
}

func (svc *dynamoDBService) SendHeartbeat(ctx context.Context, parms *lockInput) (*lockOutput, error) {
	svc.logger.DebugContext(ctx, "sendHeartbeat", slog.Any("params", parms))
	if parms.PrevRevision == nil {
		return nil, errors.New("prev revision is must need")
	}
	retrier := retryPolicy.Start(ctx)
	var err error
	var ret *lockOutput
	for retrier.Continue() {
		ret, err = svc.updateItem(ctx, parms)
		if err == nil {
			return ret, nil
		}
		svc.logger.WarnContext(ctx, "send heartbeat failed retrying", slog.Any("params", parms), slog.String("detail", err.Error()))
	}
	return nil, fmt.Errorf("heartbeet failed: %w", err)
}

func (svc *dynamoDBService) ReleaseLock(ctx context.Context, parms *lockInput) error {
	if parms.PrevRevision == nil {
		return errors.New("prev revision is must need")
	}
	retrier := retryPolicy.Start(ctx)
	var err error
	for retrier.Continue() {
		err = svc.deleteItemForUnlock(ctx, parms)
		if err == nil {
			return nil
		}
		svc.logger.WarnContext(ctx, "release lock failed retrying", slog.Any("params", parms), slog.String("detail", err.Error()))
	}
	return fmt.Errorf("release lock failed: %w", err)
}

func (svc *dynamoDBService) deleteItemForUnlock(ctx context.Context, parms *lockInput) error {
	svc.logger.DebugContext(ctx, "try delete item to ddb")
	_, err := svc.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &parms.TableName,
		Key: map[string]types.AttributeValue{
			"ID": &types.AttributeValueMemberS{
				Value: parms.ItemID,
			},
		},
		ConditionExpression: aws.String("attribute_exists(ID) AND Revision=:PrevRevision"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":PrevRevision": &types.AttributeValueMemberS{
				Value: *parms.PrevRevision,
			},
		},
	})
	if err == nil {
		svc.logger.DebugContext(ctx, "success delete item to ddb")
		return nil
	}
	if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
		return nil
	}
	return err
}
