package ddblock

import (
	"context"
	"errors"
	"fmt"
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
	logger Logger
}

func newDynamoDBService(opts *Options) (*dynamoDBService, error) {
	if opts.Region == "" {
		opts.Region = os.Getenv("AWS_DEFAULT_REGION")
	}
	awsOpts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithRegion(opts.Region),
	}
	if opts.Endpoint != "" {
		awsOpts = append(awsOpts, awsConfig.WithEndpointResolver(
			aws.EndpointResolverFunc(
				func(service, region string) (aws.Endpoint, error) {
					if opts.Region != "" && opts.Region != region {
						return aws.Endpoint{}, &aws.EndpointNotFoundError{}
					}
					switch service {
					case dynamodb.ServiceID:
						return aws.Endpoint{
							PartitionID:   "aws",
							URL:           opts.Endpoint,
							SigningRegion: region,
						}, nil
					}
					return aws.Endpoint{}, &aws.EndpointNotFoundError{}
				},
			),
		))
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(context.Background(), awsOpts...)
	if err != nil {
		return nil, err
	}
	return &dynamoDBService{
		client: dynamodb.NewFromConfig(awsCfg),
		logger: opts.Logger,
	}, nil
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
	if table.Table.TableStatus == types.TableStatusActive || table.Table.TableStatus == types.TableStatusUpdating {
		return true, nil
	}
	return false, nil
}

func (svc *dynamoDBService) CreateLockTable(ctx context.Context, tableName string) error {
	svc.logger.Printf("[debug][ddblock] try create table %s", tableName)
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
		return err
	}
	svc.logger.Printf("[debug][ddblock] create table %s", *output.TableDescription.TableArn)
	svc.logger.Printf("[debug][ddblock] try update time to live `%s`", tableName)
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
	svc.logger.Printf("[debug][ddblock] success update time to live `%s`", tableName)
	return nil
}

type lockInput struct {
	TableName     string
	ItemID        string
	Revision      string
	PravRevision  *string
	LeaseDuration time.Duration
}

func (parms *lockInput) String() string {
	prevRevision := "<nil>"
	if parms.PravRevision != nil {
		prevRevision = *parms.PravRevision
	}
	return fmt.Sprintf(
		"item_id=%s, lease_duration=%s, revision=%s, prav_revision=%s",
		parms.ItemID,
		parms.LeaseDuration,
		parms.Revision,
		prevRevision,
	)
}

func (parms *lockInput) caluTime() (time.Time, time.Time) {
	nextHartbeatLimit := time.Now().Add(parms.LeaseDuration)
	ttl := nextHartbeatLimit.Add(parms.LeaseDuration / 2).Truncate(time.Second).Add(time.Second)
	return nextHartbeatLimit, ttl
}

func (parms *lockInput) Item() (map[string]types.AttributeValue, time.Time) {
	nextHartbeatLimit, ttl := parms.caluTime()
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
	}, nextHartbeatLimit
}

type lockOutput struct {
	LockGranted       bool
	LeaseDuration     time.Duration
	NextHartbeatLimit time.Time
	Revision          string
}

var (
	errMaybeRaceDeleted = errors.New("maybe race")
)

func (output *lockOutput) String() string {
	return fmt.Sprintf(
		"lock_granted=%v, lease_duration=%s, revision=%s, next_hartbeat_limit=%s",
		output.LockGranted,
		output.LeaseDuration,
		output.Revision,
		output.NextHartbeatLimit,
	)
}

func (svc *dynamoDBService) AquireLock(ctx context.Context, parms *lockInput) (*lockOutput, error) {
	svc.logger.Printf("[debug][ddblock] AquireLock %s", parms)
	if parms.LeaseDuration > time.Minute {
		return nil, errors.New("lease duration is so long, please set under 1m")
	}
	var ret *lockOutput
	var err error
	if parms.PravRevision == nil {
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
		svc.logger.Printf("[debug][ddblock] race retry put item or get item")
		ret, err = svc.putItemForLock(ctx, parms)
		if err != errMaybeRaceDeleted {
			return ret, err
		}
	}
	return nil, err
}

func (svc *dynamoDBService) putItemForLock(ctx context.Context, parms *lockInput) (*lockOutput, error) {
	item, nextHartbeatLimit := parms.Item()
	svc.logger.Printf("[debug][ddblock] try put item to ddb")
	_, err := svc.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           &parms.TableName,
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(ID)"),
	})
	if err == nil {
		svc.logger.Printf("[debug][ddblock] lock granted")
		return &lockOutput{
			LockGranted:       true,
			LeaseDuration:     parms.LeaseDuration,
			NextHartbeatLimit: nextHartbeatLimit.Truncate(time.Millisecond),
			Revision:          parms.Revision,
		}, nil
	}
	if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
		svc.logger.Printf("[debug][ddblock] not lock granted")
		return svc.getItemForLock(ctx, parms)
	}
	return nil, err
}

func (svc *dynamoDBService) getItemForLock(ctx context.Context, parms *lockInput) (*lockOutput, error) {
	svc.logger.Printf("[debug][ddblock] try get item table_name=%s, item_id=%s", parms.TableName, parms.ItemID)
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
	svc.logger.Println("[debug][ddblock] get item success")
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
		LockGranted:       false,
		LeaseDuration:     leaseDuration,
		Revision:          revision,
		NextHartbeatLimit: time.Now().Add(leaseDuration).Truncate(time.Millisecond),
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
	svc.logger.Printf("[debug][ddblock] try update item to ddb")
	ret, err := svc.updateItem(ctx, parms)
	if err == nil {
		svc.logger.Printf("[debug][ddblock] lock granted")
		return ret, nil
	}
	if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
		svc.logger.Printf("[debug][ddblock] not lock granted")
		return svc.getItemForLock(ctx, parms)
	}
	return nil, err
}

func (svc *dynamoDBService) updateItem(ctx context.Context, parms *lockInput) (*lockOutput, error) {
	item, nextHartbeatLimit := parms.Item()
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
				Value: *parms.PravRevision,
			},
		},
	})
	if err == nil {
		return &lockOutput{
			LockGranted:       true,
			LeaseDuration:     parms.LeaseDuration,
			NextHartbeatLimit: nextHartbeatLimit.Truncate(time.Millisecond),
			Revision:          parms.Revision,
		}, nil
	}
	return nil, err
}

var retryPolicy = retry.Policy{
	MinDelay: 10 * time.Millisecond,
	MaxDelay: 500 * time.Millisecond,
	MaxCount: 10,
}

func (svc *dynamoDBService) SendHartbeat(ctx context.Context, parms *lockInput) (*lockOutput, error) {
	svc.logger.Printf("[debug][ddblock] sendHartbeat %s", parms)
	if parms.LeaseDuration > time.Minute {
		return nil, errors.New("lease duration is so long, please set under 1m")
	}
	if parms.PravRevision == nil {
		return nil, errors.New("prav revision is must need")
	}
	retrier := retryPolicy.Start(ctx)
	var err error
	var ret *lockOutput
	for retrier.Continue() {
		ret, err = svc.updateItem(ctx, parms)
		if err == nil {
			return ret, nil
		}
		svc.logger.Printf("[warn][ddblock] send hartbeat failed retrying %s, err=%s", parms, err)
	}
	return nil, fmt.Errorf("hartbeet failed: %w", err)
}

func (svc *dynamoDBService) ReleaseLock(ctx context.Context, parms *lockInput) error {
	if parms.PravRevision == nil {
		return errors.New("prav revision is must need")
	}
	retrier := retryPolicy.Start(ctx)
	var err error
	for retrier.Continue() {
		err = svc.deleteItemForUnlock(ctx, parms)
		if err == nil {
			return nil
		}
		svc.logger.Printf("[warn][ddblock] release lock failed retrying %s, err=%s", parms, err)
	}
	return fmt.Errorf("release lock failed: %w", err)
}

func (svc *dynamoDBService) deleteItemForUnlock(ctx context.Context, parms *lockInput) error {
	svc.logger.Printf("[debug][ddblock] try delete item to ddb")
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
				Value: *parms.PravRevision,
			},
		},
	})
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
		return nil
	}
	return err
}
