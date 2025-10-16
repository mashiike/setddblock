# setddblock

![Latest GitHub release](https://img.shields.io/github/release/mashiike/setddblock.svg)
![Github Actions test](https://github.com/mashiike/setddblock/workflows/Test/badge.svg?branch=main)
[![Go Report Card](https://goreportcard.com/badge/mashiike/setddblock)](https://goreportcard.com/report/mashiike/setddblock) 
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/mashiike/setddblock/blob/master/LICENSE)
[![Documentation](https://godoc.org/github.com/mashiike/setddblock?status.svg)](https://godoc.org/github.com/mashiike/setddblock)

setddblock is setlock like command line tool with [AWS DynamoDB](https://aws.amazon.com/dynamodb/)

## Usage 

```console
$ setddblock -xN ddb://ddb_lock_table/lock_item_id your_command
```

```console
Usage: setddblock [ -nNxX ] [--endpoint <endpoint>] [--debug --version] ddb://<table_name>/<item_id> your_command
Flags:
  -n
        No delay. If fn is locked by another process, setlock gives up.
  -N
        (Default.) Delay. If fn is locked by another process, setlock waits until it can obtain a new lock.
  -x
        If fn cannot be update-item (or put-item) or locked, setlock exits zero.
  -X
        (Default.) If fn cannot be update-item (or put-item) or locked, setlock prints an error message and exits nonzero.
  --debug
        show debug log
  --endpoint string
        If you switch remote, set AWS DynamoDB endpoint url.
  --region string
        aws region
  --timeout string
        set command timeout
  --version
        show version
```

the required IAM Policy is as follows:
```json 
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "dynamodb:CreateTable",
                "dynamodb:UpdateTimeToLive",
                "dynamodb:PutItem",
                "dynamodb:DeleteItem",
                "dynamodb:DescribeTable",
                "dynamodb:GetItem",
                "dynamodb:UpdateItem"
            ],
            "Resource": "*"
        }
    ]
}
```

If the lock table has already been created, `dynamodb:CreateTable` and `dynamodb:UpdateTimeToLive` are not required.
## Install 

### binary packages

[Releases](https://github.com/mashiike/setddblock/releases).

### Homebrew tap

```console
$ brew install mashiike/tap/setddblock
```

## Usage as a library

`setddblock.New(url string, optFns ...func(*setddblock.Options))` returns a DynamoDBLocker that satisfies the sync.Locker interface.


```go
l, err := setddblock.New("ddb://ddb_lock_table/lock_item_id")
if err != nil {
	// ...
}
func () {
    l.Lock()
    defer l.Unlock()
    // ...
}()
```

Note: If Lock or Unlock fails, for example because you can't connect to DynamoDB, it will panic.  
      If you don't want it to panic, use `LockWithError()` and `UnlockWithErr()`. Alternatively, use the `WithNoPanic` option.

more infomation see [go doc](https://godoc.org/github.com/mashiike/setddblock).
## License

see [LICENSE](https://github.com/mashiike/setddblock/blob/master/LICENSE) file.

