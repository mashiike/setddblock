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
        If the lock cannot be acquired (held by another process, or DynamoDB is unreachable), setddblock exits zero.
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

### Exit status and signals

setddblock follows [setlock](https://cr.yp.to/daemontools/setlock.html):

- When the command runs, setddblock exits with the command's exit status (128+N if the command was killed by signal N).
- With `-x`, setddblock exits 0 whenever the lock cannot be acquired: another process holds it (`-n`), or DynamoDB cannot be reached.
- SIGTERM and SIGHUP are forwarded to the command; setddblock waits for it to exit and then releases the lock. SIGINT is not forwarded, because Ctrl-C on a terminal already reaches the command; setddblock still waits and releases the lock. A signal received while waiting for the lock stops the wait (exit 128+N).

setddblock's own errors (without `-x`):

| code | meaning |
|------|---------|
| 2 | invalid `ddb://` URL or options |
| 3 | the lock is held by another process (`-n`) |
| 4 | the lock was lost while the command was running; the command was sent SIGTERM |
| 5 | the command could not be started |
| 6 | failed to acquire the lock (DynamoDB error, or `--timeout` expired while waiting) |
| 7 | invalid `--timeout` |

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

