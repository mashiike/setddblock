package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/fujiwara/logutils"
	"github.com/mashiike/setddblock"
)

var (
	Version = "current"
)

func main() {
	os.Exit(_main())
}

func _main() int {
	var (
		n, N, x, X, debug, versionFlag bool
		endpoint, region, timeout      string
	)
	flag.CommandLine.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: setddblock [ -nNxX ] [-endpoint <endpoint>] [-debug -version] ddb://<table_name>/<item_id> your_command\n")
		flag.CommandLine.PrintDefaults()
	}
	flag.BoolVar(&n, "n", false, "No delay. If fn is locked by another process, setlock gives up.")
	flag.BoolVar(&N, "N", false, "(Default.) Delay. If fn is locked by another process, setlock waits until it can obtain a new lock.")
	flag.BoolVar(&x, "x", false, "If fn cannot be update-item (or put-item) or locked, setlock exits zero.")
	flag.BoolVar(&X, "X", false, "(Default.) If fn cannot be update-item (or put-item) or locked, setlock prints an error message and exits nonzero.")
	flag.BoolVar(&debug, "debug", false, "show debug log")
	flag.BoolVar(&versionFlag, "version", false, "show version")
	flag.StringVar(&endpoint, "endpoint", "", "If you switch remote, set AWS DynamoDB endpoint url.")
	flag.StringVar(&region, "region", "", "aws region")
	flag.StringVar(&timeout, "timeout", "", "set command timeout")
	args := make([]string, 1, len(os.Args))
	args[0] = os.Args[0]
	for _, arg := range os.Args[1:] {
		// long flags
		if strings.HasPrefix(arg, "--") && len(arg) > 2 {
			if strings.Contains(arg, "=") {
				parts := strings.SplitN(arg[2:], "=", 2)
				args = append(args, "--"+parts[0])
				args = append(args, parts[1])
			} else {
				args = append(args, arg)
			}
			continue
		}
		//short flags
		if strings.HasPrefix(arg, "-") && len(arg) > 1 {
			for i := 1; i < len(arg); i++ {
				args = append(args, "-"+string(arg[i]))
			}
			continue
		}
		args = append(args, arg)
	}
	flag.CommandLine.Parse(args[1:])
	if versionFlag {
		fmt.Fprintf(flag.CommandLine.Output(), "setddblock version: %s\n", Version)
		fmt.Fprintf(flag.CommandLine.Output(), "go runtime version: %s\n", runtime.Version())
		return 0
	}
	offset := 0
	if flag.NArg() < 1 {
		flag.CommandLine.Usage()
		fmt.Fprintf(os.Stderr, "\nsetddblock: missing ddb dsn\n")
		return 1
	}
	if flag.Arg(1) == "--" {
		offset = 1
	}
	if flag.NArg()-offset < 2 {
		flag.CommandLine.Usage()
		fmt.Fprintf(os.Stderr, "\nsetddblock: missing your command\n")
		return 1
	}
	args = flag.Args()
	if offset > 0 {
		args = append(args[0:offset], args[offset+1:]...)
	}
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"debug", "warn", "error"},
		MinLevel: "warn",
		ModifierFuncs: []logutils.ModifierFunc{
			logutils.Color(color.FgHiBlack),
			logutils.Color(color.FgYellow),
			logutils.Color(color.FgRed, color.Bold),
		},
		Writer: os.Stderr,
	}
	if debug {
		filter.MinLevel = logutils.LogLevel("debug")
	}
	logger := log.New(filter, "", log.LstdFlags|log.Lmsgprefix)
	// -N and -n both specified, Delay is true by default
	// -N and -n both not specified, Delay is true by default
	// -N specified, -n not specified, Delay is true
	// -N not specified, -n specified, Delay is false
	delay := N || (!N && !n)
	optFns := []func(*setddblock.Options){
		setddblock.WithDelay(delay),
		setddblock.WithLogger(logger),
		setddblock.WithRegion(region),
	}
	if endpoint != "" {
		optFns = append(optFns, setddblock.WithEndpoint(endpoint))
	}
	locker, err := setddblock.New(args[0], optFns...)
	if err != nil {
		logger.Println("[error][setddblock]", err)
		return 2
	}
	ctx := context.Background()
	if timeout != "" {
		t, err := time.ParseDuration(timeout)
		if err != nil {
			logger.Println("[error][setddblock] failed timeout parse: ", err)
			return 7
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}
	lockGranted, err := locker.LockWithErr(ctx)
	if err != nil {
		logger.Println("[error][setddblock]", err)
		return 6
	}
	if !lockGranted {
		logger.Println("[warn][setddblock] lock was not granted")
		if x && !X {
			return 0
		}
		return 3
	}
	defer locker.Unlock()

	cmd := exec.CommandContext(ctx, args[1], args[2:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		logger.Printf("[error][setddblock] setddblock: fatal: unable to run %s\n", err)
		return 5
	}
	return 0
}
