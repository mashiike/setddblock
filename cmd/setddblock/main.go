package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/fatih/color"
	"github.com/fujiwara/logutils"
	"github.com/mashiike/setddblock"
)

var (
	Version = "current"
)

func main() {
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
	flag.Parse()

	if versionFlag {
		fmt.Fprintf(flag.CommandLine.Output(), "setddblock version: %s\n", Version)
		fmt.Fprintf(flag.CommandLine.Output(), "go runtime version: %s\n", runtime.Version())
		os.Exit(0)
	}

	if flag.NArg() < 2 {
		flag.CommandLine.Usage()
		os.Exit(1)
	}
	args := flag.Args()
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
	optFns := []func(*setddblock.Options){
		setddblock.WithDelay(n && !N),
		setddblock.WithLogger(logger),
		setddblock.WithRegion(region),
	}
	if endpoint != "" {
		optFns = append(optFns, setddblock.WithEndpoint(endpoint))
	}
	locker, err := setddblock.New(args[0], optFns...)
	if err != nil {
		logger.Println("[error][setddblock]", err)
		os.Exit(2)
	}
	ctx := context.Background()
	if timeout != "" {
		t, err := time.ParseDuration(timeout)
		if err != nil {
			logger.Println("[error][setddblock] failed timeout parse: ", err)
			os.Exit(7)
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}
	lockGranted, err := locker.LockWithErr(ctx)
	if err != nil {
		logger.Println("[error][setddblock]", err)
		os.Exit(6)
	}
	if !lockGranted {
		logger.Println("[warn][setddblock] lock was not granted")
		if x && !X {
			return
		}
		os.Exit(3)
	}
	defer locker.Unlock()

	cmd := exec.Command(args[1], args[2:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		logger.Printf("[error][setddblock] setddblock: fatal: unable to run %s\n", err)
		os.Exit(5)
	}
}
