package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/fatih/color"
	"github.com/fujiwara/logutils"
	"github.com/mashiike/setddblock"
)

func main() {
	var (
		n, N, x, X, debug bool
		endpoint, region  string
	)
	flag.BoolVar(&n, "n", false, "No delay. If fn is locked by another process, setlock gives up.")
	flag.BoolVar(&N, "N", false, "(Default.) Delay. If fn is locked by another process, setlock waits until it can obtain a new lock.")
	flag.BoolVar(&x, "x", false, "If fn cannot be update-item (or put-item) or locked, setlock exits zero.")
	flag.BoolVar(&X, "X", false, "(Default.) If fn cannot be update-item (or put-item) or locked, setlock prints an error message and exits nonzero.")
	flag.BoolVar(&debug, "debug", false, "show debug log")
	flag.StringVar(&endpoint, "endpoint", "", "If you switch remote, set AWS DynamoDB endpoint url.")
	flag.StringVar(&region, "region", "", "aws region")
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "Usage: setddblock [ -nNxX -endpoint <endpoint> -debug] ddb://<table_name>/<item_id> child\n")
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
	if err := locker.LockWithErr(ctx); err != nil {
		if x && !X {
			return
		}
		logger.Println("[error][setddblock]", err)
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
