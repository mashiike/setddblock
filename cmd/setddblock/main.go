package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

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
		expireGracePeriod              time.Duration
	)
	flag.CommandLine.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: setddblock [ -nNxX ] [--endpoint <endpoint>] [--debug --version] ddb://<table_name>/<item_id> your_command\n")
		printDefaults(flag.CommandLine)
	}
	flag.BoolVar(&n, "n", false, "No delay. If fn is locked by another process, setlock gives up.")
	flag.BoolVar(&N, "N", false, "(Default.) Delay. If fn is locked by another process, setlock waits until it can obtain a new lock.")
	flag.BoolVar(&x, "x", false, "If the lock cannot be acquired (held by another process, or DynamoDB is unreachable), setddblock exits zero.")
	flag.BoolVar(&X, "X", false, "(Default.) If fn cannot be update-item (or put-item) or locked, setlock prints an error message and exits nonzero.")
	flag.BoolVar(&debug, "debug", false, "show debug log")
	flag.BoolVar(&versionFlag, "version", false, "show version")
	flag.StringVar(&endpoint, "endpoint", "", "If you switch remote, set AWS DynamoDB endpoint url.")
	flag.StringVar(&region, "region", "", "aws region")
	flag.StringVar(&timeout, "timeout", "", "set command timeout")
	flag.DurationVar(&expireGracePeriod, "expire-grace-period", 0, "set expire grace period duration after TTL expiration")

	args := normalizeArgs(flag.CommandLine, os.Args)
	if err := flag.CommandLine.Parse(args[1:]); err != nil {
		fmt.Fprintf(flag.CommandLine.Output(), "setddblock: %v\n", err)
		return 1
	}
	if versionFlag {
		fmt.Fprintf(flag.CommandLine.Output(), "setddblock version: %s\n", Version)
		fmt.Fprintf(flag.CommandLine.Output(), "go runtime version: %s\n", runtime.Version())
		return 0
	}
	offset := 0
	if flag.NArg() < 1 {
		flag.CommandLine.Usage()
		fmt.Fprintf(flag.CommandLine.Output(), "\nsetddblock: missing ddb dsn\n")
		return 1
	}
	if flag.Arg(1) == "--" {
		offset = 1
	}
	if flag.NArg()-offset < 2 {
		flag.CommandLine.Usage()
		fmt.Fprintf(flag.CommandLine.Output(), "\nsetddblock: missing your command\n")
		return 1
	}
	args = flag.Args()
	if offset > 0 {
		args = append(args[0:offset], args[offset+1:]...)
	}
	minLevel := slog.LevelInfo
	if debug {
		minLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: minLevel,
	}))
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
	if expireGracePeriod > 0 {
		optFns = append(optFns, setddblock.WithExpireGracePeriod(expireGracePeriod))
	}
	if endpoint != "" {
		optFns = append(optFns, setddblock.WithEndpoint(endpoint))
	}
	locker, err := setddblock.New(args[0], optFns...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "setddblock: failed to create locker: %v\n", err)
		return 2
	}
	ctx := context.Background()
	if timeout != "" {
		t, err := time.ParseDuration(timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "setddblock: failed to parse timeout: %v\n", err)
			return 7
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}

	// Catch termination signals so that the lock is always released on the way out.
	// While waiting for the lock, a signal aborts the wait.
	// While the command runs, the signal is forwarded to it (see runCommand).
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, forwardSignals...)
	defer signal.Stop(sigCh)
	lockCtx, cancelLock := context.WithCancel(ctx)
	defer cancelLock()
	var received os.Signal
	waitDone := make(chan struct{})
	watcherExited := make(chan struct{})
	go func() {
		defer close(watcherExited)
		select {
		case sig := <-sigCh:
			received = sig
			cancelLock()
		case <-waitDone:
		}
	}()
	lockGranted, err := locker.LockWithErr(lockCtx)
	close(waitDone)
	<-watcherExited
	if received != nil {
		fmt.Fprintf(os.Stderr, "setddblock: received %v while waiting for the lock\n", received)
		if lockGranted {
			if err := locker.UnlockWithErr(context.Background()); err != nil {
				fmt.Fprintf(os.Stderr, "setddblock: failed to release lock: %v\n", err)
			}
		}
		return signalExitCode(received)
	}
	if code, ok := lockFailureExitCode(lockGranted, err, x && !X); ok {
		return code
	}
	defer locker.Unlock()

	return runCommand(ctx, locker.LockLost(), sigCh, args[1], args[2:])
}

// lockFailureExitCode returns the exit code when the lock could not be acquired.
// Like setlock, -x makes setddblock exit zero whenever the lock cannot be acquired,
// whether another process holds it or DynamoDB cannot be reached.
// ok is false when the lock was granted.
func lockFailureExitCode(lockGranted bool, err error, exitZero bool) (code int, ok bool) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "setddblock: failed to acquire lock: %v\n", err)
		if exitZero {
			return 0, true
		}
		return 6, true
	}
	if !lockGranted {
		fmt.Fprintf(os.Stderr, "setddblock: lock was not granted\n")
		if exitZero {
			return 0, true
		}
		return 3, true
	}
	return 0, false
}

// forwardSignals are the signals passed on to the command.
// SIGINT is caught but not forwarded: Ctrl-C on a terminal already reaches the command
// (same process group), and sending it twice can make programs force-quit.
var forwardSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP}

// exitLockLost is the exit code when the lock was lost while the command was running.
const exitLockLost = 4

// runCommand runs the command while the lock is held and returns the command's exit code,
// like setlock which execs the command.
// Signals received on sigCh (except SIGINT) are forwarded to the command, and setddblock waits
// for it to exit so the caller can release the lock.
// If the lock is lost while the command is running, the command is sent SIGTERM
// (killed on platforms without SIGTERM support) so it never runs without the lock,
// and setddblock exits with exitLockLost.
func runCommand(ctx context.Context, lockLost <-chan struct{}, sigCh <-chan os.Signal, name string, args []string) int {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "setddblock: unable to run command: %v\n", err)
		return 5
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	for {
		select {
		case err := <-done:
			if ctx.Err() != nil {
				fmt.Fprintf(os.Stderr, "setddblock: command stopped: %v\n", ctx.Err())
			}
			return commandExitCode(err)
		case sig := <-sigCh:
			if sig == syscall.SIGINT {
				continue
			}
			if err := cmd.Process.Signal(sig); err != nil {
				fmt.Fprintf(os.Stderr, "setddblock: failed to forward %v to command: %v\n", sig, err)
			}
		case <-lockLost:
			fmt.Fprintf(os.Stderr, "setddblock: lock lost, terminating command\n")
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				if err := cmd.Process.Kill(); err != nil {
					fmt.Fprintf(os.Stderr, "setddblock: failed to stop command: %v\n", err)
				}
			}
			if err := <-done; err != nil {
				fmt.Fprintf(os.Stderr, "setddblock: command stopped: %v\n", err)
			}
			return exitLockLost
		}
	}
}

// commandExitCode converts the result of cmd.Wait() to an exit code.
// A command killed by a signal is reported as 128+signal, like a shell does.
func commandExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		fmt.Fprintf(os.Stderr, "setddblock: unable to run command: %v\n", err)
		return 5
	}
	if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return signalExitCode(status.Signal())
	}
	return exitErr.ExitCode()
}

// signalExitCode returns 128+signal for a signal, as shells do.
func signalExitCode(sig os.Signal) int {
	if s, ok := sig.(syscall.Signal); ok {
		return 128 + int(s)
	}
	return 1
}

func normalizeArgs(flagSet *flag.FlagSet, osArgs []string) []string {
	args := make([]string, 1, len(osArgs))
	args[0] = osArgs[0]
	rest := osArgs[1:]
	for i := 0; i < len(rest); i++ {
		arg := rest[i]
		switch {
		case arg == "--" || arg == "-" || !strings.HasPrefix(arg, "-"):
			// end of setddblock's own flags
			return append(args, rest[i:]...)
		case strings.HasPrefix(arg, "--"):
			// long flags
			name, value, hasValue := strings.Cut(arg[2:], "=")
			if hasValue && !takesValue(flagSet, name) {
				// boolean flags such as --debug=false must keep the = form
				args = append(args, arg)
				continue
			}
			args = append(args, "--"+name)
			if hasValue {
				args = append(args, value)
			} else if takesValue(flagSet, name) && i+1 < len(rest) {
				i++
				args = append(args, rest[i])
			}
		default:
			// short flags
			for j := 1; j < len(arg); j++ {
				args = append(args, "-"+string(arg[j]))
			}
		}
	}
	return args
}

func takesValue(flagSet *flag.FlagSet, name string) bool {
	f := flagSet.Lookup(name)
	if f == nil {
		return false
	}
	if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
		return false
	}
	return true
}

func printDefaults(flagSet *flag.FlagSet) {
	shortFlags := make([]*flag.Flag, 0, flagSet.NFlag())
	longFlags := make([]*flag.Flag, 0, flagSet.NFlag())

	flagSet.VisitAll(func(f *flag.Flag) {
		if len(f.Name) > 1 {
			longFlags = append(longFlags, f)
		} else {
			shortFlags = append(shortFlags, f)
		}
	})
	fmt.Fprintln(flagSet.Output(), "Flags:")
	sort.Slice(shortFlags, func(i, j int) bool {
		if strings.EqualFold(shortFlags[i].Name, shortFlags[j].Name) {
			return shortFlags[i].Name > shortFlags[j].Name
		}
		return strings.ToLower(shortFlags[i].Name) < strings.ToLower(shortFlags[j].Name)
	})
	sort.Slice(longFlags, func(i, j int) bool {
		return strings.ToLower(longFlags[i].Name) < strings.ToLower(longFlags[j].Name)
	})
	flags := append(shortFlags, longFlags...)
	for _, f := range flags {
		var builder strings.Builder
		if len(f.Name) > 1 {
			//long flag
			fmt.Fprintf(&builder, "  --%s", f.Name)
		} else {
			//short flag
			fmt.Fprintf(&builder, "  -%s", f.Name)
		}
		name, usage := flag.UnquoteUsage(f)
		if len(name) > 0 {
			builder.WriteString(" ")
			builder.WriteString(name)
		}
		builder.WriteString("\t")
		if builder.Len() <= 4 { // space, space, '-', 'x'.
			builder.WriteString("\t")
		} else {
			builder.WriteString("\n    \t")
		}
		builder.WriteString(strings.ReplaceAll(usage, "\n", "\n    \t"))
		fmt.Fprint(flagSet.Output(), builder.String(), "\n")
	}
}
