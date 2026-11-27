package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
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
	flag.BoolVar(&x, "x", false, "If fn cannot be update-item (or put-item) or locked, setlock exits zero.")
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
	lockGranted, err := locker.LockWithErr(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "setddblock: failed to acquire lock: %v\n", err)
		return 6
	}
	if !lockGranted {
		fmt.Fprintf(os.Stderr, "setddblock: lock was not granted\n")
		if x && !X {
			return 0
		}
		return 3
	}
	defer locker.Unlock()

	return runCommand(ctx, locker.LockLost(), args[1], args[2:])
}

// exitLockLost is the exit code when the lock was lost while the command was running.
const exitLockLost = 4

// runCommand runs the command while the lock is held.
// If the lock is lost while the command is running, the command is sent SIGTERM
// (killed on platforms without SIGTERM support) so it never runs without the lock,
// and setddblock exits with exitLockLost.
func runCommand(ctx context.Context, lockLost <-chan struct{}, name string, args []string) int {
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
	select {
	case err := <-done:
		if err != nil {
			fmt.Fprintf(os.Stderr, "setddblock: unable to run command: %v\n", err)
			return 5
		}
		return 0
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
