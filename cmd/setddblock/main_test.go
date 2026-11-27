package main

import (
	"errors"
	"flag"
	"io"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestFlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("setddblock", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var b bool
	var s string
	var d time.Duration
	for _, name := range []string{"n", "N", "x", "X", "debug", "version"} {
		fs.BoolVar(&b, name, false, "")
	}
	for _, name := range []string{"endpoint", "region", "timeout"} {
		fs.StringVar(&s, name, "", "")
	}
	fs.DurationVar(&d, "expire-grace-period", 0, "")
	return fs
}

func TestNormalizeArgs(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "combined short flags",
			args: []string{"setddblock", "-nx", "ddb://t/i", "echo", "hi"},
			want: []string{"setddblock", "-n", "-x", "ddb://t/i", "echo", "hi"},
		},
		{
			name: "long flag with equal",
			args: []string{"setddblock", "--endpoint=http://localhost:8000", "ddb://t/i", "echo"},
			want: []string{"setddblock", "--endpoint", "http://localhost:8000", "ddb://t/i", "echo"},
		},
		{
			name: "long flag with separate value followed by short flags",
			args: []string{"setddblock", "--region", "ap-northeast-1", "-nX", "ddb://t/i", "echo"},
			want: []string{"setddblock", "--region", "ap-northeast-1", "-n", "-X", "ddb://t/i", "echo"},
		},
		{
			name: "boolean long flag with equal keeps its form",
			args: []string{"setddblock", "--debug=false", "ddb://t/i", "echo"},
			want: []string{"setddblock", "--debug=false", "ddb://t/i", "echo"},
		},
		{
			name: "wrapped command flags are kept as is",
			args: []string{"setddblock", "-n", "ddb://t/i", "find", ".", "-name", "x", "--newer=y"},
			want: []string{"setddblock", "-n", "ddb://t/i", "find", ".", "-name", "x", "--newer=y"},
		},
		{
			name: "short flag with attached value of wrapped command",
			args: []string{"setddblock", "ddb://t/i", "head", "-n5", "file"},
			want: []string{"setddblock", "ddb://t/i", "head", "-n5", "file"},
		},
		{
			name: "double dash after url",
			args: []string{"setddblock", "-x", "ddb://t/i", "--", "sort", "-k2"},
			want: []string{"setddblock", "-x", "ddb://t/i", "--", "sort", "-k2"},
		},
		{
			name: "double dash before url",
			args: []string{"setddblock", "-x", "--", "ddb://t/i", "sort", "-k2"},
			want: []string{"setddblock", "-x", "--", "ddb://t/i", "sort", "-k2"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, normalizeArgs(newTestFlagSet(), c.args))
		})
	}
}

func TestNormalizeArgsParse(t *testing.T) {
	fs := flag.NewFlagSet("setddblock", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	n := fs.Bool("n", false, "")
	x := fs.Bool("x", false, "")
	endpoint := fs.String("endpoint", "", "")
	args := normalizeArgs(fs, []string{"setddblock", "-nx", "--endpoint=http://localhost:8000", "ddb://t/i", "find", ".", "-name", "x"})
	require.NoError(t, fs.Parse(args[1:]))
	require.True(t, *n)
	require.True(t, *x)
	require.Equal(t, "http://localhost:8000", *endpoint)
	require.Equal(t, []string{"ddb://t/i", "find", ".", "-name", "x"}, fs.Args())
}

func TestRunCommandLockLost(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses sleep command")
	}
	lost := make(chan struct{})
	close(lost)
	start := time.Now()
	code := runCommand(t.Context(), lost, nil, "sleep", []string{"10"})
	require.Equal(t, exitLockLost, code)
	require.Less(t, time.Since(start), 5*time.Second, "command should be terminated when the lock is lost")
}

func TestRunCommandExitCode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses sh")
	}
	cases := []struct {
		name   string
		script string
		want   int
	}{
		{name: "success", script: "exit 0", want: 0},
		{name: "exit 1", script: "exit 1", want: 1},
		{name: "exit 3 is passed through", script: "exit 3", want: 3},
		{name: "exit 42 is passed through", script: "exit 42", want: 42},
		{name: "killed by SIGTERM is 128+15", script: "kill -TERM $$", want: 143},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, runCommand(t.Context(), nil, nil, "sh", []string{"-c", c.script}))
		})
	}
}

func TestRunCommandNotFound(t *testing.T) {
	require.Equal(t, 5, runCommand(t.Context(), nil, nil, "setddblock-no-such-command", nil))
}

func TestRunCommandForwardsSignal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals are not supported on windows")
	}
	sigCh := make(chan os.Signal, 1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		sigCh <- syscall.SIGTERM
	}()
	start := time.Now()
	// the command exits with 7 when it receives SIGTERM, so 7 proves both forwarding and pass-through
	code := runCommand(t.Context(), nil, sigCh, "sh", []string{"-c", `trap 'kill $!; exit 7' TERM; sleep 10 & wait`})
	require.Equal(t, 7, code)
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestRunCommandDoesNotForwardSIGINT(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signals are not supported on windows")
	}
	sigCh := make(chan os.Signal, 1)
	sigCh <- syscall.SIGINT
	code := runCommand(t.Context(), nil, sigCh, "sh", []string{"-c", `trap "exit 7" INT; sleep 0.5`})
	require.Equal(t, 0, code, "SIGINT should not be forwarded (the terminal already sends it to the command)")
}

func TestLockFailureExitCode(t *testing.T) {
	cases := []struct {
		name     string
		granted  bool
		err      error
		exitZero bool
		want     int
		wantOK   bool
	}{
		{name: "granted", granted: true, want: 0, wantOK: false},
		{name: "not granted", want: 3, wantOK: true},
		{name: "not granted with -x", exitZero: true, want: 0, wantOK: true},
		{name: "dynamodb error", err: errors.New("connection refused"), want: 6, wantOK: true},
		{name: "dynamodb error with -x", err: errors.New("connection refused"), exitZero: true, want: 0, wantOK: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, ok := lockFailureExitCode(c.granted, c.err, c.exitZero)
			require.Equal(t, c.wantOK, ok)
			require.Equal(t, c.want, code)
		})
	}
}
