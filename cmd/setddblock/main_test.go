package main

import (
	"flag"
	"io"
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
