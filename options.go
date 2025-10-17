package setddblock

import (
	"context"
	"io"
	"log/slog"
	"time"
)

// Options are for changing the behavior of DynamoDB Locker and are changed by the function passed to the New () function.
// See the WithXXX options for more information.
type Options struct {
	NoPanic       bool
	Logger        *slog.Logger
	Delay         bool
	Endpoint      string
	Region        string
	LeaseDuration time.Duration
	ctx           context.Context
}

// Default values
var (
	DefaultLeaseDuration = 10 * time.Second
)

func newOptions() *Options {
	return &Options{
		Logger:        slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
		LeaseDuration: DefaultLeaseDuration,
		Delay:         true,
		ctx:           context.Background(),
	}
}

// WithNoPanic changes the behavior so that it does not panic if an error occurs in the Lock () and Unlock () functions.
// Check the LastErr () function to see if an error has occurred when WithNoPanic is specified.
func WithNoPanic() func(opts *Options) {
	return func(opts *Options) {
		opts.NoPanic = true
	}
}

// WithDelay will delay the acquisition of the lock if it fails to acquire the lock. This is similar to the N option of setlock.
// The default is delay enalbed(true). Specify false if you want to exit immediately if Lock acquisition fails.
func WithDelay(delay bool) func(opts *Options) {
	return func(opts *Options) {
		opts.Delay = delay
	}
}

// WithLogger is a setting to enable the log output of DynamoDB Locker. By default, Logger that does not output anywhere is specified.
func WithLogger(logger *slog.Logger) func(opts *Options) {
	return func(opts *Options) {
		opts.Logger = logger.With("component", "setddblock")
	}
}

// WithEndpoint is an endpoint specification option for Local development. Please enter the URL of DynamoDB Local etc.
func WithEndpoint(endpoint string) func(opts *Options) {
	return func(opts *Options) {
		opts.Endpoint = endpoint
	}
}

// WithRegion specifies the AWS Region. Default AWS_DEFAULT_REGION env
func WithRegion(region string) func(opts *Options) {
	return func(opts *Options) {
		opts.Region = region
	}
}

// WithLeaseDuration affects the heartbeat interval and TTL after Lock acquisition. The default is 10 seconds
func WithLeaseDuration(d time.Duration) func(opts *Options) {
	return func(opts *Options) {
		opts.LeaseDuration = d
	}
}

// WithContext specifies the Context used by Lock() and Unlock().
func WithContext(ctx context.Context) func(opts *Options) {
	return func(opts *Options) {
		opts.ctx = ctx
	}
}
