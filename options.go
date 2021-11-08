package ddblock

import "time"

type Options struct {
	NoPanic       bool
	Logger        Logger
	Delay         bool
	Endpoint      string
	Region        string
	LeaseDuration time.Duration
}

var (
	DefaultLeaseDuration = 10 * time.Second
)

func newOptions() *Options {
	return &Options{
		Logger:        voidLogger{},
		LeaseDuration: DefaultLeaseDuration,
	}
}

func WithDelay(delay bool) func(opts *Options) {
	return func(opts *Options) {
		opts.Delay = delay
	}
}

type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type voidLogger struct{}

func (voidLogger) Print(v ...interface{})                 {}
func (voidLogger) Printf(format string, v ...interface{}) {}
func (voidLogger) Println(v ...interface{})               {}

func WithLogger(logger Logger) func(opts *Options) {
	return func(opts *Options) {
		opts.Logger = logger
	}
}

func WithEndpoint(endpoint string) func(opts *Options) {
	return func(opts *Options) {
		opts.Endpoint = endpoint
	}
}

func WithRegion(region string) func(opts *Options) {
	return func(opts *Options) {
		opts.Region = region
	}
}

func WithLeaseDuration(d time.Duration) func(opts *Options) {
	return func(opts *Options) {
		opts.LeaseDuration = d
	}
}
