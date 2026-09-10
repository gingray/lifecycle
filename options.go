package lifecycle

import (
	"os"
	"time"
)

type config struct {
	shutdownTimeout time.Duration
	signals         []os.Signal
}

func newConfig(opts []Option) *config {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// Option configures a Node tree created via DefaultRoot or GetNodeCreator.
type Option func(*config)

// WithShutdownTimeout bounds how long a component's Shutdown is given to
// complete once shutdown begins. Without it, shutdown is unbounded.
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *config) { c.shutdownTimeout = d }
}

// WithSignals overrides the OS signals that trigger shutdown of the root
// component. Only meaningful when passed to DefaultRoot. Defaults to
// os.Interrupt and syscall.SIGTERM when unset.
func WithSignals(sig ...os.Signal) Option {
	return func(c *config) { c.signals = sig }
}
