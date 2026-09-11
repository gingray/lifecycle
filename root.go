package lifecycle

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// RootComponent sits at the top of a tree. It stops when one of its signals arrives or its ctx is cancelled.
type RootComponent struct {
	Component
	signals []os.Signal
}

// Name returns "root".
func (r *RootComponent) Name() string {
	return "root"
}

// Run blocks until a signal arrives or ctx is cancelled. Both are a clean stop, so it returns nil.
func (r *RootComponent) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, r.signals...)
	defer stop()

	<-ctx.Done()
	return nil
}

// NewRootComponent creates a RootComponent that triggers shutdown on the
// given signals, defaulting to os.Interrupt and syscall.SIGTERM when none
// are given.
func NewRootComponent(signals ...os.Signal) *RootComponent {
	if len(signals) == 0 {
		signals = []os.Signal{os.Interrupt, syscall.SIGTERM}
	}
	return &RootComponent{signals: signals}
}
