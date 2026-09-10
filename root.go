package lifecycle

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

type RootComponent struct {
	BaseComponent
	signals []os.Signal
}

func (r *RootComponent) Name() string {
	return "root"
}

func (r *RootComponent) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, r.signals...)
	defer stop()

	<-ctx.Done()
	return ctx.Err()
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
