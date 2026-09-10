package lifecycle

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

type RootComponent struct {
	BaseComponent
}

func (r *RootComponent) Name() string {
	return "root"
}

func (r *RootComponent) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	return ctx.Err()
}
func NewRootComponent() *RootComponent {
	return &RootComponent{}
}
