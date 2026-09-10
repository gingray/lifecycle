package lifecycle

import "context"

// Component is a unit managed by a Node. Run must return once ctx is cancelled: Shutdown isn't called until it does.
type Component interface {
	Name() string
	Ready(ctx context.Context) error
	Run(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type ReadyFunc func(ctx context.Context) error
type ShutdownFunc func(ctx context.Context) error
type BaseComponent struct {
	ReadyHandlers    []ReadyFunc
	ShutdownHandlers []ShutdownFunc
}

func (b *BaseComponent) Ready(ctx context.Context) error {
	fns := make([]func(context.Context) error, len(b.ReadyHandlers))
	for i, handler := range b.ReadyHandlers {
		fns[i] = handler
	}
	return runAll(ctx, fns...)
}

func (b *BaseComponent) Shutdown(ctx context.Context) error {
	fns := make([]func(context.Context) error, len(b.ShutdownHandlers))
	for i, handler := range b.ShutdownHandlers {
		fns[i] = handler
	}
	return runAll(ctx, fns...)
}

func (b *BaseComponent) AddReadyHandler(handler ReadyFunc) {
	b.ReadyHandlers = append(b.ReadyHandlers, handler)
}

func (b *BaseComponent) AddShutdownHandler(handler ShutdownFunc) {
	b.ShutdownHandlers = append(b.ShutdownHandlers, handler)
}
