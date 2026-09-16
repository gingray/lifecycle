package lifecycle

import "context"

// C is a unit managed by a Node. Run must return once ctx is cancelled: Shutdown isn't called until it does.
type C interface {
	Name() string
	Ready(ctx context.Context) error
	Run(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// ReadyFunc is a readiness check registered on a Component.
type ReadyFunc func(ctx context.Context) error

// ShutdownFunc is a cleanup step registered on a Component.
type ShutdownFunc func(ctx context.Context) error

// Component implements Ready and Shutdown by running registered handlers. Embed it in a type that adds Name
// and Run.
type Component struct {
	ReadyHandlers    []ReadyFunc
	ShutdownHandlers []ShutdownFunc
}

// Ready runs every ready handler concurrently. The first failure cancels the others' ctx; all errors are returned
// joined.
func (b *Component) Ready(ctx context.Context) error {
	fns := make([]func(context.Context) error, len(b.ReadyHandlers))
	for i, handler := range b.ReadyHandlers {
		fns[i] = handler
	}
	return runAll(ctx, cancelOnError, fns...)
}

// TODO: maybe it's good to make shutdown and ready handlers run in order as they was registered

// Shutdown runs every shutdown handler concurrently. A failing handler doesn't cancel the others; all errors are
// returned joined.
func (b *Component) Shutdown(ctx context.Context) error {
	fns := make([]func(context.Context) error, len(b.ShutdownHandlers))
	for i, handler := range b.ShutdownHandlers {
		fns[i] = handler
	}
	return runAll(ctx, cancelNever, fns...)
}

// AddReadyHandler registers a readiness check.
func (b *Component) AddReadyHandler(handler ReadyFunc) {
	b.ReadyHandlers = append(b.ReadyHandlers, handler)
}

// AddShutdownHandler registers a cleanup step.
func (b *Component) AddShutdownHandler(handler ShutdownFunc) {
	b.ShutdownHandlers = append(b.ShutdownHandlers, handler)
}
