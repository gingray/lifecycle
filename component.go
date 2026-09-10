package lifecycle

import (
	"context"

	"golang.org/x/sync/errgroup"
)

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
	g, errCtx := errgroup.WithContext(ctx)

	for _, handler := range b.ReadyHandlers {
		g.Go(func() error {
			return handler(errCtx)
		})
	}
	err := g.Wait()
	return err
}

func (b *BaseComponent) Shutdown(ctx context.Context) error {
	g, errCtx := errgroup.WithContext(ctx)

	for _, handler := range b.ShutdownHandlers {
		g.Go(func() error {
			return handler(errCtx)
		})
	}
	err := g.Wait()
	return err
}

func (b *BaseComponent) AddReadyHandler(handler ReadyFunc) {
	b.ReadyHandlers = append(b.ReadyHandlers, handler)
}

func (b *BaseComponent) AddShutdownHandler(handler ShutdownFunc) {
	b.ShutdownHandlers = append(b.ShutdownHandlers, handler)
}
