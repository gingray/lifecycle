package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// process run itself component and child components and childs run's own componets as well
// TODO: because all of components run concurrently technically no guarantee that current component run first and then children
// even that first execution is current component. Maybe it's good to think about different execution model, but huge plus
// that it simple to understand and manage
func process(ctx context.Context, node *Node) error {
	executions := make([]func(context.Context) error, 0, len(node.Nodes)+1)
	executions = append(executions, func(ctx context.Context) error {
		node.logger.Info("supervisor", "status", RunStart, "component", node.Component.Name())
		return node.Component.Run(ctx)
	})
	for _, child := range node.Nodes {
		executions = append(executions, child.Run)
	}
	joined := runComponents(ctx, executions...)

	node.logger.Info("supervisor", "status", ShutdownStart, "component", node.Component.Name())
	// create new context for shutdown because no point to use ctx because it may be cancelled already
	// also it's propagating parent cancellation to children, means timeout of child can't be more then parent
	shutdownCtx, cancel := createShutdownContext(context.Background(), node.shutdownTimeout)
	defer cancel()
	stopErr := fmt.Errorf("component: %s, %w", node.Component.Name(), ErrComponentStop)
	joined = errors.Join(joined, node.Component.Shutdown(shutdownCtx), stopErr)
	node.logger.Info("supervisor", "status", ShutdownFinish, "component", node.Component.Name())

	return joined
}

// runComponents runs every function concurrently. A non-nil error from any
// one of them cancels the shared context passed to the rest, matching
// errgroup's usual cancel-on-first-error behaviour, but unlike a plain
// errgroup.Wait() every error is preserved and returned joined together
// rather than only the first one.
func runComponents(ctx context.Context, fns ...func(context.Context) error) error {
	g, errCtx := errgroup.WithContext(ctx)

	var mu sync.Mutex
	var joined error
	for _, fn := range fns {
		g.Go(func() error {
			err := fn(errCtx)
			if err != nil {
				mu.Lock()
				joined = errors.Join(joined, err)
				mu.Unlock()
			}
			return err
		})
	}
	err := g.Wait()
	if err != nil {
		joined = errors.Join(joined, err)
	}

	return joined
}

// createShutdownContext derives a context for the shutdown phase. If timeout is
// set, it's applied to a context detached from ctx's own cancellation (which
// may already be cancelled, e.g. by the signal that triggered shutdown) so
// shutdown gets its own fresh, bounded budget rather than none at all.
func createShutdownContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}
