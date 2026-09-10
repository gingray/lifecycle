package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// process runs the node's component and its children concurrently, then shuts the node's component down.
// Each child's Run shuts down its own subtree before returning, so children always stop before their parent.
// TODO: everything starts concurrently, so there's no guarantee the node's component starts before its children
// (or even first). A different execution model may be worth exploring, but this one is simple to understand and manage.
func process(ctx context.Context, node *Node) error {
	executions := make([]func(context.Context) error, 0, len(node.Nodes)+1)
	executions = append(executions, func(ctx context.Context) error {
		node.logger.Info("supervisor", "status", RunStart, "component", node.Component.Name())
		return node.Component.Run(ctx)
	})
	for _, child := range node.Nodes {
		executions = append(executions, child.Run)
	}
	joined := runAll(ctx, executions...)

	node.logger.Info("supervisor", "status", ShutdownStart, "component", node.Component.Name())
	// Each node gets its own timeout, so a whole tree can take longer than shutdownTimeout to stop.
	shutdownCtx, cancel := createShutdownContext(ctx, node.shutdownTimeout)
	defer cancel()
	stopErr := fmt.Errorf("component: %s, %w", node.Component.Name(), ErrComponentStop)
	joined = errors.Join(joined, node.Component.Shutdown(shutdownCtx), stopErr)
	node.logger.Info("supervisor", "status", ShutdownFinish, "component", node.Component.Name())

	return joined
}

// runAll runs every function concurrently. A non-nil error from any
// one of them cancels the shared context passed to the rest, matching
// errgroup's usual cancel-on-first-error behaviour, but unlike a plain
// errgroup.Wait() every error is preserved and returned joined together
// rather than only the first one.
func runAll(ctx context.Context, fns ...func(context.Context) error) error {
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
	// Wait's error is already in joined; adding it again would duplicate it.
	_ = g.Wait()

	return joined
}

// createShutdownContext detaches from ctx's cancellation (it's usually already cancelled by the time
// shutdown starts) while keeping its values, then applies timeout if one is set.
func createShutdownContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(ctx)
	if timeout <= 0 {
		return context.WithCancel(detached)
	}
	return context.WithTimeout(detached, timeout)
}
