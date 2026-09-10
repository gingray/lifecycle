package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// cancelPolicy decides when runAll cancels the ctx shared by the functions it runs.
type cancelPolicy int

const (
	cancelNever    cancelPolicy = iota // every function runs to completion
	cancelOnError                      // the first error cancels the rest
	cancelOnReturn                     // any function returning, with or without an error, cancels the rest
)

// process runs the node's component and its children concurrently, then shuts the node's component down.
// Each child's Run shuts down its own subtree before returning, so children always stop before their parent.
// TODO: everything starts concurrently, so there's no guarantee the node's component starts before its children
// (or even first). A different execution model may be worth exploring, but this one is simple to understand and manage.
func process(ctx context.Context, node *Node) error {
	name := node.Component.Name()
	executions := make([]func(context.Context) error, 0, len(node.Nodes)+1)
	executions = append(executions, func(ctx context.Context) error {
		node.logger.Info("supervisor", "status", RunStart, "component", name)
		err := safeCall(func() error { return node.Component.Run(ctx) })
		if err == nil || stoppedByShutdown(ctx, err) {
			return nil
		}
		node.logger.Error("supervisor", "status", RunFailed, "component", name, "error", err)
		return fmt.Errorf("component %s: run: %w", name, err)
	})
	for _, child := range node.Nodes {
		executions = append(executions, child.Run)
	}
	joined := runAll(ctx, cancelOnReturn, executions...)

	node.logger.Info("supervisor", "status", ShutdownStart, "component", name)
	// Each node gets its own timeout, so a whole tree can take longer than shutdownTimeout to stop.
	shutdownCtx, cancel := createShutdownContext(context.Background(), node.shutdownTimeout)
	defer cancel()
	if err := safeCall(func() error { return node.Component.Shutdown(shutdownCtx) }); err != nil {
		node.logger.Error("supervisor", "status", ShutdownFailed, "component", name, "error", err)
		joined = errors.Join(joined, fmt.Errorf("component %s: shutdown: %w", name, err))
	}
	node.logger.Info("supervisor", "status", ShutdownFinish, "component", name)

	return joined
}

// runAll runs fns concurrently and returns all of their errors joined. policy decides when the ctx passed to fns
// is cancelled.
func runAll(ctx context.Context, policy cancelPolicy, fns ...func(context.Context) error) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		joined error
	)
	for _, fn := range fns {
		wg.Go(func() {
			err := safeCall(func() error { return fn(runCtx) })
			if err != nil {
				mu.Lock()
				joined = errors.Join(joined, err)
				mu.Unlock()
			}
			if policy == cancelOnReturn || (policy == cancelOnError && err != nil) {
				cancel()
			}
		})
	}
	wg.Wait()

	return joined
}

// safeCall turns a panic in fn into an error, so one misbehaving component can't skip everyone else's shutdown.
func safeCall(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	return fn()
}

// stoppedByShutdown reports whether err is just ctx's own cancellation, i.e. the component stopped because shutdown
// began rather than because it failed.
func stoppedByShutdown(ctx context.Context, err error) bool {
	ctxErr := ctx.Err()
	return ctxErr != nil && errors.Is(err, ctxErr)
}

// createShutdownContext detaches from ctx's cancellation (it's usually already cancelled by the time
// shutdown starts) while keeping its values, then applies timeout if one is set.
func createShutdownContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}
