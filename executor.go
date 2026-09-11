package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// cancelPolicy decides when runAll cancels the ctx shared by the functions it runs.
type cancelPolicy int

const (
	cancelNever   cancelPolicy = iota // every function runs to completion
	cancelOnError                     // the first error cancels the rest
)

// process runs the node's component and its children concurrently (see runNode), then shuts the node's component
// down. Each child's Run shuts down its own subtree before returning, so children always stop before their parent.
func process(ctx context.Context, node *Node) error {
	name := node.Component.Name()
	parentFn := func(ctx context.Context) error {
		node.logger.Info("supervisor", "status", RunStart, "component", name)
		err := safeCall(func() error { return node.Component.Run(ctx) })
		if err == nil || stoppedByShutdown(ctx, err) {
			return nil
		}
		node.logger.Error("supervisor", "status", RunFailed, "component", name, "error", err)
		return fmt.Errorf("component %s: run: %w", name, err)
	}
	childFns := make([]func(context.Context) error, 0, len(node.Nodes))
	for _, child := range node.Nodes {
		childFns = append(childFns, child.Run)
	}
	joined := runNode(ctx, parentFn, childFns)

	node.logger.Info("supervisor", "status", ShutdownStart, "component", name)
	// Each node gets its own timeout, so a whole tree can take longer than shutdownTimeout to stop.
	shutdownCtx, cancel := createShutdownContext(ctx, node.shutdownTimeout)
	defer cancel()
	if err := safeCall(func() error { return node.Component.Shutdown(shutdownCtx) }); err != nil {
		node.logger.Error("supervisor", "status", ShutdownFailed, "component", name, "error", err)
		joined = errors.Join(joined, fmt.Errorf("component %s: shutdown: %w", name, err))
	}
	node.logger.Info("supervisor", "status", ShutdownFinish, "component", name)

	return joined
}

// runNode runs parentFn and childFns concurrently and returns all of their errors joined. Children depend on the
// parent, so:
//   - parentFn returning, with or without an error, cancels the children;
//   - a child returning an error cancels the parent and the other children;
//   - a child returning nil leaves the rest running, unless it was the last child still running: then the parent is
//     cancelled too, since nothing depends on it anymore.
func runNode(ctx context.Context, parentFn func(context.Context) error, childFns []func(context.Context) error) error {
	parentCtx, cancelParent := context.WithCancel(ctx)
	defer cancelParent()
	childrenCtx, cancelChildren := context.WithCancel(ctx)
	defer cancelChildren()

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		joined  error
		running atomic.Int64
	)
	record := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		joined = errors.Join(joined, err)
	}

	running.Store(int64(len(childFns)))
	wg.Go(func() {
		defer cancelChildren()
		if err := safeCall(func() error { return parentFn(parentCtx) }); err != nil {
			record(err)
		}
	})
	for _, fn := range childFns {
		wg.Go(func() {
			if err := safeCall(func() error { return fn(childrenCtx) }); err != nil {
				record(err)
				cancelChildren()
				cancelParent()
			}
			if running.Add(-1) == 0 {
				cancelParent()
			}
		})
	}
	wg.Wait()

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
			if policy == cancelOnError && err != nil {
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
	detached := context.WithoutCancel(ctx)
	if timeout <= 0 {
		return context.WithCancel(detached)
	}
	return context.WithTimeout(detached, timeout)
}
