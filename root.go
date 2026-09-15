package lifecycle

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// RootComponent sits at the top of a tree. It stops when one of its signals arrives or its ctx is cancelled.
type RootComponent struct {
	Component
	signals      []os.Signal
	drainTimeout time.Duration
}

// preShutdownKey is the ctx key under which the drain channel travels down the tree.
type preShutdownKey struct{}

// stopContextKey is the ctx key under which the root's runner passes the caller's ctx to the root component, so
// the root can watch it after the rest of the tree has been detached from its cancellation.
type stopContextKey struct{}

// PreShutdownDone returns a channel that is closed as soon as the tree has been asked to stop, before any component
// is cancelled, so a component can drain in-flight work, fail its readiness probe, or stop accepting new work while
// the rest of the tree keeps running. How long the tree keeps running after that is set with WithDrainTimeout.
//
// The channel is only available under a root built with DefaultRoot. For any other ctx the result is nil, and a
// receive on it blocks forever, so always select on ctx.Done() as well.
func PreShutdownDone(ctx context.Context) <-chan struct{} {
	ch, _ := ctx.Value(preShutdownKey{}).(chan struct{})
	return ch
}

// Name returns "root".
func (r *RootComponent) Name() string {
	return "root"
}

// Run blocks until a signal arrives, the caller's ctx is cancelled, or ctx itself is cancelled because the tree
// below has stopped on its own. It then closes the channel returned by PreShutdownDone and, if a drain timeout is
// set, waits for it before returning. The wait ends early when the tree stops on its own. All of these are a clean
// stop, so it returns nil.
func (r *RootComponent) Run(ctx context.Context) error {
	stopCtx := ctx
	if outer, ok := ctx.Value(stopContextKey{}).(context.Context); ok {
		stopCtx = outer
	}
	sigCtx, stop := signal.NotifyContext(stopCtx, r.signals...)
	defer stop()

	select {
	case <-sigCtx.Done():
	case <-ctx.Done():
	}

	if drain, ok := ctx.Value(preShutdownKey{}).(chan struct{}); ok {
		close(drain)
	}
	if r.drainTimeout > 0 {
		timer := time.NewTimer(r.drainTimeout)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
	}
	return nil
}

// NewRootComponent creates a RootComponent that triggers shutdown on the signals set with WithSignals, defaulting
// to os.Interrupt and syscall.SIGTERM, and drains for the duration set with WithDrainTimeout. Only those two
// options are read. A root created this way and attached with GetNodeCreator handles signals but has no drain
// window: PreShutdownDone returns nil for its subtree. Use DefaultRoot to get both.
func NewRootComponent(opts ...Option) *RootComponent {
	return newRootComponent(newConfig(opts))
}

func newRootComponent(cfg *config) *RootComponent {
	root := &RootComponent{}
	root.signals = cfg.signals
	root.drainTimeout = cfg.drainTimeout
	if len(root.signals) <= 0 {
		root.signals = []os.Signal{os.Interrupt, syscall.SIGTERM}
	}
	return root
}
