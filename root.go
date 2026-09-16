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

// PreShutdownDone returns a channel that is closed when the root receives a stop signal, before any component is
// cancelled, so a component can drain in-flight work, fail its readiness probe, or stop accepting new work while
// the rest of the tree keeps running. How long the tree keeps running after that is set with WithDrainTimeout.
//
// Only a signal opens that window. Cancelling the ctx passed to the root's Run is deliberately an immediate stop:
// the channel is closed too, but every component is cancelled at the same instant, with no drain even when a
// drain timeout is set. Run the root under context.Background() so every stop goes through the signal path, and
// cancel the ctx yourself only when the app must stop right now and draining is not an option.
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

// Run blocks until a signal arrives or ctx is cancelled, then closes the channel returned by PreShutdownDone. After
// a signal it also waits out the drain timeout, if one is set, before returning; the children keep running until it
// does. The wait ends as soon as ctx is cancelled, which is how the tree reports that it stopped on its own, so a
// cancelled ctx never waits. Both are a clean stop, so it returns nil.
func (r *RootComponent) Run(ctx context.Context) error {
	sigCtx, stop := signal.NotifyContext(ctx, r.signals...)
	defer stop()

	<-sigCtx.Done()

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
