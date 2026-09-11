package lifecycle

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ Logger = (*slog.Logger)(nil)

type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) record(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

type fakeComponent struct {
	name        string
	recorder    *recorder
	readyErr    error
	runErr      error
	runPanic    any
	blockRun    bool
	shutdownErr error
	shutdownCtx context.Context
}

func (c *fakeComponent) Name() string { return c.name }

func (c *fakeComponent) Ready(_ context.Context) error {
	c.recorder.record("ready:" + c.name)
	return c.readyErr
}

func (c *fakeComponent) Run(ctx context.Context) error {
	c.recorder.record("run:" + c.name)
	if c.runPanic != nil {
		panic(c.runPanic)
	}
	if c.blockRun {
		<-ctx.Done()
		return ctx.Err()
	}
	return c.runErr
}

func (c *fakeComponent) Shutdown(ctx context.Context) error {
	c.recorder.record("shutdown:" + c.name)
	c.shutdownCtx = ctx
	return c.shutdownErr
}

func startRun(ctx context.Context, root *Node) <-chan error {
	done := make(chan error, 1)
	go func() { done <- root.Run(ctx) }()
	return done
}

func waitFor(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		require.FailNow(t, "Run did not return within 5s")
		return nil
	}
}

func count(events []string, event string) int {
	n := 0
	for _, e := range events {
		if e == event {
			n++
		}
	}
	return n
}

func TestCancelledContextStopsTreeCleanly(t *testing.T) {
	rec := &recorder{}
	root := DefaultRoot(nil)
	root.ThenLast(&fakeComponent{name: "parent", recorder: rec, blockRun: true}).
		Then(&fakeComponent{name: "child", recorder: rec, blockRun: true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := startRun(ctx, root)
	require.Eventually(t, func() bool {
		events := rec.snapshot()
		return slices.Contains(events, "run:parent") && slices.Contains(events, "run:child")
	}, time.Second, time.Millisecond)
	cancel()
	err := waitFor(t, done)

	assert.NoError(t, err)
	events := rec.snapshot()
	assert.Equal(t, 1, count(events, "shutdown:parent"))
	assert.Equal(t, 1, count(events, "shutdown:child"))
	assert.Less(t, slices.Index(events, "ready:parent"), slices.Index(events, "ready:child"), "parent must be ready before child starts")
	assert.Less(t, slices.Index(events, "shutdown:child"), slices.Index(events, "shutdown:parent"), "child must shut down before parent")
}

func TestRunErrorStopsTreeAndNamesComponent(t *testing.T) {
	boom := errors.New("boom")
	rec := &recorder{}
	root := DefaultRoot(nil)
	root.Then(
		&fakeComponent{name: "a", recorder: rec, blockRun: true},
		&fakeComponent{name: "b", recorder: rec, runErr: boom},
	)

	err := waitFor(t, startRun(context.Background(), root))

	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
	assert.ErrorContains(t, err, "component b: run: boom")
	assert.NotContains(t, err.Error(), "component a", "a stopped because of shutdown, which isn't a failure")
	events := rec.snapshot()
	assert.Equal(t, 1, count(events, "shutdown:a"))
	assert.Equal(t, 1, count(events, "shutdown:b"))
}

func TestReadyErrorSkipsChildren(t *testing.T) {
	boom := errors.New("boom")
	rec := &recorder{}
	root := DefaultRoot(nil)
	root.ThenLast(&fakeComponent{name: "parent", recorder: rec, readyErr: boom}).
		Then(&fakeComponent{name: "child", recorder: rec, blockRun: true})

	err := waitFor(t, startRun(context.Background(), root))

	assert.ErrorIs(t, err, boom)
	assert.ErrorContains(t, err, "component parent: ready: boom")
	events := rec.snapshot()
	assert.NotContains(t, events, "run:parent")
	assert.NotContains(t, events, "shutdown:parent")
	assert.NotContains(t, events, "ready:child")
}

func TestRunReturningFromComponentWithChildrenStopsTree(t *testing.T) {
	rec := &recorder{}
	root := DefaultRoot(nil)
	root.ThenLast(&fakeComponent{name: "parent", recorder: rec}).
		Then(&fakeComponent{name: "child", recorder: rec, blockRun: true})

	err := waitFor(t, startRun(context.Background(), root))

	assert.NoError(t, err)
	events := rec.snapshot()
	assert.Equal(t, 1, count(events, "shutdown:parent"))
	assert.Equal(t, 1, count(events, "shutdown:child"))
}

func TestCleanChildReturnKeepsSiblingsAndParentRunning(t *testing.T) {
	rec := &recorder{}
	root := DefaultRoot(nil)
	root.ThenLast(&fakeComponent{name: "parent", recorder: rec, blockRun: true}).
		Then(
			&fakeComponent{name: "a", recorder: rec},
			&fakeComponent{name: "b", recorder: rec, blockRun: true},
		)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := startRun(ctx, root)
	require.Eventually(t, func() bool {
		events := rec.snapshot()
		return slices.Contains(events, "shutdown:a") && slices.Contains(events, "run:b") && slices.Contains(events, "run:parent")
	}, time.Second, time.Millisecond)
	assert.Never(t, func() bool {
		events := rec.snapshot()
		return slices.Contains(events, "shutdown:b") || slices.Contains(events, "shutdown:parent")
	}, 50*time.Millisecond, time.Millisecond, "b and parent must keep running after a finishes cleanly")
	cancel()
	err := waitFor(t, done)

	assert.NoError(t, err)
	events := rec.snapshot()
	assert.Equal(t, 1, count(events, "shutdown:b"))
	assert.Equal(t, 1, count(events, "shutdown:parent"))
	assert.Less(t, slices.Index(events, "shutdown:b"), slices.Index(events, "shutdown:parent"), "child must shut down before parent")
}

func TestAllChildrenFinishingStopsParent(t *testing.T) {
	rec := &recorder{}
	root := DefaultRoot(nil)
	root.ThenLast(&fakeComponent{name: "parent", recorder: rec, blockRun: true}).
		Then(
			&fakeComponent{name: "a", recorder: rec},
			&fakeComponent{name: "b", recorder: rec},
		)

	err := waitFor(t, startRun(context.Background(), root))

	assert.NoError(t, err)
	events := rec.snapshot()
	assert.Equal(t, 1, count(events, "shutdown:a"))
	assert.Equal(t, 1, count(events, "shutdown:b"))
	assert.Equal(t, 1, count(events, "shutdown:parent"))
	assert.Less(t, slices.Index(events, "shutdown:a"), slices.Index(events, "shutdown:parent"), "child must shut down before parent")
	assert.Less(t, slices.Index(events, "shutdown:b"), slices.Index(events, "shutdown:parent"), "child must shut down before parent")
}

func TestNestedChildErrorStopsWholeTree(t *testing.T) {
	boom := errors.New("boom")
	rec := &recorder{}
	root := DefaultRoot(nil)
	root.ThenLast(&fakeComponent{name: "parent", recorder: rec, blockRun: true}).
		Then(&fakeComponent{name: "child", recorder: rec, runErr: boom})
	root.Then(&fakeComponent{name: "sibling", recorder: rec, blockRun: true})

	err := waitFor(t, startRun(context.Background(), root))

	assert.ErrorIs(t, err, boom)
	assert.ErrorContains(t, err, "component child: run: boom")
	events := rec.snapshot()
	for _, name := range []string{"child", "parent", "sibling"} {
		assert.Equal(t, 1, count(events, "shutdown:"+name), name)
	}
}

func TestPanicInRunIsReturnedAndTreeShutsDown(t *testing.T) {
	rec := &recorder{}
	root := DefaultRoot(nil)
	root.Then(
		&fakeComponent{name: "a", recorder: rec, blockRun: true},
		&fakeComponent{name: "b", recorder: rec, runPanic: "kaboom"},
	)

	err := waitFor(t, startRun(context.Background(), root))

	assert.ErrorContains(t, err, "component b: run: panic: kaboom")
	events := rec.snapshot()
	assert.Equal(t, 1, count(events, "shutdown:a"))
	assert.Equal(t, 1, count(events, "shutdown:b"))
}

func TestFailingShutdownHandlerDoesNotCancelOthers(t *testing.T) {
	failed := make(chan struct{})
	component := &Component{}
	component.AddShutdownHandler(func(_ context.Context) error {
		close(failed)
		return errors.New("boom")
	})
	component.AddShutdownHandler(func(ctx context.Context) error {
		<-failed
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return nil
		}
	})

	err := component.Shutdown(context.Background())

	assert.EqualError(t, err, "boom")
}

func TestHandlerErrorNotDuplicated(t *testing.T) {
	component := &Component{}
	component.AddReadyHandler(func(_ context.Context) error { return errors.New("boom") })

	err := component.Ready(context.Background())

	assert.EqualError(t, err, "boom")
}

type ctxKey struct{}

func TestShutdownContextHasTimeoutAndKeepsValues(t *testing.T) {
	component := &fakeComponent{name: "a", recorder: &recorder{}}
	root := DefaultRoot(nil, WithShutdownTimeout(time.Minute))
	root.Then(component)
	ctx := context.WithValue(context.Background(), ctxKey{}, "trace-1")

	err := waitFor(t, startRun(ctx, root))

	require.NoError(t, err)
	require.NotNil(t, component.shutdownCtx)
	deadline, ok := component.shutdownCtx.Deadline()
	assert.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(time.Minute), deadline, 5*time.Second)
	assert.Equal(t, "trace-1", component.shutdownCtx.Value(ctxKey{}))
}

func TestThenVariantsReturnNewlyAddedNodes(t *testing.T) {
	root := DefaultRoot(nil)
	root.Then(&fakeComponent{name: "existing"})
	a, b := &fakeComponent{name: "a"}, &fakeComponent{name: "b"}
	c, d := &fakeComponent{name: "c"}, &fakeComponent{name: "d"}

	assert.Same(t, a, root.ThenFirst(a, b).Component)
	assert.Same(t, d, root.ThenNth(2, c, d).Component)
	assert.Panics(t, func() { root.ThenFirst() })
	assert.Panics(t, func() { root.ThenNth(0, &fakeComponent{name: "e"}) })
	assert.Panics(t, func() { root.ThenNth(2, &fakeComponent{name: "f"}) })
}

func TestNilLoggerDoesNotPanic(t *testing.T) {
	root := DefaultRoot(nil)
	root.Then(&fakeComponent{name: "a", recorder: &recorder{}})

	assert.NotPanics(t, func() { _ = root.Run(context.Background()) })
}
