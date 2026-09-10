package lifecycle

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testLogger struct {
}

func (t *testLogger) Info(msg string, args ...any) {
}

func (t *testLogger) Warn(msg string, args ...any) {
}

func (t *testLogger) Error(msg string, args ...any) {
}

type testComponent struct {
	counter func()
}

func (t *testComponent) Name() string {
	return "test-component"
}

func (t *testComponent) Ready(ctx context.Context) error {
	t.counter()
	return nil
}

func (t *testComponent) Run(ctx context.Context) error {
	return nil
}

func (t *testComponent) Shutdown(ctx context.Context) error {
	return nil
}

func TestLifecycle(t *testing.T) {
	assertions := assert.New(t)
	var counter atomic.Int32
	logger := &testLogger{}
	root := DefaultRoot(logger)
	app := root.asNode(&testComponent{counter: func() { counter.Add(1) }})
	root.Then(&testComponent{counter: func() {
		counter.Add(1)
	}}, &testComponent{counter: func() {
		counter.Add(1)
	}}, app)
	err := root.Run(context.Background())
	assertions.ErrorIs(err, ErrComponentStop)
	assertions.EqualValues(3, counter.Load(), "3 components should be run")
}

type countingComponent struct {
	name          string
	shutdownCount atomic.Int32
}

func (c *countingComponent) Name() string                    { return c.name }
func (c *countingComponent) Ready(ctx context.Context) error { return nil }
func (c *countingComponent) Run(ctx context.Context) error   { return nil }
func (c *countingComponent) Shutdown(ctx context.Context) error {
	c.shutdownCount.Add(1)
	return nil
}

func TestShutdownCalledExactlyOnce(t *testing.T) {
	assertions := assert.New(t)
	logger := &testLogger{}
	root := DefaultRoot(logger)

	child := &countingComponent{name: "child"}
	grandchild := &countingComponent{name: "grandchild"}

	childNode := root.ThenLast(child)
	childNode.Then(grandchild)

	err := root.Run(context.Background())

	assertions.ErrorIs(err, ErrComponentStop)
	assertions.EqualValues(1, child.shutdownCount.Load(), "child should be shut down exactly once")
	assertions.EqualValues(1, grandchild.shutdownCount.Load(), "grandchild should be shut down exactly once")
}
