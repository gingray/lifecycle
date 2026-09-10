package lifecycle

import (
	"context"
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
	counter := 0
	logger := &testLogger{}
	root := DefaultRoot(logger)
	app := root.asNode(&testComponent{counter: func() { counter++ }})
	root.Then(&testComponent{counter: func() {
		counter++
	}}, &testComponent{counter: func() {
		counter++
	}}, app)
	err := root.Run(context.Background())
	assertions.ErrorIs(err, ErrComponentStop)
	assertions.Equal(3, counter, "3 components should be run")
}
