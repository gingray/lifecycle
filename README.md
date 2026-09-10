# lifecycle

`lifecycle` is a small, dependency-free library for orchestrating the startup and graceful shutdown of long-running Go applications.

Modern services are rarely a single loop — an HTTP server, a background scheduler, a Kafka consumer, and the database pool they all rely on each have their own startup and teardown requirements, and those requirements depend on each other. `lifecycle` lets you describe those components as a tree, then handles readiness checks, concurrent execution, and reverse-dependency-order shutdown for you.

## Features

- **Dependency-ordered startup and shutdown** — model components as a tree of nodes; children start once their parent's `Ready` check passes and shut down before it, so shared dependencies (a database pool, a broker connection) always outlive the components that use them.
- **Readiness gating** — every component implements a `Ready` check that must succeed before it starts running.
- **Concurrent by default** — sibling components run and shut down concurrently, so independent parts of your app aren't held up by one another.
- **Signal-aware root** — `DefaultRoot` wires up `SIGINT`/`SIGTERM` handling out of the box (overridable via `WithSignals`), triggering an orderly shutdown of the whole tree.
- **Bounded shutdown** — optionally give each component's `Shutdown` a deadline with `WithShutdownTimeout`, independent of how long the app was running.
- **Clear results** — `Run` returns `nil` on a clean stop; failures, including panics, come back wrapped with the failing component's name.
- **Pluggable logging** — bring your own logger via a minimal `Logger` interface (`*slog.Logger` works as-is), or omit it entirely (defaults to a no-op `NopLogger`).
- **Zero-boilerplate components** — embed `BaseComponent` to get `Ready`/`Shutdown` handler registration for free.

## Installation

```sh
go get github.com/gingray/lifecycle
```

## Quick start

```go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gingray/lifecycle"
)

// HTTPServer is a Component that depends on the Database being ready.
type HTTPServer struct {
	lifecycle.BaseComponent
	server *http.Server
}

func (s *HTTPServer) Name() string { return "http-server" }

// Run must return once ctx is cancelled — Shutdown isn't called until it does.
func (s *HTTPServer) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() { errCh <- s.server.ListenAndServe() }()

	select {
	case err := <-errCh: // failed to start, e.g. port already in use
		return err
	case <-ctx.Done():
		return nil
	}
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func main() {
	logger := slog.Default()
	root := lifecycle.DefaultRoot(logger, lifecycle.WithShutdownTimeout(30*time.Second))

	db := root.ThenLast(NewDatabase())     // Ready before its children start; stops after them
	db.Then(NewHTTPServer(), NewScheduler(), NewKafkaConsumer())

	if err := root.Run(context.Background()); err != nil {
		logger.Error("lifecycle failed", "error", err)
	}
}
```

On `SIGINT`/`SIGTERM`, the root cancels its context, which unwinds the tree: dependent components (`HTTPServer`, `Scheduler`, `KafkaConsumer`) shut down first and concurrently, and only once they've finished does the `Database` they depend on shut down. `Run` then returns `nil`.

## How it works

Every component implements the `Component` interface:

```go
type Component interface {
	Name() string
	Ready(ctx context.Context) error
	Run(ctx context.Context) error
	Shutdown(ctx context.Context) error
}
```

`Run` must return once its `ctx` is cancelled: a component's `Shutdown` isn't called until its `Run` has returned. A `Run` that blocks without watching `ctx` (e.g. a bare `ListenAndServe()`) will stop the whole tree from shutting down.

Components are attached to a `Node` tree with `Then`, `ThenFirst`, `ThenLast`, and `ThenNth`. Each node's lifecycle is:

1. `Ready` is checked before the component is considered eligible to run.
2. The node's `Run` and its children's own `Ready` → `Run` sequences all start concurrently — there's no guarantee the parent's `Run` starts before its children's.
3. Shutdown begins when the root receives a shutdown signal, the `ctx` passed to `Run` is cancelled, any `Ready` or `Run` returns an error or panics, or any component returns from `Run`. The shared context is cancelled, which cascades: each node waits for its own component and all of its children to stop, then shuts its own component down exactly once — bottom-up, so a component is never shut down while something that depends on it is still running.

`Run` returns `nil` on a clean stop; a component returning `ctx.Err()` after shutdown has begun counts as clean too. Otherwise it returns every failure joined, each wrapped with the component and phase, e.g. `component db: ready: dial tcp 127.0.0.1:5432: connect: connection refused`.

Each node must have exactly one parent: adding the same node under two parents runs it twice. Components that depend on several others (a DAG rather than a tree) aren't supported yet.

`Node` also exposes a standalone `Shutdown(ctx)` method for tearing down a subtree manually (outside of `Run`), which applies the same bottom-up, all-children-visited semantics.

For components that don't need custom orchestration, embed `BaseComponent` and register handlers instead of implementing `Ready`/`Shutdown` directly:

```go
type Cache struct {
	lifecycle.BaseComponent
}

func NewCache() *Cache {
	c := &Cache{}
	c.AddReadyHandler(func(ctx context.Context) error { /* warm up */ return nil })
	c.AddShutdownHandler(func(ctx context.Context) error { /* flush */ return nil })
	return c
}
```

Handlers run concurrently. The first failing ready handler cancels the others; shutdown handlers all run to completion even if one fails.

## Configuration

`DefaultRoot` (and `GetNodeCreator`) accept functional options:

```go
root := lifecycle.DefaultRoot(logger,
	lifecycle.WithShutdownTimeout(30*time.Second), // deadline for each component's Shutdown; unset means none
	lifecycle.WithSignals(syscall.SIGTERM),         // override the default os.Interrupt + syscall.SIGTERM
)
```

The shutdown timeout applies to each component separately, not the whole tree: a parent's timeout starts only after its children have finished, so total shutdown time can exceed it. It's delivered as a `ctx` deadline, so it only takes effect if `Shutdown` respects `ctx`.

## Testing

Run the test suite with:

```sh
make test      # race detector on, output rendered by sift
make test-ci   # same tests, plain output
```

`make test` runs `go test -race ./... -json` piped through [`sift`](https://github.com/timtatt/sift), a helper that renders Go's JSON test output in a human-readable form. `sift` needs an interactive terminal, so CI uses `make test-ci`.

## Linting

```sh
make lint   # golangci-lint
make vet    # go vet
```

`lint` requires [`golangci-lint`](https://golangci-lint.run/) to be installed locally; the enabled linters are configured in `.golangci.yml`.

## License

MIT — see [LICENSE](LICENSE).
