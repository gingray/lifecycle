# lifecycle

`lifecycle` is a small, dependency-free library for orchestrating the startup and graceful shutdown of long-running Go applications.

Modern services are rarely a single loop — an HTTP server, a background scheduler, a Kafka consumer, and the database pool they all rely on each have their own startup and teardown requirements, and those requirements depend on each other. `lifecycle` lets you describe those components as a tree, then handles readiness checks, concurrent execution, and reverse-dependency-order shutdown for you.

## Features

- **Dependency-ordered startup and shutdown** — model components as a tree of nodes; children start once their parent's `Ready` check passes and shut down before it, so shared dependencies (a database pool, a broker connection) always outlive the components that use them.
- **Readiness gating** — every component implements a `Ready` check that must succeed before it starts running.
- **Concurrent by default** — sibling components run and shut down concurrently, so independent parts of your app aren't held up by one another.
- **Predictable stopping** — a failure anywhere stops the whole app, while a component that finishes cleanly stops only what depends on it, so a tree whose work is done exits on its own. See [Execution model](#execution-model).
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
3. The tree keeps running until something makes (part of) it stop: a signal, a cancelled `ctx`, a failure, or a component finishing its work. Which components stop in each case is described in [Execution model](#execution-model).
4. Stopping cascades bottom-up: each node waits for its own component and all of its children to stop, then shuts its own component down exactly once, so a component is never shut down while something that depends on it is still running.

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

## Execution model

Each node runs its own component's `Run` and all of its children's subtrees concurrently. What happens when one of them returns follows the direction of the dependencies: a child depends on its parent, so a parent is never stopped while its children still need it, and children never keep running once their parent is gone.

### When the app shuts down

| Trigger | What stops | Reported as |
|---|---|---|
| The root receives `SIGINT`/`SIGTERM` (or a signal set with `WithSignals`) | The whole tree | Clean stop: `root.Run` returns `nil` |
| The `ctx` passed to `root.Run` is cancelled | The whole tree | Clean stop: `root.Run` returns `nil` |
| A component's `Ready` returns an error or panics | The whole tree. That component's `Run` and its children never start | `component <name>: ready: <err>` |
| A component's `Run` returns an error or panics | The whole tree | `component <name>: run: <err>` |
| A component's `Shutdown` returns an error or panics | Everything still running, since this is a failure too | `component <name>: shutdown: <err>` |
| A component's `Run` returns `nil` | Only that component's subtree: its children stop first, then it shuts down. Its siblings and parent keep running | Not an error; the app keeps running |
| The last running child of a component returns `nil` | That component too, since nothing depends on it anymore. This cascades upward: when the root's last child finishes, the app exits | Clean stop: `root.Run` returns `nil` |

A component that returns `nil` or `ctx.Err()` after its `ctx` was cancelled has stopped cleanly, not failed. When several components fail, `root.Run` returns all of their errors joined.

### Examples

The examples below use this tree:

```
root
└── db
    ├── http-server
    ├── scheduler
    └── cache-warmup   (one-shot: returns nil once the cache is warm)
```

- **`SIGTERM` arrives:** the root cancels its context. `http-server`, `scheduler`, and `cache-warmup` (if it's still running) stop and shut down concurrently. Then `db` shuts down, then the root. `root.Run` returns `nil`.
- **`cache-warmup` finishes:** `cache-warmup` shuts down. `http-server`, `scheduler`, and `db` keep running.
- **`scheduler`'s `Run` returns an error:** `http-server`, `cache-warmup`, and `db` are cancelled. The children shut down first, then `db`, then the root. `root.Run` returns `component scheduler: run: <err>`.
- **`db`'s `Run` returns an error** (e.g. the connection is lost): its children are cancelled and shut down first, then `db`. The error then stops the rest of the tree, and `root.Run` returns `component db: run: <err>`. If `db` returned `nil` instead, the same components would stop, and since `db` is the root's only child, the app would exit with `nil`.
- **A batch tree `root → db → migrate`:** once `migrate` returns `nil`, nothing depends on `db` anymore, so `db` stops, then the root. `root.Run` returns `nil` without waiting for a signal.

### Guarantees

- A child's `Ready` is checked only after its parent's `Ready` has passed. There's no guarantee about the order in which a parent's and its children's `Run` start.
- A component's `Shutdown` is called exactly once, after its own `Run` and all of its children have returned. Siblings shut down concurrently.
- A component whose `Ready` didn't pass is never run or shut down.
- `WithShutdownTimeout` applies to each component's `Shutdown` separately (see [Configuration](#configuration)).

### Writing components for this model

- `Run` must return once its `ctx` is cancelled. Otherwise its `Shutdown`, and everything above it, is never reached.
- Return an error when the component can no longer do its job. Returning `nil` early from a long-running component (for example, treating a server stopping unexpectedly as success) makes it stop quietly while the rest of the app keeps running.
- Return `nil` only when the work is actually done, like a migration or a warm-up job.

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
