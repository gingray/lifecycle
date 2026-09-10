# lifecycle

`lifecycle` is a small, dependency-free framework for orchestrating the startup and graceful shutdown of long-running Go applications.

Modern services are rarely a single loop — an HTTP server, a background scheduler, a Kafka consumer, and the database pool they all rely on each have their own startup and teardown requirements, and those requirements depend on each other. `lifecycle` lets you describe those components as a tree, then handles readiness checks, concurrent execution, and reverse-dependency-order shutdown for you.

## Features

- **Dependency-ordered startup and shutdown** — model components as a tree of nodes; children start once their parent is running and shut down before it, so shared dependencies (a database pool, a broker connection) always outlive the components that use them.
- **Readiness gating** — every component implements a `Ready` check that must succeed before it starts running.
- **Concurrent by default** — sibling components run and shut down concurrently via `errgroup`, so independent parts of your app aren't held up by one another.
- **Signal-aware root** — `DefaultRoot` wires up `SIGINT`/`SIGTERM` handling out of the box, triggering an orderly shutdown of the whole tree.
- **Pluggable logging** — bring your own logger via a minimal `Logger` interface.
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

	"github.com/gingray/lifecycle"
)

// HTTPServer is a Component that depends on the Database being ready.
type HTTPServer struct {
	lifecycle.BaseComponent
	server *http.Server
}

func (s *HTTPServer) Name() string { return "http-server" }

func (s *HTTPServer) Run(ctx context.Context) error {
	return s.server.ListenAndServe()
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func main() {
	logger := slog.Default()
	root := lifecycle.DefaultRoot(logger)

	db := root.ThenLast(NewDatabase())     // starts first, stops last
	db.Then(NewHTTPServer(), NewScheduler(), NewKafkaConsumer())

	if err := root.Run(context.Background()); err != nil {
		logger.Error("lifecycle stopped", "error", err)
	}
}
```

On `SIGINT`/`SIGTERM`, the root cancels its context, which unwinds the tree: dependent components (`HTTPServer`, `Scheduler`, `KafkaConsumer`) shut down first and concurrently, and only once they've finished does the `Database` they depend on shut down.

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

Components are attached to a `Node` tree with `Then`, `ThenFirst`, `ThenLast`, and `ThenNth`. Each node's lifecycle is:

1. `Ready` is checked before the component is considered eligible to run.
2. `Run` starts, and the node's children begin their own `Ready` → `Run` sequence, all running concurrently.
3. When the context is cancelled (by a shutdown signal, an error, or any component returning), children are shut down first, depth-first and in reverse order of startup, followed by the node itself.

This ordering guarantees that a component is never shut down while something that depends on it is still running.

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

## Testing

Run the test suite with:

```sh
make test
```

This runs `go test ./... -json` piped through [`sift`](https://github.com/timtatt/sift), a helper that renders Go's JSON test output in a human-readable form.
