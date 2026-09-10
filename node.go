package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Status values logged under the "status" key as a component moves through its lifecycle.
const (
	ReadyCheckStart  = "ready-check-start"
	ReadyCheckFinish = "ready-check-finish"
	ReadyCheckFailed = "ready-check-failed"
	RunStart         = "running"
	RunFailed        = "run-failed"
	ShutdownStart    = "shutdown-start"
	ShutdownFinish   = "shutdown-finish"
	ShutdownFailed   = "shutdown-failed"
)

// Node is a component in a lifecycle tree, together with its children. Build trees with DefaultRoot or
// GetNodeCreator and attach children with Then and its variants. A node must have exactly one parent:
// adding the same node under two parents runs it twice.
type Node struct {
	Component       Component
	Nodes           []*Node
	logger          Logger
	shutdownTimeout time.Duration
	nodeCreator     func(component Component) *Node
}

// DefaultRoot returns a root node that stops the whole tree on os.Interrupt or syscall.SIGTERM (see WithSignals),
// or when the ctx passed to Run is cancelled. A nil logger means NopLogger.
func DefaultRoot(logger Logger, opts ...Option) *Node {
	cfg := newConfig(opts)
	creator := newNodeCreator(logger, cfg)
	return creator(NewRootComponent(cfg.signals...))
}

// GetNodeCreator returns a function that wraps components in nodes sharing logger and opts, for building a tree
// without the default root. A nil logger means NopLogger.
func GetNodeCreator(logger Logger, opts ...Option) func(component Component) *Node {
	return newNodeCreator(logger, newConfig(opts))
}

func newNodeCreator(logger Logger, cfg *config) func(component Component) *Node {
	if logger == nil {
		logger = NopLogger{}
	}

	var creator func(component Component) *Node
	creator = func(component Component) *Node {
		return &Node{
			Component:       component,
			Nodes:           []*Node{},
			logger:          logger,
			shutdownTimeout: cfg.shutdownTimeout,
			nodeCreator:     creator,
		}
	}
	return creator
}

// Then attaches components as children. A *Node is attached as-is; any other Component is wrapped in a new node.
func (n *Node) Then(component ...Component) {
	for _, item := range component {
		switch c := item.(type) {
		case *Node:
			n.Nodes = append(n.Nodes, c)
		default:
			n.Nodes = append(n.Nodes, n.nodeCreator(c))
		}
	}
}

// ThenLast attaches components as children and returns the node for the last of them.
// It panics if no components are given.
func (n *Node) ThenLast(component ...Component) *Node {
	requirePosition("ThenLast", 1, component)
	n.Then(component...)
	return n.Nodes[len(n.Nodes)-1]
}

// ThenFirst attaches components as children and returns the node for the first of them.
// It panics if no components are given.
func (n *Node) ThenFirst(component ...Component) *Node {
	requirePosition("ThenFirst", 1, component)
	start := len(n.Nodes)
	n.Then(component...)
	return n.Nodes[start]
}

// ThenNth attaches components as children and returns the node for the nth of them, counting from 1.
// It panics if nth is out of range for the components given.
func (n *Node) ThenNth(nth int, component ...Component) *Node {
	requirePosition("ThenNth", nth, component)
	start := len(n.Nodes)
	n.Then(component...)
	return n.Nodes[start+nth-1]
}

func requirePosition(method string, position int, components []Component) {
	if position < 1 || position > len(components) {
		panic(fmt.Sprintf("lifecycle: %s: position %d is out of range for %d component(s)", method, position, len(components)))
	}
}

// Shutdown tears down this node's subtree directly, without going through
// Run. Children are shut down before the node itself; every child is
// visited, and every error is joined, even if an earlier child fails.
func (n *Node) Shutdown(ctx context.Context) error {
	var err error
	for _, child := range n.Nodes {
		err = errors.Join(err, child.Shutdown(ctx))
	}
	return errors.Join(err, n.Component.Shutdown(ctx))
}

// Name returns the component's name.
func (n *Node) Name() string {
	return n.Component.Name()
}

// Ready runs this node's own readiness check, not its children's.
func (n *Node) Ready(ctx context.Context) error {
	return n.Component.Ready(ctx)
}

// Run checks Ready, then runs the component and its children concurrently until any of them returns or ctx is
// cancelled, and shuts the subtree down bottom-up. It returns nil on a clean stop; otherwise every Ready, Run and
// Shutdown failure joined, each wrapped with the component's name.
func (n *Node) Run(ctx context.Context) error {
	name := n.Component.Name()
	n.logger.Info("supervisor", "status", ReadyCheckStart, "component", name)
	err := safeCall(func() error { return n.Component.Ready(ctx) })
	if stoppedByShutdown(ctx, err) {
		return nil
	}
	if err != nil {
		n.logger.Error("supervisor", "status", ReadyCheckFailed, "component", name, "error", err)
		return fmt.Errorf("component %s: ready: %w", name, err)
	}
	n.logger.Info("supervisor", "status", ReadyCheckFinish, "component", name)

	return process(ctx, n)
}
