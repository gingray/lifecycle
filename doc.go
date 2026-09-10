// Package lifecycle starts and gracefully stops the long-running parts of a Go
// application, such as HTTP servers, schedulers and message consumers.
//
// Components are arranged in a tree of Nodes. A node's children start once its
// Ready check passes, run concurrently with it, and are shut down before it, so
// shared dependencies outlive the components that use them. Each node must have
// exactly one parent.
//
// Shutdown of the whole tree begins when the root receives a signal, the ctx
// passed to Run is cancelled, any Ready or Run fails or panics, or any
// component's Run returns. Run must therefore return once its ctx is cancelled:
// a component's Shutdown isn't called until its Run has returned.
//
// Node.Run returns nil on a clean stop, and otherwise every failure joined, each
// wrapped with the name of the component that failed.
package lifecycle
