// Package lifecycle starts and gracefully stops the long-running parts of a Go
// application, such as HTTP servers, schedulers and message consumers.
//
// Components are arranged in a tree of Nodes. A node's children start once its
// Ready check passes, run concurrently with it, and are shut down before it, so
// shared dependencies outlive the components that use them. Each node must have
// exactly one parent.
//
// The whole tree stops when the root receives a signal, the ctx passed to Run
// is cancelled, or any Ready or Run fails or panics. A Run that returns nil
// stops only its own subtree, since its children depend on it; once a node's
// last child has returned, the node stops too, so a tree whose work is done
// exits on its own. Run must return once its ctx is cancelled, because a
// component's Shutdown isn't called until its Run has returned, and it should
// return nil only when its work is actually done.
//
// Node.Run returns nil on a clean stop, and otherwise every failure joined, each
// wrapped with the name of the component that failed.
package lifecycle
