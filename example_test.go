package lifecycle_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gingray/lifecycle"
)

type httpServer struct {
	lifecycle.BaseComponent
	server   *http.Server
	listener net.Listener
}

func (s *httpServer) Name() string { return "http-server" }

func (s *httpServer) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() { errCh <- s.server.Serve(s.listener) }()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return nil
	}
}

func (s *httpServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func ExampleDefaultRoot() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Println(err)
		return
	}

	root := lifecycle.DefaultRoot(nil, lifecycle.WithShutdownTimeout(5*time.Second))
	root.Then(&httpServer{server: &http.Server{ReadHeaderTimeout: time.Second}, listener: listener})

	// The timeout stands in for SIGTERM: either one stops the tree cleanly.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := root.Run(ctx); err != nil {
		fmt.Println("stopped with error:", err)
		return
	}
	fmt.Println("stopped cleanly")
	// Output: stopped cleanly
}
