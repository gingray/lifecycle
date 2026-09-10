package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
)

type DefaultStrategy struct {
}

func (i *DefaultStrategy) Process(ctx context.Context, node *Node) error {
	g, errCtx := errgroup.WithContext(ctx)
	wg := sync.WaitGroup{}
	wg.Add(1)
	g.Go(func() error {
		node.logger.Info("supervisor", "status", Run, "component", node.Component.Name())
		wg.Done()
		return node.Component.Run(errCtx)
	})
	wg.Wait()

	for _, node := range node.Nodes {
		g.Go(func() error {
			return node.Run(errCtx)
		})
	}
	err := g.Wait()
	for _, component := range node.Nodes {
		compErr := component.Shutdown(ctx)
		err = errors.Join(err, compErr)
	}
	node.logger.Info("supervisor", "status", ShutdownStart, "component", node.Component.Name())
	stopErr := fmt.Errorf("component: %s, %w", node.Component.Name(), ErrComponentStop)
	err = errors.Join(err, node.Component.Shutdown(ctx), stopErr)
	node.logger.Info("supervisor", "status", ShutdownFinish, "component", node.Component.Name())
	return err
}
