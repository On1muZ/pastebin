package checker

import (
	"context"
	"fmt"
	"log/slog"
	"pastebin/gateway/internal/nodepool"
	"pastebin/gateway/internal/transport"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"sync"
	"time"
)

type Checker struct {
	pool      *nodepool.Pool
	transport *transport.Transport
	log       *slog.Logger
	timeout   time.Duration
}

func New(pool *nodepool.Pool, transport *transport.Transport, log *slog.Logger, timeout time.Duration) *Checker {
	return &Checker{
		pool:      pool,
		transport: transport,
		log:       log,
		timeout:   timeout,
	}
}

func (c *Checker) CheckOnce(ctx context.Context) {
	var wg sync.WaitGroup
	for _, node := range c.pool.GetAllNodes() {
		wg.Go(func() {
			timeoutCtx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()
			status, err := c.checkNode(timeoutCtx, node)
			if err != nil {
				if node.IsAlive() {
					c.log.Warn("node down", slog.Any("error", err), slog.String("id", node.ID), slog.String("addr", node.Addr))
					node.MarkDead()
				}
				return
			}

			node.ChangeStatus(*status)
			if !node.IsAlive() {
				c.log.Info("node up", slog.String("id", node.ID), slog.String("addr", node.Addr))
				node.MarkAlive()
			}
		})
	}
	wg.Wait()
}

func (c *Checker) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.log.Info("checker stopping work because ctx closed")
			return
		case <-ticker.C:
			c.CheckOnce(ctx)
		}
	}
}

func (c *Checker) checkNode(ctx context.Context, node *nodepool.Node) (*nodepool.Status, error) {
	var status *nodepool.Status
	t, err := c.transport.Client(node)
	if err != nil {
		return nil, fmt.Errorf("could not get transport to node: %s, err: %w", node.ID, err)
	}
	s, err := t.GetStatus(ctx, &storagenodev1.GetStatusRequest{})
	if err != nil {
		return nil, err
	}
	status = &nodepool.Status{
		TotalSpace: s.TotalBytes,
		FreeSpace:  s.FreeBytes,
	}
	return status, nil
}
