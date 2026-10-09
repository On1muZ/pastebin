package balancer

import (
	"log/slog"
	"pastebin/gateway/internal/nodepool"
)

type Balancer struct {
	pool      *nodepool.Pool
	algorithm Algorithm
	log       *slog.Logger
}

func NewBalancer(pool *nodepool.Pool, algorithm Algorithm, log *slog.Logger) *Balancer {
	return &Balancer{
		pool:      pool,
		algorithm: algorithm,
		log:       log,
	}
}

func (b *Balancer) GetNode(size int64, except []*nodepool.Node) (*nodepool.Node, error) {
	node, err := b.algorithm.Next(size, except)
	if err != nil {
		return nil, err
	}
	return node, nil
}
