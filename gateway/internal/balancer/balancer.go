package balancer

import (
	"pastebin/gateway/internal/nodepool"
)

type Balancer struct {
	algorithm Algorithm
}

func NewBalancer(algorithm Algorithm) *Balancer {
	return &Balancer{
		algorithm: algorithm,
	}
}

func (b *Balancer) GetNode(size int64, except []*nodepool.Node) (*nodepool.Node, error) {
	node, err := b.algorithm.Next(size, except)
	if err != nil {
		return nil, err
	}
	return node, nil
}
