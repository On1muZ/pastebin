package balancer

import (
	"fmt"
	"pastebin/gateway/internal/nodepool"
)

type LeastConnsByFreeSpace struct {
	pool *nodepool.Pool
}

func NewLeastConnsByFreeSpace(pool *nodepool.Pool) Algorithm {
	return &LeastConnsByFreeSpace{
		pool: pool,
	}
}

func (l *LeastConnsByFreeSpace) Next(size int64, except []*nodepool.Node) (*nodepool.Node, error) {
	aliveNodes := l.pool.GetAllAliveNodes()
	if len(aliveNodes) == 0 {
		return nil, ErrNoNodes
	}
	maxWeight := -1.0
	var bestNode *nodepool.Node
	candidates := 0
OuterLoop:
	for _, node := range aliveNodes {
		for _, badNode := range except {
			if node == badNode {
				continue OuterLoop
			}
		}
		candidates++
		nodeStatus := node.GetStatus()
		if nodeStatus.FreeSpace-ReserveSpace < size {
			continue
		}

		weight := (float64(nodeStatus.FreeSpace) / float64(nodeStatus.TotalSpace+1)) / float64(node.GetAliveConns()+1)
		if weight > maxWeight {
			maxWeight = weight
			bestNode = node
		}
	}
	if bestNode == nil {
		if candidates == 0 {
			return nil, fmt.Errorf("no nodes found, except list contains all alive nodes: %w", ErrNoNodes)
		}
		return nil, fmt.Errorf("no node has enough free space for %d bytes: %w", size, ErrNoNodesCanWriteThisFile)
	}
	return bestNode, nil
}
