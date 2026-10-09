package balancer

import (
	"errors"
	"fmt"
	"pastebin/gateway/internal/nodepool"
)

var ErrNoNodes error = errors.New("no nodes found")
var ErrNoNodesCanWriteThisFile error = errors.New("file too big to be written on any node")

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
		if nodeStatus.FreeSpace-1024*1024*10 < size {
			continue
		}

		weight := (float64(nodeStatus.FreeSpace) / float64(nodeStatus.TotalSpace)) / float64(node.GetAliveConns()+1)
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
