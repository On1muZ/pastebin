package balancer

import (
	"fmt"
	"math/rand/v2"
	"pastebin/gateway/internal/nodepool"
)

type WeightedRandom struct {
	pool *nodepool.Pool
}

func NewWeightedRandom(pool *nodepool.Pool) Algorithm {
	return &WeightedRandom{
		pool: pool,
	}
}

func (w *WeightedRandom) Next(size int64, except []*nodepool.Node) (*nodepool.Node, error) {
	aliveNodes := w.pool.GetAllAliveNodes()
	if len(aliveNodes) == 0 {
		return nil, ErrNoNodes
	}
	var bestNode *nodepool.Node
	total := 0.0
	candidates := make([]*nodepool.Node, 0, len(aliveNodes))
	candidatesWeight := make([]float64, 0, len(aliveNodes))
	candidatesPicked := 0
OuterLoop:
	for _, node := range aliveNodes {
		for _, badNode := range except {
			if node == badNode {
				continue OuterLoop
			}
		}
		candidatesPicked++
		nodeStatus := node.GetStatus()
		if nodeStatus.FreeSpace-ReserveSpace < size {
			continue
		}
		candidates = append(candidates, node)
		weight := float64(nodeStatus.FreeSpace) / float64(nodeStatus.TotalSpace+1)
		candidatesWeight = append(candidatesWeight, weight)
		total += weight
		bestNode = node // default value, fix returns (nil, nil)
	}
	if len(candidates) == 0 {
		if candidatesPicked == 0 {
			return nil, fmt.Errorf("no nodes found, except list contains all alive nodes: %w", ErrNoNodes)
		}
		return nil, fmt.Errorf("no nodes have enough space to write file: %w", ErrNoNodesCanWriteThisFile)
	}
	needWeight := rand.Float64() * total
	for i := 0; i < len(candidates); i++ {
		needWeight -= candidatesWeight[i]
		if needWeight <= 0 {
			bestNode = candidates[i]
			break
		}
	}

	return bestNode, nil
}
