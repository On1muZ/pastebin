package nodepool

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
)

var ErrPoolContainsDuplicatedValues error = errors.New("pool contains duplicated values")

type Pool struct {
	nodes atomic.Pointer[[]*Node]
	mu    sync.Mutex
}

func NewPool(nodes []*Node) (*Pool, error) {
	p := &Pool{}
	n := slices.Clone(nodes)
	p.nodes.Store(&n)
	_, err := p.checkDuplicateNodeID([]*Node{})
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Pool) GetAllAliveNodes() []*Node {
	nodes := *p.nodes.Load()
	aliveNodes := make([]*Node, 0, len(nodes))
	for _, node := range nodes {
		if node.IsAlive() {
			aliveNodes = append(aliveNodes, node)
		}
	}
	return aliveNodes
}

func (p *Pool) GetTotalNodesCount() int {
	return len(*p.nodes.Load())
}

func (p *Pool) Add(newNodes []*Node) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err := p.checkDuplicateNodeID(newNodes)
	if err != nil {
		return err
	}
	nodes := *p.nodes.Load()
	newPool := make([]*Node, 0, len(nodes)+len(newNodes))
	newPool = append(newPool, nodes...)
	newPool = append(newPool, newNodes...)
	p.nodes.Store(&newPool)
	return nil
}

func (p *Pool) GetAllNodes() []*Node {
	return *p.nodes.Load()
}

func (p *Pool) checkDuplicateNodeID(nodeToAdd []*Node) (*[]string, error) {
	ids := make(map[string]struct{})
	duplicates := make([]string, 0)
	for _, node := range p.GetAllNodes() {
		_, ok := ids[node.ID]
		if ok {
			duplicates = append(duplicates, node.ID)
		}
		ids[node.ID] = struct{}{}
	}
	for _, addingNode := range nodeToAdd {
		_, ok := ids[addingNode.ID]
		if ok {
			duplicates = append(duplicates, addingNode.ID)
		}
		ids[addingNode.ID] = struct{}{}
	}
	if len(duplicates) == 0 {
		return nil, nil
	}
	return &duplicates, fmt.Errorf("pool contains duplicated ids: %w", ErrPoolContainsDuplicatedValues)
}
