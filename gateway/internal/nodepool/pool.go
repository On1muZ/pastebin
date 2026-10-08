package nodepool

import (
	"slices"
	"sync"
	"sync/atomic"
)

type Pool struct {
	nodes atomic.Pointer[[]*Node]
	mu    sync.Mutex
}

func NewPool(nodes []*Node) *Pool {
	p := &Pool{}
	n := slices.Clone(nodes)
	p.nodes.Store(&n)
	return p
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

func (p *Pool) Add(newNodes []*Node) {
	p.mu.Lock()
	defer p.mu.Unlock()
	nodes := *p.nodes.Load()
	newPool := make([]*Node, 0, len(nodes)+len(newNodes))
	newPool = append(newPool, nodes...)
	newPool = append(newPool, newNodes...)
	p.nodes.Store(&newPool)
}

func (p *Pool) GetAllNodes() []*Node {
	return *p.nodes.Load()
}
