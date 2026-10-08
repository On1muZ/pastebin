package nodepool

import (
	"sync"
	"testing"
)

func TestGettingTotalNodesCount(t *testing.T) {
	nodes := make([]*Node, 0, 20)
	for i := 0; i < 20; i++ {
		nodes = append(nodes, NewNode())
	}
	p := NewPool(nodes)
	if p.GetTotalNodesCount() != 20 {
		t.Fatalf("get total nodes count returns incorrect val: got %d, want: 20", p.GetTotalNodesCount())
	}
}

func TestGettingAliveNodes(t *testing.T) {
	nodes := make([]*Node, 0, 22)
	for i := 0; i < 20; i++ {
		nodes = append(nodes, NewNode())
	}
	p := NewPool(nodes)
	pn := p.GetAllNodes()
	pn[0].MarkAlive()
	pn[1].MarkAlive()
	if len(p.GetAllAliveNodes()) != 2 {
		t.Fatalf("incorrect value of alive nodes: got %d, want 2", len(p.GetAllAliveNodes()))
	}
	for _, node := range p.GetAllAliveNodes() {
		if node != pn[0] && node != pn[1] {
			t.Fatal("get all alive nodes works incorrect")
		}
	}
}

func TestConcurrentAddingNodeToPool(t *testing.T) {
	var wg sync.WaitGroup
	p := NewPool([]*Node{NewNode()})
	n := 19
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node := NewNode()

			p.Add([]*Node{node})
		}()
	}
	wg.Wait()
	if len(p.GetAllNodes()) != 20 {
		t.Fatalf("incorrect value of len nodes: got %d, want 20", len(p.GetAllNodes()))
	}
}

func TestGettingAliveNodesFromEmptyPool(t *testing.T) {
	p := NewPool(nil)
	if got := len(p.GetAllAliveNodes()); got != 0 {
		t.Fatalf("empty pool returns alive nodes: got %d, want 0", got)
	}
	if got := p.GetTotalNodesCount(); got != 0 {
		t.Fatalf("empty pool has incorrect nodes count: got %d, want 0", got)
	}
}

func TestGettingAliveNodesWithoutAlive(t *testing.T) {
	p := NewPool([]*Node{NewNode(), NewNode(), NewNode()})
	if got := len(p.GetAllAliveNodes()); got != 0 {
		t.Fatalf("pool without alive nodes returns alive nodes: got %d, want 0", got)
	}
}

func TestGettingAliveNodesAfterMarkDead(t *testing.T) {
	a, b := NewNode(), NewNode()
	a.MarkAlive()
	b.MarkAlive()
	p := NewPool([]*Node{a, b})
	b.MarkDead()
	alive := p.GetAllAliveNodes()
	if len(alive) != 1 || alive[0] != a {
		t.Fatalf("dead node must be filtered out: got %d alive nodes, want only first", len(alive))
	}
}

func TestNewPoolIsIsolatedFromSourceSlice(t *testing.T) {
	first, second := NewNode(), NewNode()
	nodes := []*Node{first}
	p := NewPool(nodes)
	nodes[0] = second
	if p.GetAllNodes()[0] != first {
		t.Fatalf("changing of source slice changes nodes of pool")
	}
}

func TestAddingNodesKeepsOldOnes(t *testing.T) {
	old, added := NewNode(), NewNode()
	p := NewPool([]*Node{old})
	p.Add([]*Node{added})
	nodes := p.GetAllNodes()
	if len(nodes) != 2 {
		t.Fatalf("incorrect nodes count after add: got %d, want 2", len(nodes))
	}
	if nodes[0] != old || nodes[1] != added {
		t.Fatalf("add must keep old nodes and append new ones in order")
	}
}

func TestConcurrentAddingKeepsAllNodes(t *testing.T) {
	n := 19
	added := make([]*Node, 0, n)
	for range n {
		added = append(added, NewNode())
	}
	p := NewPool([]*Node{NewNode()})
	var wg sync.WaitGroup
	for _, node := range added {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Add([]*Node{node})
		}()
	}
	wg.Wait()
	inPool := make(map[*Node]bool)
	for _, node := range p.GetAllNodes() {
		inPool[node] = true
	}
	for i, node := range added {
		if !inPool[node] {
			t.Fatalf("node %d was lost during concurrent add", i)
		}
	}
}

func TestConcurrentAddingAndReadingAliveNodes(t *testing.T) {
	n := 20
	p := NewPool([]*Node{NewNode()})
	var wg sync.WaitGroup
	for range n {
		wg.Add(2)
		go func() {
			defer wg.Done()
			node := NewNode()
			node.MarkAlive()
			p.Add([]*Node{node})
		}()
		go func() {
			defer wg.Done()
			_ = p.GetAllAliveNodes()
			_ = p.GetTotalNodesCount()
		}()
	}
	wg.Wait()
	if got := len(p.GetAllAliveNodes()); got != n {
		t.Fatalf("incorrect alive nodes count after concurrent add and read: got %d, want %d", got, n)
	}
}
