package nodepool

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestGettingTotalNodesCount(t *testing.T) {
	nodes := make([]*Node, 0, 20)
	for range 20 {
		nodes = append(nodes, newUniqueNode())
	}
	p := mustNewPool(t, nodes)
	if p.GetTotalNodesCount() != 20 {
		t.Fatalf("get total nodes count returns incorrect val: got %d, want: 20", p.GetTotalNodesCount())
	}
}

func TestGettingAliveNodes(t *testing.T) {
	nodes := make([]*Node, 0, 22)
	for range 20 {
		nodes = append(nodes, newUniqueNode())
	}
	p := mustNewPool(t, nodes)
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
	p := mustNewPool(t, []*Node{newUniqueNode()})
	n := 19
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Add([]*Node{newUniqueNode()}); err != nil {
				t.Errorf("add of unique node returned error: %v", err)
			}
		}()
	}
	wg.Wait()
	if len(p.GetAllNodes()) != 20 {
		t.Fatalf("incorrect value of len nodes: got %d, want 20", len(p.GetAllNodes()))
	}
}

func TestGettingAliveNodesFromEmptyPool(t *testing.T) {
	p := mustNewPool(t, nil)
	if got := len(p.GetAllAliveNodes()); got != 0 {
		t.Fatalf("empty pool returns alive nodes: got %d, want 0", got)
	}
	if got := p.GetTotalNodesCount(); got != 0 {
		t.Fatalf("empty pool has incorrect nodes count: got %d, want 0", got)
	}
}

func TestGettingAliveNodesWithoutAlive(t *testing.T) {
	p := mustNewPool(t, []*Node{newUniqueNode(), newUniqueNode(), newUniqueNode()})
	if got := len(p.GetAllAliveNodes()); got != 0 {
		t.Fatalf("pool without alive nodes returns alive nodes: got %d, want 0", got)
	}
}

func TestGettingAliveNodesAfterMarkDead(t *testing.T) {
	a, b := newUniqueNode(), newUniqueNode()
	a.MarkAlive()
	b.MarkAlive()
	p := mustNewPool(t, []*Node{a, b})
	b.MarkDead()
	alive := p.GetAllAliveNodes()
	if len(alive) != 1 || alive[0] != a {
		t.Fatalf("dead node must be filtered out: got %d alive nodes, want only first", len(alive))
	}
}

func TestNewPoolIsIsolatedFromSourceSlice(t *testing.T) {
	first, second := newUniqueNode(), newUniqueNode()
	nodes := []*Node{first}
	p := mustNewPool(t, nodes)
	nodes[0] = second
	if p.GetAllNodes()[0] != first {
		t.Fatalf("changing of source slice changes nodes of pool")
	}
}

func TestAddingNodesKeepsOldOnes(t *testing.T) {
	old, added := newUniqueNode(), newUniqueNode()
	p := mustNewPool(t, []*Node{old})
	if err := p.Add([]*Node{added}); err != nil {
		t.Fatalf("add of unique node returned error: %v", err)
	}
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
		added = append(added, newUniqueNode())
	}
	p := mustNewPool(t, []*Node{newUniqueNode()})
	var wg sync.WaitGroup
	for _, node := range added {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Add([]*Node{node}); err != nil {
				t.Errorf("add of unique node returned error: %v", err)
			}
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
	p := mustNewPool(t, []*Node{newUniqueNode()})
	var wg sync.WaitGroup
	for range n {
		wg.Add(2)
		go func() {
			defer wg.Done()
			node := newUniqueNode()
			node.MarkAlive()
			if err := p.Add([]*Node{node}); err != nil {
				t.Errorf("add of unique node returned error: %v", err)
			}
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

func TestNewPoolAcceptsEmptyInput(t *testing.T) {
	for name, nodes := range map[string][]*Node{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			p, err := NewPool(nodes)
			if err != nil {
				t.Fatalf("NewPool returned error: %v", err)
			}
			if p == nil {
				t.Fatalf("NewPool returned nil pool without error")
			}
		})
	}
}

func TestNewPoolRejectsDuplicateIDs(t *testing.T) {
	nodes := []*Node{newTestNode("same"), newUniqueNode(), newTestNode("same")}
	p, err := NewPool(nodes)
	if !errors.Is(err, ErrPoolContainsDuplicatedValues) {
		t.Fatalf("incorrect error: got %v, want %v", err, ErrPoolContainsDuplicatedValues)
	}
	if p != nil {
		t.Fatalf("pool must be nil when ids are duplicated")
	}
}

func TestAddRejectsIDAlreadyInPool(t *testing.T) {
	old := newTestNode("a")
	p := mustNewPool(t, []*Node{old})

	err := p.Add([]*Node{newTestNode("a")})

	if !errors.Is(err, ErrPoolContainsDuplicatedValues) {
		t.Fatalf("incorrect error: got %v, want %v", err, ErrPoolContainsDuplicatedValues)
	}
	if nodes := p.GetAllNodes(); len(nodes) != 1 || nodes[0] != old {
		t.Fatalf("pool must stay unchanged after rejected add")
	}
}

func TestAddRejectsDuplicatesInsideNewNodes(t *testing.T) {
	p := mustNewPool(t, []*Node{newTestNode("a")})

	err := p.Add([]*Node{newTestNode("b"), newTestNode("b")})

	if !errors.Is(err, ErrPoolContainsDuplicatedValues) {
		t.Fatalf("incorrect error: got %v, want %v", err, ErrPoolContainsDuplicatedValues)
	}
	if got := p.GetTotalNodesCount(); got != 1 {
		t.Fatalf("pool must stay unchanged after rejected add: got %d nodes, want 1", got)
	}
}

func TestAddIsAllOrNothing(t *testing.T) {
	p := mustNewPool(t, []*Node{newTestNode("a")})
	fresh := newTestNode("fresh")

	err := p.Add([]*Node{fresh, newTestNode("a")})

	if !errors.Is(err, ErrPoolContainsDuplicatedValues) {
		t.Fatalf("incorrect error: got %v, want %v", err, ErrPoolContainsDuplicatedValues)
	}
	if got := p.GetTotalNodesCount(); got != 1 {
		t.Fatalf("valid node of a rejected batch must not be added: got %d nodes, want 1", got)
	}
	// the rejected batch must not leave the pool in a broken state
	if err := p.Add([]*Node{fresh}); err != nil {
		t.Fatalf("add of the same valid node after rejection returned error: %v", err)
	}
	if got := p.GetTotalNodesCount(); got != 2 {
		t.Fatalf("incorrect nodes count after retry: got %d, want 2", got)
	}
}

func TestConcurrentAddingSameIDKeepsOnlyOne(t *testing.T) {
	n := 20
	p := mustNewPool(t, []*Node{newTestNode("old")})
	var added, rejected atomic.Int32
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := p.Add([]*Node{newTestNode("same")})
			switch {
			case err == nil:
				added.Add(1)
			case errors.Is(err, ErrPoolContainsDuplicatedValues):
				rejected.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if got := added.Load(); got != 1 {
		t.Fatalf("exactly one add must win: got %d", got)
	}
	if got := rejected.Load(); got != int32(n-1) {
		t.Fatalf("all other adds must be rejected: got %d, want %d", got, n-1)
	}
	if got := p.GetTotalNodesCount(); got != 2 {
		t.Fatalf("incorrect nodes count: got %d, want 2", got)
	}
}

func TestGettingNodeByID(t *testing.T) {
	nodes := []*Node{newTestNode("a"), newTestNode("b"), newTestNode("c")}
	p := mustNewPool(t, nodes)
	got, ok := p.GetNodeByID("b")
	if !ok {
		t.Fatal("node b not found")
	}
	if got != nodes[1] {
		t.Fatalf("returned another pointer: got %p, want %p", got, nodes[1])
	}
}

func TestGettingNodeByUnknownID(t *testing.T) {
	p := mustNewPool(t, []*Node{newTestNode("a")})
	got, ok := p.GetNodeByID("unknown")
	if ok || got != nil {
		t.Fatalf("incorrect result for unknown id: got (%v, %t), want (nil, false)", got, ok)
	}
}

func TestGettingNodeByIDFromEmptyPool(t *testing.T) {
	p := mustNewPool(t, []*Node{})
	got, ok := p.GetNodeByID("a")
	if ok || got != nil {
		t.Fatalf("incorrect result for empty pool: got (%v, %t), want (nil, false)", got, ok)
	}
}

func TestGettingNodeByIDAfterAdd(t *testing.T) {
	p := mustNewPool(t, []*Node{newTestNode("a")})
	added := newTestNode("b")
	if err := p.Add([]*Node{added}); err != nil {
		t.Fatalf("Add returned error: %v", err)
	}
	got, ok := p.GetNodeByID("b")
	if !ok || got != added {
		t.Fatalf("added node not found: got (%v, %t), want (%p, true)", got, ok, added)
	}
	if _, ok := p.GetNodeByID("a"); !ok {
		t.Fatal("old node lost after Add")
	}
}

func TestConcurrentGettingNodeByIDAndAdding(t *testing.T) {
	first := newTestNode("first")
	p := mustNewPool(t, []*Node{first})
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if err := p.Add([]*Node{newUniqueNode()}); err != nil {
				t.Errorf("Add returned error: %v", err)
			}
		})
		wg.Go(func() {
			got, ok := p.GetNodeByID("first")
			if !ok || got != first {
				t.Errorf("node first not found during concurrent Add: got (%v, %t)", got, ok)
			}
		})
	}
	wg.Wait()
}
