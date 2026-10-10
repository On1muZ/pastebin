package nodepool

import (
	"fmt"
	"sync/atomic"
	"testing"
)

var nodeSeq atomic.Int64

// newTestNode builds a node whose address is derived from its id, so distinct
// ids always give distinct addresses.
func newTestNode(id string) *Node {
	return NewNode(id, id+":50051")
}

// newUniqueNode builds a node with an id that was never handed out before, for
// tests that do not care about the concrete id but must not collide in a pool.
func newUniqueNode() *Node {
	return newTestNode(fmt.Sprintf("node-%d", nodeSeq.Add(1)))
}

func mustNewPool(t *testing.T, nodes []*Node) *Pool {
	t.Helper()
	p, err := NewPool(nodes)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	return p
}
