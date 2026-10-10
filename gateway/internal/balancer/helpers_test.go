package balancer

import (
	"fmt"
	"pastebin/gateway/internal/nodepool"
	"sync/atomic"
	"testing"
)

const gib int64 = 1 << 30

var nodeSeq atomic.Int64

// newTestNode builds a node with the given free/total space (bytes) and number
// of in-flight requests. Every node gets a unique id and address, because the
// pool rejects duplicates. Status is stored before MarkAlive, same order as the
// checker will use.
func newTestNode(free, total int64, conns int, alive bool) *nodepool.Node {
	id := fmt.Sprintf("node-%d", nodeSeq.Add(1))
	n := nodepool.NewNode(id, id+":50051")
	n.ChangeStatus(nodepool.Status{FreeSpace: free, TotalSpace: total})
	for range conns {
		n.IncAliveConn()
	}
	if alive {
		n.MarkAlive()
	}
	return n
}

func mustPool(t *testing.T, nodes []*nodepool.Node) *nodepool.Pool {
	t.Helper()
	p, err := nodepool.NewPool(nodes)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	return p
}

func mustNext(t *testing.T, algo Algorithm, size int64, except []*nodepool.Node) *nodepool.Node {
	t.Helper()
	node, err := algo.Next(size, except)
	if err != nil {
		t.Fatalf("Next(%d) returned error: %v", size, err)
	}
	if node == nil {
		t.Fatalf("Next(%d) returned nil node without error", size)
	}
	return node
}
