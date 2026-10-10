package balancer

import (
	"pastebin/gateway/internal/nodepool"
	"testing"
)

const gib int64 = 1 << 30

// newTestNode builds a node with the given free/total space (bytes) and number
// of in-flight requests. Status is stored before MarkAlive, same order as the
// checker will use.
func newTestNode(free, total int64, conns int, alive bool) *nodepool.Node {
	n := nodepool.NewNode()
	n.ChangeStatus(nodepool.Status{FreeSpace: free, TotalSpace: total})
	for range conns {
		n.IncAliveConn()
	}
	if alive {
		n.MarkAlive()
	}
	return n
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
