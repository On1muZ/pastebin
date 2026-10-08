package nodepool

import (
	"sync"
	"sync/atomic"
)

type Status struct {
	FreeSpace  int64
	TotalSpace int64
}

type Node struct {
	mu         sync.Mutex
	aliveConns atomic.Int32
	alive      atomic.Bool
	status     atomic.Pointer[Status]
}

func NewNode() *Node {
	n := &Node{}
	n.status.Store(&Status{})
	return n
}

func (n *Node) IncAliveConn() {
	n.aliveConns.Add(1)
}

func (n *Node) DecAliveConn() {
	n.aliveConns.Add(-1)
}

func (n *Node) GetAliveConns() int32 {
	return n.aliveConns.Load()
}

func (n *Node) MarkAlive() {
	n.alive.Store(true)
}

func (n *Node) MarkDead() {
	n.alive.Store(false)
}

func (n *Node) AddFreeSpace(value int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	curS := *n.status.Load()
	curS.FreeSpace += value
	n.status.Store(&curS)
}

func (n *Node) DeleteFreeSpace(value int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	curS := *n.status.Load()
	curS.FreeSpace -= value
	n.status.Store(&curS)
}

func (n *Node) GetStatus() Status {
	return *n.status.Load()
}

func (n *Node) ChangeStatus(s Status) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.status.Store(&s)
}

func (n *Node) IsAlive() bool {
	return n.alive.Load()
}
