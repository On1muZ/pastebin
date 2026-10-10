package nodepool

import (
	"net/url"
	"sync"
	"testing"
)

func TestNewNodeKeepsIDAndAddr(t *testing.T) {
	addr := url.URL{Scheme: "grpc", Host: "storage-1:50051"}
	node := NewNode("storage-1", addr)
	if node.ID != "storage-1" {
		t.Fatalf("incorrect node id: got %q, want %q", node.ID, "storage-1")
	}
	if got := node.Addr.String(); got != addr.String() {
		t.Fatalf("incorrect node addr: got %q, want %q", got, addr.String())
	}
}

func TestNewNode(t *testing.T) {
	node := newUniqueNode()
	if node.IsAlive() {
		t.Fatalf("new node must be dead by default")
	}
	if s := node.GetStatus(); s.FreeSpace != 0 || s.TotalSpace != 0 {
		t.Fatalf("new node must have zero status: got %+v", s)
	}
	if node.GetAliveConns() != 0 {
		t.Fatalf("new node must have zero alive conns: got %d", node.GetAliveConns())
	}
}

func TestMarkDeadAndMarkAlive(t *testing.T) {
	node := newUniqueNode()
	node.MarkAlive()
	if !node.IsAlive() {
		t.Fatalf("mark alive do nothing")
	}
	node.MarkDead()
	if node.IsAlive() {
		t.Fatalf("mark dead do nothing")
	}
}

func TestIncAndDecAliveConns(t *testing.T) {
	node := newUniqueNode()
	for i := 0; i < 32; i++ {
		node.IncAliveConn()
	}
	if node.GetAliveConns() != 32 {
		t.Fatalf("incorrect alive conns val after inc: got %d, want 32", node.GetAliveConns())
	}
	for i := 0; i < 32; i++ {
		node.DecAliveConn()
	}
	if node.GetAliveConns() != 0 {
		t.Fatalf("incorrect alive conns val after dec: got %d, want 0", node.GetAliveConns())
	}
}

func TestStatusChanging(t *testing.T) {
	node := newUniqueNode()
	s := Status{
		FreeSpace:  500,
		TotalSpace: 1000,
	}
	node.ChangeStatus(s)
	if ns := node.GetStatus(); ns != s {
		t.Fatalf("change or get status works wrong: got %+v, want %+v", ns, s)
	}
	s.FreeSpace -= 200
	if node.GetStatus() == s {
		t.Fatalf("changing of local status changes status of node")
	}
	node.AddFreeSpace(200)
	if node.GetStatus().FreeSpace != 700 {
		t.Fatalf("adding free space doesn`t works: got %d, want 700", node.GetStatus().FreeSpace)
	}
	node.DeleteFreeSpace(300)
	if node.GetStatus().FreeSpace != 400 {
		t.Fatalf("deleting free space doesn`t works: got %d, want 400", node.GetStatus().FreeSpace)
	}
}

func TestConcurentAddingFreeSpace(t *testing.T) {
	n := 20
	m := 10
	node := newUniqueNode()
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range m {
				node.AddFreeSpace(1)
			}
		}()
	}
	wg.Wait()
	if node.GetStatus().FreeSpace != 200 {
		t.Fatalf("incorrect value of concurent adding free space: got %d, want 200", node.GetStatus().FreeSpace)
	}
}

func TestConcurentDeletingFreeSpace(t *testing.T) {
	n := 20
	m := 10
	node := newUniqueNode()
	node.AddFreeSpace(300)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range m {
				node.DeleteFreeSpace(1)
			}
		}()
	}
	wg.Wait()
	if node.GetStatus().FreeSpace != 100 {
		t.Fatalf("incorrect value of concurent deleting free space: got %d, want 100", node.GetStatus().FreeSpace)
	}
}

func TestSpaceChangingKeepsTotalSpace(t *testing.T) {
	node := newUniqueNode()
	node.ChangeStatus(Status{FreeSpace: 500, TotalSpace: 1000})
	node.AddFreeSpace(100)
	node.DeleteFreeSpace(50)
	if got := node.GetStatus().TotalSpace; got != 1000 {
		t.Fatalf("changing free space changes total space: got %d, want 1000", got)
	}
}

func TestConcurrentIncAndDecAliveConns(t *testing.T) {
	n := 20
	m := 10
	node := newUniqueNode()
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range m {
				node.IncAliveConn()
				node.DecAliveConn()
			}
		}()
	}
	wg.Wait()
	if got := node.GetAliveConns(); got != 0 {
		t.Fatalf("incorrect alive conns after concurrent inc and dec: got %d, want 0", got)
	}
}

func TestConcurrentSpaceChangingAndReading(t *testing.T) {
	n := 20
	m := 10
	node := newUniqueNode()
	node.ChangeStatus(Status{FreeSpace: 1000, TotalSpace: 2000})
	var wg sync.WaitGroup
	for range n {
		wg.Add(3)
		go func() {
			defer wg.Done()
			for range m {
				node.AddFreeSpace(1)
			}
		}()
		go func() {
			defer wg.Done()
			for range m {
				node.DeleteFreeSpace(1)
			}
		}()
		go func() {
			defer wg.Done()
			for range m {
				_ = node.GetStatus()
			}
		}()
	}
	wg.Wait()
	if got := node.GetStatus().FreeSpace; got != 1000 {
		t.Fatalf("incorrect free space after concurrent add, delete and read: got %d, want 1000", got)
	}
}
