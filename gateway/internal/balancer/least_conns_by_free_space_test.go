package balancer

import (
	"pastebin/gateway/internal/nodepool"
	"testing"
)

func TestLeastConnsPrefersLargerFreeShare(t *testing.T) {
	less := newTestNode(50*gib, 100*gib, 0, true)
	more := newTestNode(80*gib, 100*gib, 0, true)
	algo := NewLeastConnsByFreeSpace(nodepool.NewPool([]*nodepool.Node{less, more}))

	if got := mustNext(t, algo, gib, nil); got != more {
		t.Fatalf("must pick node with larger free share (80%% vs 50%%)")
	}
}

func TestLeastConnsComparesShareNotAbsoluteSpace(t *testing.T) {
	// big has more free bytes, but only 9% of its disk is free.
	big := newTestNode(90*gib, 1000*gib, 0, true)
	small := newTestNode(40*gib, 50*gib, 0, true)
	algo := NewLeastConnsByFreeSpace(nodepool.NewPool([]*nodepool.Node{big, small}))

	if got := mustNext(t, algo, gib, nil); got != small {
		t.Fatalf("weight is free/total share, node with 80%% free must win over 9%% free")
	}
}

func TestLeastConnsPrefersFewerConns(t *testing.T) {
	busy := newTestNode(50*gib, 100*gib, 5, true)
	idle := newTestNode(50*gib, 100*gib, 0, true)
	algo := NewLeastConnsByFreeSpace(nodepool.NewPool([]*nodepool.Node{busy, idle}))

	if got := mustNext(t, algo, gib, nil); got != idle {
		t.Fatalf("must pick node with fewer active conns at equal free share")
	}
}

func TestLeastConnsWeighsConnsAgainstShare(t *testing.T) {
	// weight = (free/total) / (conns+1); total is 100 GiB for both nodes.
	tests := []struct {
		name      string
		busyFree  int64 // GiB
		busyConns int
		idleFree  int64 // GiB
		wantBusy  bool
	}{
		{name: "100% free with 3 conns (0.25) loses to idle 50% (0.5)", busyFree: 100, busyConns: 3, idleFree: 50, wantBusy: false},
		{name: "100% free with 1 conn (0.5) beats idle 40% (0.4)", busyFree: 100, busyConns: 1, idleFree: 40, wantBusy: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			busy := newTestNode(tc.busyFree*gib, 100*gib, tc.busyConns, true)
			idle := newTestNode(tc.idleFree*gib, 100*gib, 0, true)
			algo := NewLeastConnsByFreeSpace(nodepool.NewPool([]*nodepool.Node{busy, idle}))

			want := idle
			if tc.wantBusy {
				want = busy
			}
			if got := mustNext(t, algo, gib, nil); got != want {
				t.Fatalf("picked wrong node: wantBusy=%v", tc.wantBusy)
			}
		})
	}
}

func TestLeastConnsSkipsExcludedBest(t *testing.T) {
	best := newTestNode(90*gib, 100*gib, 0, true)
	second := newTestNode(60*gib, 100*gib, 0, true)
	third := newTestNode(30*gib, 100*gib, 0, true)
	algo := NewLeastConnsByFreeSpace(nodepool.NewPool([]*nodepool.Node{best, second, third}))

	if got := mustNext(t, algo, gib, []*nodepool.Node{best}); got != second {
		t.Fatalf("must pick second best when the best is excluded")
	}
	if got := mustNext(t, algo, gib, []*nodepool.Node{best, second}); got != third {
		t.Fatalf("must pick third when two best are excluded")
	}
}

func TestLeastConnsSkipsDeadBest(t *testing.T) {
	dead := newTestNode(100*gib, 100*gib, 0, false)
	alive := newTestNode(10*gib, 100*gib, 0, true)
	algo := NewLeastConnsByFreeSpace(nodepool.NewPool([]*nodepool.Node{dead, alive}))

	if got := mustNext(t, algo, gib, nil); got != alive {
		t.Fatalf("dead node must never be picked")
	}
}

func TestLeastConnsSkipsBestWithoutEnoughSpace(t *testing.T) {
	// tiny has the best share (90%) but only 9 GiB free.
	tiny := newTestNode(9*gib, 10*gib, 0, true)
	huge := newTestNode(500*gib, 1000*gib, 0, true)
	algo := NewLeastConnsByFreeSpace(nodepool.NewPool([]*nodepool.Node{tiny, huge}))

	if got := mustNext(t, algo, 20*gib, nil); got != huge {
		t.Fatalf("node that cannot hold the file must be skipped even with best share")
	}
}

func TestLeastConnsReactsToLoadChanges(t *testing.T) {
	a := newTestNode(50*gib, 100*gib, 0, true)
	b := newTestNode(50*gib, 100*gib, 0, true)
	algo := NewLeastConnsByFreeSpace(nodepool.NewPool([]*nodepool.Node{a, b}))

	if got := mustNext(t, algo, gib, nil); got != a {
		t.Fatalf("at equal weight the first node of the pool is expected")
	}
	for range 5 {
		a.IncAliveConn()
	}
	if got := mustNext(t, algo, gib, nil); got != b {
		t.Fatalf("after node a got busy, node b must be picked (no stale weights)")
	}
	for range 5 {
		a.DecAliveConn()
	}
	if got := mustNext(t, algo, gib, nil); got != a {
		t.Fatalf("after node a became idle again it must be picked again")
	}
}

func TestLeastConnsReactsToStatusChanges(t *testing.T) {
	a := newTestNode(80*gib, 100*gib, 0, true)
	b := newTestNode(50*gib, 100*gib, 0, true)
	algo := NewLeastConnsByFreeSpace(nodepool.NewPool([]*nodepool.Node{a, b}))

	if got := mustNext(t, algo, gib, nil); got != a {
		t.Fatalf("node a with 80%% free must win initially")
	}
	a.ChangeStatus(nodepool.Status{FreeSpace: 20 * gib, TotalSpace: 100 * gib})
	if got := mustNext(t, algo, gib, nil); got != b {
		t.Fatalf("after node a dropped to 20%% free node b must win")
	}
}
