package checker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"pastebin/gateway/internal/nodepool"
	"pastebin/gateway/internal/transport"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeNode is a storage node whose GetStatus behaviour a test controls.
type fakeNode struct {
	storagenodev1.UnimplementedStorageNodeServer
	total   atomic.Int64
	free    atomic.Int64
	failing atomic.Bool   // answer with codes.Unavailable
	hang    atomic.Bool   // block until the caller gives up
	delay   time.Duration // answer after this delay (respects caller's context)
	calls   atomic.Int32
}

func newFakeNode(total, free int64) *fakeNode {
	f := &fakeNode{}
	f.total.Store(total)
	f.free.Store(free)
	return f
}

func (f *fakeNode) GetStatus(ctx context.Context, _ *storagenodev1.GetStatusRequest) (*storagenodev1.GetStatusResponse, error) {
	f.calls.Add(1)
	if f.hang.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.failing.Load() {
		return nil, status.Error(codes.Unavailable, "node is down")
	}
	return &storagenodev1.GetStatusResponse{TotalBytes: f.total.Load(), FreeBytes: f.free.Load()}, nil
}

// cluster holds the fake nodes and the pool nodes that point at them.
type cluster struct {
	fakes     map[string]*fakeNode
	listeners map[string]*bufconn.Listener
	nodes     map[string]*nodepool.Node
	pool      *nodepool.Pool
	checker   *Checker
}

// newCluster starts the given fake nodes. Names listed in unreachable are
// added to the pool but nobody listens on their address.
func newCluster(t *testing.T, fakes map[string]*fakeNode, unreachable ...string) *cluster {
	t.Helper()
	return newClusterWithLogger(t, slog.New(slog.DiscardHandler), fakes, unreachable...)
}

func newClusterWithLogger(t *testing.T, log *slog.Logger, fakes map[string]*fakeNode, unreachable ...string) *cluster {
	t.Helper()
	c := &cluster{
		fakes:     fakes,
		listeners: make(map[string]*bufconn.Listener),
		nodes:     make(map[string]*nodepool.Node),
	}
	var all []*nodepool.Node
	add := func(name string) {
		node := nodepool.NewNode(name, "passthrough:///"+name)
		c.nodes[name] = node
		all = append(all, node)
	}
	for name, f := range fakes {
		lis := bufconn.Listen(1 << 20)
		srv := grpc.NewServer()
		storagenodev1.RegisterStorageNodeServer(srv, f)
		go srv.Serve(lis)
		t.Cleanup(srv.Stop)
		c.listeners[name] = lis
		add(name)
	}
	for _, name := range unreachable {
		add(name)
	}

	pool, err := nodepool.NewPool(all)
	if err != nil {
		t.Fatalf("NewPool returned error: %v", err)
	}
	c.pool = pool

	tr := transport.New(
		transport.WithLogger(slog.New(slog.DiscardHandler)),
		transport.WithDialOpts(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				lis, ok := c.listeners[addr]
				if !ok {
					return nil, errors.New("nobody listens on " + addr)
				}
				return lis.DialContext(ctx)
			}),
		),
	)
	t.Cleanup(func() { tr.Close() })

	c.checker = New(pool, tr, log)
	return c
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func TestCheckOnceMarksReachableNodeAliveWithStatus(t *testing.T) {
	c := newCluster(t, map[string]*fakeNode{"a": newFakeNode(1000, 400)})
	node := c.nodes["a"]

	c.checker.CheckOnce(context.Background())

	if !node.IsAlive() {
		t.Fatalf("reachable node must be alive")
	}
	if got := node.GetStatus(); got.TotalSpace != 1000 || got.FreeSpace != 400 {
		t.Fatalf("incorrect status: got %+v, want total 1000 free 400", got)
	}
}

// Regression: a node that was never alive and does not answer used to crash
// the whole process with a nil pointer dereference.
func TestCheckOnceNodeDownAtStartup(t *testing.T) {
	c := newCluster(t, map[string]*fakeNode{}, "ghost")

	c.checker.CheckOnce(context.Background())

	if c.nodes["ghost"].IsAlive() {
		t.Fatalf("unreachable node must stay dead")
	}
	if got := c.nodes["ghost"].GetStatus(); got != (nodepool.Status{}) {
		t.Fatalf("unreachable node must keep zero status: got %+v", got)
	}
}

func TestCheckOnceMixedNodesAreIndependent(t *testing.T) {
	c := newCluster(t, map[string]*fakeNode{
		"up1": newFakeNode(1000, 100),
		"up2": newFakeNode(2000, 200),
	}, "down")

	c.checker.CheckOnce(context.Background())

	if !c.nodes["up1"].IsAlive() || !c.nodes["up2"].IsAlive() {
		t.Fatalf("reachable nodes must be alive")
	}
	if c.nodes["down"].IsAlive() {
		t.Fatalf("unreachable node must be dead")
	}
	if got := c.nodes["up2"].GetStatus().TotalSpace; got != 2000 {
		t.Fatalf("node up2 got status of another node: total %d, want 2000", got)
	}
	if got := len(c.pool.GetAllAliveNodes()); got != 2 {
		t.Fatalf("incorrect alive nodes in pool: got %d, want 2", got)
	}
}

func TestCheckOnceMarksDeadNodeThatStartsFailing(t *testing.T) {
	f := newFakeNode(1000, 400)
	c := newCluster(t, map[string]*fakeNode{"a": f})
	node := c.nodes["a"]
	ctx := context.Background()

	c.checker.CheckOnce(ctx)
	if !node.IsAlive() {
		t.Fatalf("precondition: node must be alive")
	}

	f.failing.Store(true)
	c.checker.CheckOnce(ctx)

	if node.IsAlive() {
		t.Fatalf("node that answers with an error must be marked dead")
	}
}

func TestCheckOnceRevivesNodeAndRefreshesStatus(t *testing.T) {
	f := newFakeNode(1000, 400)
	c := newCluster(t, map[string]*fakeNode{"a": f})
	node := c.nodes["a"]
	ctx := context.Background()

	c.checker.CheckOnce(ctx)
	f.failing.Store(true)
	c.checker.CheckOnce(ctx)
	if node.IsAlive() {
		t.Fatalf("precondition: node must be dead")
	}

	f.failing.Store(false)
	f.free.Store(100)
	c.checker.CheckOnce(ctx)

	if !node.IsAlive() {
		t.Fatalf("node must be alive again after it answers")
	}
	if got := node.GetStatus().FreeSpace; got != 100 {
		t.Fatalf("status must be refreshed after recovery: free %d, want 100", got)
	}
}

func TestCheckOnceRefreshesStatusOfAliveNode(t *testing.T) {
	f := newFakeNode(1000, 400)
	c := newCluster(t, map[string]*fakeNode{"a": f})
	node := c.nodes["a"]
	ctx := context.Background()

	c.checker.CheckOnce(ctx)
	f.free.Store(250)
	c.checker.CheckOnce(ctx)

	if !node.IsAlive() {
		t.Fatalf("node must stay alive")
	}
	if got := node.GetStatus().FreeSpace; got != 250 {
		t.Fatalf("free space must be refreshed: got %d, want 250", got)
	}
}

// The status must be stored before the node is marked alive, otherwise the
// balancer can see an alive node with total space 0. A reader polls the nodes
// while the checker runs and fails if it ever observes that combination.
func TestCheckOnceNeverExposesAliveNodeWithZeroStatus(t *testing.T) {
	fakes := make(map[string]*fakeNode)
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"} {
		fakes[name] = newFakeNode(1000, 500)
	}
	c := newCluster(t, fakes)

	stop := make(chan struct{})
	var violations atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, node := range c.nodes {
				if node.IsAlive() && node.GetStatus().TotalSpace == 0 {
					violations.Add(1)
				}
			}
		}
	}()

	c.checker.CheckOnce(context.Background())
	close(stop)
	wg.Wait()

	if got := violations.Load(); got != 0 {
		t.Fatalf("observed an alive node with zero status %d times", got)
	}
}

// Nodes are probed in parallel: four nodes that answer after one second must
// not make the round take four seconds.
func TestCheckOnceProbesNodesInParallel(t *testing.T) {
	fakes := make(map[string]*fakeNode)
	for _, name := range []string{"a", "b", "c", "d"} {
		f := newFakeNode(1000, 500)
		f.delay = time.Second
		fakes[name] = f
	}
	c := newCluster(t, fakes)

	begin := time.Now()
	c.checker.CheckOnce(context.Background())
	took := time.Since(begin)

	if took > 2500*time.Millisecond {
		t.Fatalf("round took %v, nodes seem to be probed one by one", took)
	}
	if got := len(c.pool.GetAllAliveNodes()); got != 4 {
		t.Fatalf("incorrect alive nodes: got %d, want 4", got)
	}
}

// A node that never answers must not stay alive and must not block the others
// beyond the per-node timeout.
func TestCheckOnceHangingNodeIsDeadAndDoesNotBlockOthers(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the per-node timeout")
	}
	hanging := newFakeNode(1000, 500)
	hanging.hang.Store(true)
	c := newCluster(t, map[string]*fakeNode{
		"hanging": hanging,
		"ok":      newFakeNode(1000, 500),
	})

	begin := time.Now()
	c.checker.CheckOnce(context.Background())
	took := time.Since(begin)

	if c.nodes["hanging"].IsAlive() {
		t.Fatalf("hanging node must not be alive")
	}
	if !c.nodes["ok"].IsAlive() {
		t.Fatalf("healthy node must be alive even when another one hangs")
	}
	if took > 8*time.Second {
		t.Fatalf("round took %v, per-node timeout does not work", took)
	}
}

// Transitions are logged once, not on every round.
func TestCheckOnceLogsTransitionsOnce(t *testing.T) {
	var mu sync.Mutex
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}), nil))
	logged := func(msg string) int {
		mu.Lock()
		defer mu.Unlock()
		return strings.Count(buf.String(), msg)
	}

	f := newFakeNode(1000, 400)
	c := newClusterWithLogger(t, log, map[string]*fakeNode{"a": f})
	ctx := context.Background()

	for range 3 {
		c.checker.CheckOnce(ctx)
	}
	if got := logged("node up"); got != 1 {
		t.Fatalf("\"node up\" logged %d times over three healthy rounds, want 1", got)
	}

	f.failing.Store(true)
	for range 3 {
		c.checker.CheckOnce(ctx)
	}
	if got := logged("node down"); got != 1 {
		t.Fatalf("\"node down\" logged %d times over three failing rounds, want 1", got)
	}
}

type writerFunc func([]byte) (int, error)

func (w writerFunc) Write(p []byte) (int, error) { return w(p) }

func TestCheckOnceWhilePoolGrows(t *testing.T) {
	f := newFakeNode(1000, 500)
	c := newCluster(t, map[string]*fakeNode{"a": f})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 20 {
			c.checker.CheckOnce(context.Background())
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 20 {
			// unreachable extra nodes keep arriving while the checker runs
			node := nodepool.NewNode("extra-"+string(rune('a'+i)), "passthrough:///extra-"+string(rune('a'+i)))
			if err := c.pool.Add([]*nodepool.Node{node}); err != nil {
				t.Errorf("Add returned error: %v", err)
			}
		}
	}()
	wg.Wait()

	if !c.nodes["a"].IsAlive() {
		t.Fatalf("node a must be alive")
	}
}

func TestRunChecksPeriodicallyAndStopsOnContextCancel(t *testing.T) {
	f := newFakeNode(1000, 400)
	c := newCluster(t, map[string]*fakeNode{"a": f})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.checker.Run(ctx, 20*time.Millisecond)
	}()

	waitFor(t, 3*time.Second, "at least 3 periodic checks", func() bool { return f.calls.Load() >= 3 })
	if !c.nodes["a"].IsAlive() {
		t.Fatalf("node must be alive after periodic checks")
	}

	// the status change must be picked up by a later round
	f.free.Store(50)
	waitFor(t, 3*time.Second, "refreshed free space", func() bool {
		return c.nodes["a"].GetStatus().FreeSpace == 50
	})

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return after the context was cancelled")
	}
}
