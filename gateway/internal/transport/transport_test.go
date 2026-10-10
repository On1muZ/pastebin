package transport

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"pastebin/gateway/internal/nodepool"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeNode is a minimal storage node: it only knows GetStatus and reports its
// own total_bytes, so a test can tell which node answered.
type fakeNode struct {
	storagenodev1.UnimplementedStorageNodeServer
	totalBytes int64
}

func (f *fakeNode) GetStatus(context.Context, *storagenodev1.GetStatusRequest) (*storagenodev1.GetStatusResponse, error) {
	return &storagenodev1.GetStatusResponse{TotalBytes: f.totalBytes, FreeBytes: f.totalBytes / 2}, nil
}

// fakeCluster is a set of fake nodes reachable through in-memory listeners.
// It counts how many times each node was dialed, which equals the number of
// real connections the transport opened to it.
type fakeCluster struct {
	listeners map[string]*bufconn.Listener
	dials     map[string]*atomic.Int32
}

// startCluster starts one fake node per entry: name -> reported total_bytes.
func startCluster(t *testing.T, totals map[string]int64) *fakeCluster {
	t.Helper()
	c := &fakeCluster{
		listeners: make(map[string]*bufconn.Listener),
		dials:     make(map[string]*atomic.Int32),
	}
	for name, total := range totals {
		lis := bufconn.Listen(1 << 20)
		srv := grpc.NewServer()
		storagenodev1.RegisterStorageNodeServer(srv, &fakeNode{totalBytes: total})
		go srv.Serve(lis)
		t.Cleanup(srv.Stop)
		c.listeners[name] = lis
		c.dials[name] = new(atomic.Int32)
	}
	return c
}

// dialOption routes every dial to the listener of the requested fake node.
// The address given to the dialer is the part after "passthrough:///".
func (c *fakeCluster) dialOption() grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		lis, ok := c.listeners[addr]
		if !ok {
			return nil, errors.New("no such fake node: " + addr)
		}
		c.dials[addr].Add(1)
		return lis.DialContext(ctx)
	})
}

// node returns a pool node whose address resolves to the fake node name.
func (c *fakeCluster) node(name string) *nodepool.Node {
	return nodepool.NewNode(name, "passthrough:///"+name)
}

func (c *fakeCluster) transport(t *testing.T) *Transport {
	t.Helper()
	tr := New(WithLogger(slog.New(slog.DiscardHandler)), WithDialOpts(c.dialOption()))
	t.Cleanup(func() { tr.Close() })
	return tr
}

func callStatus(client storagenodev1.StorageNodeClient) (*storagenodev1.GetStatusResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return client.GetStatus(ctx, &storagenodev1.GetStatusRequest{})
}

func mustTotalBytes(t *testing.T, client storagenodev1.StorageNodeClient) int64 {
	t.Helper()
	resp, err := callStatus(client)
	if err != nil {
		t.Fatalf("GetStatus returned error: %v", err)
	}
	return resp.GetTotalBytes()
}

func TestClientReturnsWorkingClient(t *testing.T) {
	c := startCluster(t, map[string]int64{"a": 1000})
	tr := c.transport(t)

	client, err := tr.Client(c.node("a"))
	if err != nil {
		t.Fatalf("Client returned error: %v", err)
	}
	if got := mustTotalBytes(t, client); got != 1000 {
		t.Fatalf("incorrect total_bytes: got %d, want 1000", got)
	}
}

func TestClientRoutesToTheRightNode(t *testing.T) {
	c := startCluster(t, map[string]int64{"a": 1000, "b": 2000})
	tr := c.transport(t)

	clientA, err := tr.Client(c.node("a"))
	if err != nil {
		t.Fatal(err)
	}
	clientB, err := tr.Client(c.node("b"))
	if err != nil {
		t.Fatal(err)
	}

	if got := mustTotalBytes(t, clientA); got != 1000 {
		t.Fatalf("client of node a reached the wrong node: total_bytes %d, want 1000", got)
	}
	if got := mustTotalBytes(t, clientB); got != 2000 {
		t.Fatalf("client of node b reached the wrong node: total_bytes %d, want 2000", got)
	}
}

func TestClientReusesConnectionForSameNode(t *testing.T) {
	c := startCluster(t, map[string]int64{"a": 1000})
	tr := c.transport(t)
	node := c.node("a")

	first, err := tr.Client(node)
	if err != nil {
		t.Fatal(err)
	}
	mustTotalBytes(t, first)
	for range 5 {
		again, err := tr.Client(node)
		if err != nil {
			t.Fatal(err)
		}
		if again != first {
			t.Fatalf("the same node must always give the same client")
		}
		mustTotalBytes(t, again)
	}

	if got := c.dials["a"].Load(); got != 1 {
		t.Fatalf("incorrect number of connections to node a: got %d, want 1", got)
	}
}

func TestClientOpensSeparateConnectionPerNode(t *testing.T) {
	c := startCluster(t, map[string]int64{"a": 1000, "b": 2000})
	tr := c.transport(t)

	clientA, err := tr.Client(c.node("a"))
	if err != nil {
		t.Fatal(err)
	}
	clientB, err := tr.Client(c.node("b"))
	if err != nil {
		t.Fatal(err)
	}
	if clientA == clientB {
		t.Fatalf("different nodes must not share a client")
	}
	mustTotalBytes(t, clientA)
	mustTotalBytes(t, clientB)

	for _, name := range []string{"a", "b"} {
		if got := c.dials[name].Load(); got != 1 {
			t.Fatalf("incorrect number of connections to node %s: got %d, want 1", name, got)
		}
	}
}

// Many goroutines ask for the same node at the same moment. Exactly one
// connection must be opened (double-checked locking). Run with -race.
func TestClientConcurrentCallsOpenOneConnection(t *testing.T) {
	c := startCluster(t, map[string]int64{"a": 1000})
	tr := c.transport(t)
	node := c.node("a")

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			client, err := tr.Client(node)
			if err != nil {
				t.Errorf("Client returned error: %v", err)
				return
			}
			if _, err := callStatus(client); err != nil {
				t.Errorf("GetStatus returned error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := c.dials["a"].Load(); got != 1 {
		t.Fatalf("incorrect number of connections after concurrent calls: got %d, want 1", got)
	}
}

func TestClientConcurrentCallsForDifferentNodes(t *testing.T) {
	names := []string{"a", "b", "c", "d"}
	totals := make(map[string]int64)
	for i, name := range names {
		totals[name] = int64(1000 * (i + 1))
	}
	c := startCluster(t, totals)
	tr := c.transport(t)

	var wg sync.WaitGroup
	for range 16 {
		for _, name := range names {
			wg.Add(1)
			go func() {
				defer wg.Done()
				client, err := tr.Client(c.node(name))
				if err != nil {
					t.Errorf("Client(%s) returned error: %v", name, err)
					return
				}
				resp, err := callStatus(client)
				if err != nil {
					t.Errorf("GetStatus(%s) returned error: %v", name, err)
					return
				}
				if resp.GetTotalBytes() != totals[name] {
					t.Errorf("node %s answered with total_bytes %d, want %d", name, resp.GetTotalBytes(), totals[name])
				}
			}()
		}
	}
	wg.Wait()

	for _, name := range names {
		if got := c.dials[name].Load(); got != 1 {
			t.Fatalf("incorrect number of connections to node %s: got %d, want 1", name, got)
		}
	}
}

func TestClientNilNode(t *testing.T) {
	tr := New(WithLogger(slog.New(slog.DiscardHandler)))
	defer tr.Close()

	client, err := tr.Client(nil)
	if err == nil {
		t.Fatalf("nil node must return an error")
	}
	if client != nil {
		t.Fatalf("client must be nil on error")
	}
}

func TestClientWorksWithDefaultDialOptions(t *testing.T) {
	// No WithDialOpts: the default insecure credentials must be enough for
	// grpc.NewClient to succeed (it does not connect until the first call).
	tr := New(WithLogger(slog.New(slog.DiscardHandler)))
	defer tr.Close()

	client, err := tr.Client(nodepool.NewNode("a", "localhost:1"))
	if err != nil {
		t.Fatalf("Client returned error with default dial options: %v", err)
	}
	if client == nil {
		t.Fatalf("client must not be nil")
	}
}

func TestClientUnavailableNode(t *testing.T) {
	c := startCluster(t, map[string]int64{"a": 1000})
	tr := c.transport(t)

	// "ghost" has no listener: the dialer returns an error.
	client, err := tr.Client(c.node("ghost"))
	if err != nil {
		t.Fatalf("Client must not fail for an unreachable node, connection is lazy: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = client.GetStatus(ctx, &storagenodev1.GetStatusRequest{})
	if err == nil {
		t.Fatalf("call to an unreachable node must fail")
	}
	switch code := status.Code(err); code {
	case codes.Unavailable, codes.DeadlineExceeded:
	default:
		t.Fatalf("incorrect error code: got %v, want Unavailable or DeadlineExceeded (%v)", code, err)
	}
}

func TestClientAfterClose(t *testing.T) {
	c := startCluster(t, map[string]int64{"a": 1000, "b": 2000})
	tr := c.transport(t)

	// "a" already has a connection (fast path), "b" has none (slow path).
	existing, err := tr.Client(c.node("a"))
	if err != nil {
		t.Fatal(err)
	}
	mustTotalBytes(t, existing)

	if err := tr.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	for _, name := range []string{"a", "b"} {
		client, err := tr.Client(c.node(name))
		if !errors.Is(err, ErrTransportClosed) {
			t.Fatalf("node %s: incorrect error: got %v, want %v", name, err, ErrTransportClosed)
		}
		if client != nil {
			t.Fatalf("node %s: client must be nil on error", name)
		}
	}
	if got := c.dials["b"].Load(); got != 0 {
		t.Fatalf("closed transport must not open connections: got %d", got)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	c := startCluster(t, map[string]int64{"a": 1000, "b": 2000})
	tr := c.transport(t)
	for _, name := range []string{"a", "b"} {
		client, err := tr.Client(c.node(name))
		if err != nil {
			t.Fatal(err)
		}
		mustTotalBytes(t, client)
	}

	for i := range 3 {
		if err := tr.Close(); err != nil {
			t.Fatalf("Close call %d returned error: %v", i+1, err)
		}
	}
}

func TestCloseClosesConnections(t *testing.T) {
	c := startCluster(t, map[string]int64{"a": 1000, "b": 2000})
	tr := c.transport(t)

	var clients []storagenodev1.StorageNodeClient
	for _, name := range []string{"a", "b"} {
		client, err := tr.Client(c.node(name))
		if err != nil {
			t.Fatal(err)
		}
		mustTotalBytes(t, client)
		clients = append(clients, client)
	}

	if err := tr.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	for i, client := range clients {
		_, err := callStatus(client)
		if err == nil {
			t.Fatalf("client %d must not work after Close", i)
		}
		if code := status.Code(err); code != codes.Canceled {
			t.Fatalf("client %d: incorrect error code after Close: got %v, want Canceled (%v)", i, code, err)
		}
	}
}

// Client calls racing with Close must neither panic nor deadlock.
func TestCloseRacingWithClient(t *testing.T) {
	c := startCluster(t, map[string]int64{"a": 1000})
	tr := c.transport(t)
	node := c.node("a")

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// either a working client or ErrTransportClosed, nothing else
			if _, err := tr.Client(node); err != nil && !errors.Is(err, ErrTransportClosed) {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := tr.Close(); err != nil {
			t.Errorf("Close returned error: %v", err)
		}
	}()
	wg.Wait()
}
