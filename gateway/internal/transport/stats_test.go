package transport

import (
	"context"
	"log/slog"
	"net"
	"pastebin/gateway/internal/nodepool"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type blockingNode struct {
	storagenodev1.UnimplementedStorageNodeServer
	started chan struct{}
}

func (b *blockingNode) GetBlob(_ *storagenodev1.GetBlobRequest, stream storagenodev1.StorageNode_GetBlobServer) error {
	b.started <- struct{}{}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (b *blockingNode) GetStatus(context.Context, *storagenodev1.GetStatusRequest) (*storagenodev1.GetStatusResponse, error) {
	return &storagenodev1.GetStatusResponse{}, nil
}

func startBlockingNode(t *testing.T) (*Transport, *nodepool.Node, *blockingNode) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	bn := &blockingNode{started: make(chan struct{}, 1)}
	storagenodev1.RegisterStorageNodeServer(srv, bn)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	dial := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
	tr := New(WithLogger(slog.New(slog.DiscardHandler)), WithDialOpts(dial))
	t.Cleanup(func() { tr.Close() })
	return tr, nodepool.NewNode("a", "passthrough:///a"), bn
}

func waitConns(t *testing.T, node *nodepool.Node, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for node.GetAliveConns() != want {
		if time.Now().After(deadline) {
			t.Fatalf("alive conns: got %d, want %d", node.GetAliveConns(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStatsCountsStreamWhileInFlight(t *testing.T) {
	tr, node, bn := startBlockingNode(t)
	client, err := tr.Client(node)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.GetBlob(ctx, &storagenodev1.GetBlobRequest{BlobId: "x"})
	if err != nil {
		t.Fatal(err)
	}
	<-bn.started
	waitConns(t, node, 1)

	cancel()
	stream.Recv()
	waitConns(t, node, 0)
}

func TestStatsReleasesOnError(t *testing.T) {
	tr, node, _ := startBlockingNode(t)
	client, err := tr.Client(node)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.DeleteBlob(ctx, &storagenodev1.DeleteBlobRequest{BlobId: "x"}); err == nil {
		t.Fatal("DeleteBlob is unimplemented, error expected")
	}
	waitConns(t, node, 0)
}

func TestStatsIgnoresGetStatus(t *testing.T) {
	tr, node, _ := startBlockingNode(t)
	client, err := tr.Client(node)
	if err != nil {
		t.Fatal(err)
	}

	for range 10 {
		if _, err := callStatus(client); err != nil {
			t.Fatal(err)
		}
		if got := node.GetAliveConns(); got != 0 {
			t.Fatalf("GetStatus must not be counted, got %d alive conns", got)
		}
	}
}
