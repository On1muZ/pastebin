package nodeio

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	storagenodev1 "pastebin/gen/go/storagenode/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeNode is an in-memory storage node. It stores a blob only after the
// whole stream was received, like the real node does with its temp file.
type fakeNode struct {
	storagenodev1.UnimplementedStorageNodeServer

	mu    sync.Mutex
	blobs map[string][]byte

	// rejectAfterChunks makes PutBlob fail with ResourceExhausted after
	// receiving that many chunks; 0 means never reject.
	rejectAfterChunks int
	// sendChunkSize is the size of chunks GetBlob sends back.
	sendChunkSize int
	// putAborted receives the error PutBlob got while receiving chunks.
	putAborted chan error
	// chunkReceived is signalled every time PutBlob receives a chunk.
	chunkReceived chan struct{}
}

func (f *fakeNode) PutBlob(stream storagenodev1.StorageNode_PutBlobServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "header expected")
	}
	var data bytes.Buffer
	chunks := 0
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			select {
			case f.putAborted <- err:
			default:
			}
			return err
		}
		data.Write(req.GetChunk())
		chunks++
		select {
		case f.chunkReceived <- struct{}{}:
		default:
		}
		if f.rejectAfterChunks > 0 && chunks >= f.rejectAfterChunks {
			return status.Error(codes.ResourceExhausted, "disk full")
		}
	}
	f.mu.Lock()
	f.blobs[header.GetBlobId()] = data.Bytes()
	f.mu.Unlock()
	sum := sha256.Sum256(data.Bytes())
	return stream.SendAndClose(&storagenodev1.PutBlobResponse{
		BlobId:    header.GetBlobId(),
		SizeBytes: int64(data.Len()),
		Sha256:    hex.EncodeToString(sum[:]),
	})
}

func (f *fakeNode) GetBlob(req *storagenodev1.GetBlobRequest, stream storagenodev1.StorageNode_GetBlobServer) error {
	f.mu.Lock()
	data, ok := f.blobs[req.GetBlobId()]
	f.mu.Unlock()
	if !ok {
		return status.Error(codes.NotFound, "blob not found")
	}
	data = data[req.GetOffset():]
	for len(data) > 0 {
		n := min(len(data), f.sendChunkSize)
		if err := stream.Send(&storagenodev1.GetBlobResponse{Chunk: data[:n]}); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

func (f *fakeNode) blob(id string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.blobs[id]
	return data, ok
}

// startFakeNode runs fakeNode over bufconn and returns a client to it.
func startFakeNode(t *testing.T) (*fakeNode, storagenodev1.StorageNodeClient) {
	t.Helper()
	node := &fakeNode{
		blobs:         make(map[string][]byte),
		sendChunkSize: 1000,
		putAborted:    make(chan error, 1),
		chunkReceived: make(chan struct{}, 1),
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	storagenodev1.RegisterStorageNodeServer(srv, node)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient returned error: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return node, storagenodev1.NewStorageNodeClient(conn)
}

func randomBytes(t *testing.T, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand.Read returned error: %v", err)
	}
	return data
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func mustWrite(t *testing.T, client storagenodev1.StorageNodeClient, id string, data []byte) {
	t.Helper()
	if _, err := Write(context.Background(), client, id, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
}

func readAll(t *testing.T, client storagenodev1.StorageNodeClient, id string, offset int64) []byte {
	t.Helper()
	r, err := Read(context.Background(), client, id, offset, 0)
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading blob returned error: %v", err)
	}
	return data
}

// countingReader produces limit zero bytes and counts how many were read.
type countingReader struct {
	read  int
	limit int
}

func (c *countingReader) Read(p []byte) (int, error) {
	if c.read >= c.limit {
		return 0, io.EOF
	}
	n := min(len(p), c.limit-c.read)
	c.read += n
	return n, nil
}

// failingReader gives one small piece of data, waits until the node has
// received it and then fails. Waiting makes sure the node's handler is
// already running when the upload breaks.
type failingReader struct {
	sent          bool
	chunkReceived <-chan struct{}
}

var errSourceBroken = errors.New("source broken")

func (f *failingReader) Read(p []byte) (int, error) {
	if !f.sent {
		f.sent = true
		return copy(p, make([]byte, 100)), nil
	}
	select {
	case <-f.chunkReceived:
	case <-time.After(2 * time.Second):
	}
	return 0, errSourceBroken
}

func TestWriteAndReadRoundTrip(t *testing.T) {
	sizes := map[string]int{
		"empty":           0,
		"small":           10,
		"exactly a chunk": chunkSize,
		"several chunks":  3*chunkSize + 7,
	}
	for name, size := range sizes {
		t.Run(name, func(t *testing.T) {
			node, client := startFakeNode(t)
			data := randomBytes(t, size)

			sha, err := Write(context.Background(), client, "blob", int64(size), bytes.NewReader(data))
			if err != nil {
				t.Fatalf("Write returned error: %v", err)
			}
			if want := sha256Hex(data); sha != want {
				t.Fatalf("incorrect sha256: got %s, want %s", sha, want)
			}
			stored, ok := node.blob("blob")
			if !ok || !bytes.Equal(stored, data) {
				t.Fatalf("node stored incorrect data: got %d bytes, want %d", len(stored), len(data))
			}
			if got := readAll(t, client, "blob", 0); !bytes.Equal(got, data) {
				t.Fatalf("read incorrect data: got %d bytes, want %d", len(got), len(data))
			}
		})
	}
}

func TestReadFromOffset(t *testing.T) {
	_, client := startFakeNode(t)
	data := randomBytes(t, 5000)
	mustWrite(t, client, "blob", data)

	if got := readAll(t, client, "blob", 1234); !bytes.Equal(got, data[1234:]) {
		t.Fatalf("read from offset returned incorrect data: got %d bytes, want %d", len(got), len(data)-1234)
	}
}

// The caller's buffer is smaller than chunks coming from the node, so the
// reader has to keep the rest of a chunk between Read calls.
func TestReadWithBufferSmallerThanChunk(t *testing.T) {
	node, client := startFakeNode(t)
	node.sendChunkSize = 1000
	data := randomBytes(t, 4500)
	mustWrite(t, client, "blob", data)

	r, err := Read(context.Background(), client, "blob", 0, 0)
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	defer r.Close()
	var got []byte
	p := make([]byte, 300)
	for {
		n, err := r.Read(p)
		got = append(got, p[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Read returned error: %v", err)
		}
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read incorrect data: got %d bytes, want %d", len(got), len(data))
	}
}

// NotFound must come from Read itself, before the caller starts reading:
// the gateway server relies on it to switch to another node.
func TestReadReturnsNotFoundImmediately(t *testing.T) {
	_, client := startFakeNode(t)
	r, err := Read(context.Background(), client, "missing", 0, 0)
	if status.Code(err) != codes.NotFound {
		t.Fatalf("incorrect error: got %v, want code NotFound", err)
	}
	if r != nil {
		t.Fatal("Read returned a reader together with an error")
	}
}

func TestReadAfterCloseFails(t *testing.T) {
	_, client := startFakeNode(t)
	mustWrite(t, client, "blob", randomBytes(t, 5*chunkSize))

	r, err := Read(context.Background(), client, "blob", 0, 0)
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	if _, err := r.Read(make([]byte, 10)); err != nil {
		t.Fatalf("first Read returned error: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if _, err := io.ReadAll(r); status.Code(err) != codes.Canceled {
		t.Fatalf("incorrect error after Close: got %v, want code Canceled", err)
	}
}

// The node rejects the upload in the middle: Write must return the node's
// real error, not io.EOF from Send.
func TestWriteReturnsNodeError(t *testing.T) {
	node, client := startFakeNode(t)
	node.rejectAfterChunks = 1

	_, err := Write(context.Background(), client, "blob", 3*chunkSize, bytes.NewReader(make([]byte, 3*chunkSize)))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("incorrect error: got %v, want code ResourceExhausted", err)
	}
	if _, ok := node.blob("blob"); ok {
		t.Fatal("rejected blob was stored")
	}
}

// After the node rejected the upload, Write must stop pulling data from the
// source instead of draining it into a closed stream.
func TestWriteStopsReadingSourceAfterNodeRejects(t *testing.T) {
	node, client := startFakeNode(t)
	node.rejectAfterChunks = 1
	const total = 100 << 20
	src := &countingReader{limit: total}

	_, err := Write(context.Background(), client, "blob", total, src)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("incorrect error: got %v, want code ResourceExhausted", err)
	}
	if src.read >= total {
		t.Fatalf("Write read the whole source after the node rejected: read %d bytes", src.read)
	}
}

func TestWriteSourceFailureAbortsUpload(t *testing.T) {
	node, client := startFakeNode(t)

	_, err := Write(context.Background(), client, "blob", 1000, &failingReader{chunkReceived: node.chunkReceived})
	if !errors.Is(err, errSourceBroken) {
		t.Fatalf("incorrect error: got %v, want %v", err, errSourceBroken)
	}
	if _, ok := node.blob("blob"); ok {
		t.Fatal("half-written blob was stored")
	}
	// Write must cancel the stream, otherwise the node keeps waiting for
	// the rest of the blob.
	select {
	case err := <-node.putAborted:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("node saw incorrect error: got %v, want code Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("node was not notified that the upload was aborted")
	}
}

// Copy between two nodes, the way the repair worker will use the package.
func TestCopyBetweenNodes(t *testing.T) {
	_, clientA := startFakeNode(t)
	nodeB, clientB := startFakeNode(t)
	data := randomBytes(t, 2*chunkSize+123)
	mustWrite(t, clientA, "blob", data)

	src, err := Read(context.Background(), clientA, "blob", 0, 0)
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	defer src.Close()
	sha, err := Write(context.Background(), clientB, "blob", int64(len(data)), src)
	if err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if want := sha256Hex(data); sha != want {
		t.Fatalf("incorrect sha256: got %s, want %s", sha, want)
	}
	if stored, _ := nodeB.blob("blob"); !bytes.Equal(stored, data) {
		t.Fatalf("node B stored incorrect data: got %d bytes, want %d", len(stored), len(data))
	}
}

// io.Reader may return the last data together with io.EOF in one call;
// that data must not be lost.
func TestWriteSourceReturnsDataWithEOF(t *testing.T) {
	node, client := startFakeNode(t)
	data := randomBytes(t, 1000)
	src := iotest.DataErrReader(bytes.NewReader(data))

	if _, err := Write(context.Background(), client, "blob", int64(len(data)), src); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if stored, _ := node.blob("blob"); !bytes.Equal(stored, data) {
		t.Fatalf("node stored incorrect data: got %d bytes, want %d", len(stored), len(data))
	}
}
