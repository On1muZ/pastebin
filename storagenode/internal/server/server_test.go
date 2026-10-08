package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"pastebin/storagenode/internal/blob"
)

type testEnv struct {
	client  storagenodev1.StorageNodeClient
	store   *blob.Store
	dataDir string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	dataDir := t.TempDir()
	store, err := blob.NewStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	storagenodev1.RegisterStorageNodeServer(srv, New(store, slog.New(slog.DiscardHandler)))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return &testEnv{
		client:  storagenodev1.NewStorageNodeClient(conn),
		store:   store,
		dataDir: dataDir,
	}
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func headerMsg(id string, size int64) *storagenodev1.PutBlobRequest {
	return &storagenodev1.PutBlobRequest{
		Payload: &storagenodev1.PutBlobRequest_Header{
			Header: &storagenodev1.BlobHeader{BlobId: id, SizeBytes: size},
		},
	}
}

func chunkMsg(b []byte) *storagenodev1.PutBlobRequest {
	return &storagenodev1.PutBlobRequest{
		Payload: &storagenodev1.PutBlobRequest_Chunk{Chunk: b},
	}
}

func (e *testEnv) putRaw(ctx context.Context, msgs ...*storagenodev1.PutBlobRequest) (*storagenodev1.PutBlobResponse, error) {
	stream, err := e.client.PutBlob(ctx)
	if err != nil {
		return nil, err
	}
	for _, m := range msgs {
		if err := stream.Send(m); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
	}
	return stream.CloseAndRecv()
}

func (e *testEnv) putBlob(ctx context.Context, id string, data []byte, sendChunk int) (*storagenodev1.PutBlobResponse, error) {
	msgs := []*storagenodev1.PutBlobRequest{headerMsg(id, int64(len(data)))}
	for len(data) > 0 {
		n := min(sendChunk, len(data))
		msgs = append(msgs, chunkMsg(data[:n]))
		data = data[n:]
	}
	return e.putRaw(ctx, msgs...)
}

func (e *testEnv) getBlob(ctx context.Context, req *storagenodev1.GetBlobRequest) ([]byte, int, error) {
	stream, err := e.client.GetBlob(ctx, req)
	if err != nil {
		return nil, 0, err
	}
	var buf bytes.Buffer
	msgs := 0
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return buf.Bytes(), msgs, nil
		}
		if err != nil {
			return nil, msgs, err
		}
		buf.Write(resp.GetChunk())
		msgs++
	}
}

func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("code = %v, want %v (err: %v)", got, want, err)
	}
}

func (e *testEnv) assertNoBlob(t *testing.T, id string) {
	t.Helper()
	f, err := e.store.Open(id)
	if err == nil {
		f.Close()
		t.Fatalf("blob %s exists, but should not", id)
	}
	if !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Open(%s): %v", id, err)
	}
}

func (e *testEnv) tmpFiles(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(e.dataDir, "tmp"))
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func randomData(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func newID(t *testing.T) string {
	return hex.EncodeToString(randomData(t, 16))
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestPutGetRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		size      int
		sendChunk int
	}{
		{"empty", 0, 1024},
		{"one byte", 1, 1024},
		{"exactly one server chunk", chunkSize, 64 * 1024},
		{"several chunks with remainder", 3*chunkSize + 123, 100 * 1000},
		{"client chunks bigger than server chunks", 2 * chunkSize, 1 << 20},
		{"20 MB", 20 << 20, 1 << 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTestEnv(t)
			ctx := testCtx(t)
			id := newID(t)
			data := randomData(t, tt.size)

			resp, err := env.putBlob(ctx, id, data, tt.sendChunk)
			if err != nil {
				t.Fatalf("PutBlob: %v", err)
			}
			if resp.GetSizeBytes() != int64(tt.size) {
				t.Errorf("size = %d, want %d", resp.GetSizeBytes(), tt.size)
			}
			if want := sha256Hex(data); resp.GetSha256() != want {
				t.Errorf("sha256 = %s, want %s", resp.GetSha256(), want)
			}

			got, _, err := env.getBlob(ctx, &storagenodev1.GetBlobRequest{BlobId: id})
			if err != nil {
				t.Fatalf("GetBlob: %v", err)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("downloaded data differs: got %d bytes, want %d", len(got), len(data))
			}
		})
	}
}

func TestGetBlobChunking(t *testing.T) {
	env := newTestEnv(t)
	ctx := testCtx(t)
	id := newID(t)

	data := randomData(t, 3*chunkSize+1)
	if _, err := env.putBlob(ctx, id, data, 1<<20); err != nil {
		t.Fatal(err)
	}

	_, msgs, err := env.getBlob(ctx, &storagenodev1.GetBlobRequest{BlobId: id})
	if err != nil {
		t.Fatal(err)
	}
	if msgs != 4 {
		t.Errorf("got %d messages, want 4", msgs)
	}
}

func TestGetBlobRange(t *testing.T) {
	env := newTestEnv(t)
	ctx := testCtx(t)
	id := newID(t)
	if _, err := env.putBlob(ctx, id, []byte("hello world"), 1024); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name           string
		offset, length int64
		want           string
		code           codes.Code
	}{
		{"whole blob", 0, 0, "hello world", codes.OK},
		{"from offset to end", 6, 0, "world", codes.OK},
		{"prefix", 0, 5, "hello", codes.OK},
		{"middle", 4, 3, "o w", codes.OK},
		{"length past end is trimmed", 6, 100, "world", codes.OK},
		{"offset at end gives empty", 11, 0, "", codes.OK},
		{"offset past end", 12, 0, "", codes.OutOfRange},
		{"negative offset", -1, 0, "", codes.InvalidArgument},
		{"negative length", 0, -1, "", codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := env.getBlob(ctx, &storagenodev1.GetBlobRequest{
				BlobId: id, Offset: tt.offset, Length: tt.length,
			})
			assertCode(t, err, tt.code)
			if tt.code == codes.OK && string(got) != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetBlobErrors(t *testing.T) {
	env := newTestEnv(t)
	ctx := testCtx(t)

	tests := []struct {
		name string
		id   string
		code codes.Code
	}{
		{"not found", newID(t), codes.NotFound},
		{"invalid id", "abc", codes.InvalidArgument},
		{"path traversal", "../../etc/passwd", codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := env.getBlob(ctx, &storagenodev1.GetBlobRequest{BlobId: tt.id})
			assertCode(t, err, tt.code)
		})
	}
}

func TestPutBlobErrors(t *testing.T) {
	hello := []byte("hello")

	tests := []struct {
		name string
		msgs func(id string) []*storagenodev1.PutBlobRequest
	}{
		{"declared more than sent", func(id string) []*storagenodev1.PutBlobRequest {
			return []*storagenodev1.PutBlobRequest{headerMsg(id, 10), chunkMsg(hello)}
		}},
		{"declared less than sent", func(id string) []*storagenodev1.PutBlobRequest {
			return []*storagenodev1.PutBlobRequest{headerMsg(id, 3), chunkMsg(hello)}
		}},
		{"negative size", func(id string) []*storagenodev1.PutBlobRequest {
			return []*storagenodev1.PutBlobRequest{headerMsg(id, -1)}
		}},
		{"invalid id", func(string) []*storagenodev1.PutBlobRequest {
			return []*storagenodev1.PutBlobRequest{headerMsg("../../etc/passwd", 5), chunkMsg(hello)}
		}},
		{"first message is chunk", func(string) []*storagenodev1.PutBlobRequest {
			return []*storagenodev1.PutBlobRequest{chunkMsg(hello), chunkMsg(hello)}
		}},
		{"header twice", func(id string) []*storagenodev1.PutBlobRequest {
			return []*storagenodev1.PutBlobRequest{headerMsg(id, 5), headerMsg(id, 5), chunkMsg(hello)}
		}},
		{"no messages at all", func(string) []*storagenodev1.PutBlobRequest {
			return nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTestEnv(t)
			ctx := testCtx(t)
			id := newID(t)

			_, err := env.putRaw(ctx, tt.msgs(id)...)
			assertCode(t, err, codes.InvalidArgument)

			env.assertNoBlob(t, id)
			if n := env.tmpFiles(t); n != 0 {
				t.Errorf("tmp not cleaned: %d files left", n)
			}
		})
	}
}

func TestPutBlobAlreadyExists(t *testing.T) {
	env := newTestEnv(t)
	ctx := testCtx(t)
	id := newID(t)
	original := []byte("original")

	if _, err := env.putBlob(ctx, id, original, 1024); err != nil {
		t.Fatal(err)
	}
	_, err := env.putBlob(ctx, id, []byte("overwrite"), 1024)
	assertCode(t, err, codes.AlreadyExists)

	got, _, err := env.getBlob(ctx, &storagenodev1.GetBlobRequest{BlobId: id})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("got %q, want %q", got, original)
	}
}

func TestDeleteBlob(t *testing.T) {
	env := newTestEnv(t)
	ctx := testCtx(t)
	id := newID(t)

	if _, err := env.putBlob(ctx, id, []byte("x"), 1024); err != nil {
		t.Fatal(err)
	}

	_, err := env.client.DeleteBlob(ctx, &storagenodev1.DeleteBlobRequest{BlobId: id})
	assertCode(t, err, codes.OK)

	_, _, err = env.getBlob(ctx, &storagenodev1.GetBlobRequest{BlobId: id})
	assertCode(t, err, codes.NotFound)

	_, err = env.client.DeleteBlob(ctx, &storagenodev1.DeleteBlobRequest{BlobId: id})
	assertCode(t, err, codes.OK)

	_, err = env.client.DeleteBlob(ctx, &storagenodev1.DeleteBlobRequest{BlobId: "../../etc/passwd"})
	assertCode(t, err, codes.InvalidArgument)
}

func TestGetStatus(t *testing.T) {
	env := newTestEnv(t)
	ctx := testCtx(t)

	status := func() *storagenodev1.GetStatusResponse {
		t.Helper()
		resp, err := env.client.GetStatus(ctx, &storagenodev1.GetStatusRequest{})
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	st := status()
	if st.GetBlobCount() != 0 {
		t.Errorf("BlobCount = %d, want 0", st.GetBlobCount())
	}
	if st.GetTotalBytes() <= 0 || st.GetFreeBytes() <= 0 || st.GetFreeBytes() > st.GetTotalBytes() {
		t.Errorf("weird disk stats: total=%d free=%d", st.GetTotalBytes(), st.GetFreeBytes())
	}

	id1, id2 := newID(t), newID(t)
	for _, id := range []string{id1, id2} {
		if _, err := env.putBlob(ctx, id, []byte("x"), 1024); err != nil {
			t.Fatal(err)
		}
	}
	if got := status().GetBlobCount(); got != 2 {
		t.Errorf("after 2 puts BlobCount = %d, want 2", got)
	}

	if _, err := env.client.DeleteBlob(ctx, &storagenodev1.DeleteBlobRequest{BlobId: id1}); err != nil {
		t.Fatal(err)
	}
	if got := status().GetBlobCount(); got != 1 {
		t.Errorf("after delete BlobCount = %d, want 1", got)
	}
}

func TestPutBlobClientCancel(t *testing.T) {
	env := newTestEnv(t)
	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()
	id := newID(t)

	stream, err := env.client.PutBlob(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(headerMsg(id, 1<<20)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(chunkMsg(randomData(t, 1024))); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "upload started (temp file created)", func() bool { return env.tmpFiles(t) == 1 })
	cancel()
	waitFor(t, "temp file removed", func() bool { return env.tmpFiles(t) == 0 })

	env.assertNoBlob(t, id)
}

func TestConcurrentPuts(t *testing.T) {
	env := newTestEnv(t)
	ctx := testCtx(t)

	const n = 20
	ids := make([]string, n)
	blobs := make([][]byte, n)
	for i := range n {
		ids[i] = newID(t)
		blobs[i] = randomData(t, 100*1024+i)
	}

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			if _, err := env.putBlob(ctx, ids[i], blobs[i], 16*1024); err != nil {
				t.Errorf("PutBlob #%d: %v", i, err)
			}
		})
	}
	wg.Wait()

	for i := range n {
		got, _, err := env.getBlob(ctx, &storagenodev1.GetBlobRequest{BlobId: ids[i]})
		if err != nil {
			t.Fatalf("GetBlob #%d: %v", i, err)
		}
		if !bytes.Equal(got, blobs[i]) {
			t.Errorf("blob #%d corrupted", i)
		}
	}
}
