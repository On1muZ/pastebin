package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"pastebin/gateway/internal/balancer"
	"pastebin/gateway/internal/metadata"
	"pastebin/gateway/internal/nodeio"
	"pastebin/gateway/internal/nodepool"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	writeTimeout    = 4 * time.Second
	metadataTimeout = 2 * time.Second
)

var (
	errAllUploadsFailed = errors.New("all uploads failed")
	errSizeMismatch     = errors.New("size mismatch")
)

func (s *Server) PutBlob(stream storagenodev1.StorageNode_PutBlobServer) error {
	hdr, err := recvHeader(stream)
	if err != nil {
		return err
	}
	id, size := hdr.GetBlobId(), hdr.GetSizeBytes()

	if err := s.store.CreateBlob(stream.Context(), id, size); err != nil {
		return s.toStatus(err)
	}
	nodes, err := s.pickNodes(size)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(stream.Context(), writeTimeout)
	defer cancel()
	f := s.startUploads(ctx, id, size, nodes)
	if len(f.uploads) == 0 {
		return status.Error(codes.Unavailable, "could not start upload on any node")
	}

	sha, pumpErr := s.pump(stream, size, f)
	committed := s.settle(stream.Context(), id, sha, f.uploads)
	if pumpErr != nil {
		return pumpErr
	}
	if committed == 0 {
		s.log.Error("all picked nodes could not write file", slog.String("blob_id", id))
		return status.Error(codes.Unavailable, "could not write blob to any node")
	}

	mctx, mcancel := metadataContext(stream.Context())
	defer mcancel()
	if err := s.store.CommitBlob(mctx, id, sha); err != nil {
		return s.toStatus(err)
	}
	return stream.SendAndClose(&storagenodev1.PutBlobResponse{
		BlobId:    id,
		SizeBytes: size,
		Sha256:    sha,
	})
}

func recvHeader(stream storagenodev1.StorageNode_PutBlobServer) (*storagenodev1.BlobHeader, error) {
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil, status.Error(codes.InvalidArgument, "empty stream: header expected")
	}
	if err != nil {
		return nil, err
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return nil, status.Error(codes.InvalidArgument, "first message must be header")
	}
	if !validID(hdr.GetBlobId()) || hdr.GetSizeBytes() < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid blob id or size")
	}
	return hdr, nil
}

func validID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return false
		}
	}
	return true
}

func (s *Server) pickNodes(size int64) ([]*nodepool.Node, error) {
	w := len(s.pool.GetAllAliveNodes())/2 + 1
	picked := make([]*nodepool.Node, 0, w)
	var balancerErr error
	for len(picked) < w {
		var node *nodepool.Node
		node, balancerErr = s.balancer.GetNode(size, picked)
		if balancerErr != nil {
			break
		}
		picked = append(picked, node)
	}
	if len(picked) > 0 {
		return picked, nil
	}
	s.log.Error("could not pick any node", slog.String("error", balancerErr.Error()))
	if errors.Is(balancerErr, balancer.ErrNoNodesCanWriteThisFile) {
		return nil, status.Error(codes.ResourceExhausted, "no node has enough free space")
	}
	return nil, status.Error(codes.Unavailable, "no alive nodes")
}

type upload struct {
	node   *nodepool.Node
	pw     *io.PipeWriter
	failed bool
	sha    string
	err    error
}

type fanout struct {
	uploads []*upload
	wg      sync.WaitGroup
}

func (f *fanout) Write(p []byte) (int, error) {
	alive := 0
	for _, u := range f.uploads {
		if u.failed {
			continue
		}
		if _, err := u.pw.Write(p); err != nil {
			u.failed = true
			continue
		}
		alive++
	}
	if alive == 0 {
		return 0, errAllUploadsFailed
	}
	return len(p), nil
}

func (f *fanout) finish(err error) {
	for _, u := range f.uploads {
		u.pw.CloseWithError(err)
	}
	f.wg.Wait()
}

func (s *Server) startUploads(ctx context.Context, id string, size int64, nodes []*nodepool.Node) *fanout {
	f := &fanout{}
	for _, node := range nodes {
		if err := s.store.AddReplica(ctx, id, node.ID); err != nil {
			s.log.Error("could not add replica", slog.String("error", err.Error()), slog.String("node_id", node.ID), slog.String("blob_id", id))
			continue
		}
		client, err := s.transport.Client(node)
		if err != nil {
			s.log.Error("could not open client", slog.String("error", err.Error()), slog.String("node_id", node.ID), slog.String("blob_id", id))
			s.dropReplica(ctx, id, node.ID)
			continue
		}
		pr, pw := io.Pipe()
		u := &upload{node: node, pw: pw}
		f.uploads = append(f.uploads, u)
		f.wg.Go(func() {
			u.sha, u.err = nodeio.Write(ctx, client, id, size, pr)
			if u.err != nil {
				pr.CloseWithError(u.err)
			}
		})
	}
	return f
}

func (s *Server) pump(stream storagenodev1.StorageNode_PutBlobServer, size int64, f *fanout) (string, error) {
	h := sha256.New()
	r := io.TeeReader(io.LimitReader(&chunkReader{stream: stream}, size+1), h)
	n, err := io.CopyBuffer(f, r, make([]byte, chunkSize))
	switch {
	case errors.Is(err, errAllUploadsFailed):
		f.finish(err)
		return "", nil
	case err != nil:
		f.finish(err)
		return "", s.toStatus(err)
	case n != size:
		f.finish(errSizeMismatch)
		return "", status.Errorf(codes.InvalidArgument, "size mismatch: header says %d, got %d", size, n)
	}
	f.finish(nil)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (s *Server) settle(ctx context.Context, id, sha string, uploads []*upload) int {
	ctx, cancel := metadataContext(ctx)
	defer cancel()
	committed := 0
	for _, u := range uploads {
		switch {
		case sha == "":
			s.dropReplica(ctx, id, u.node.ID)
		case u.err == nil && u.sha == sha:
			if err := s.store.MarkReplicaCommitted(ctx, id, u.node.ID); err != nil {
				s.log.Error("could not commit replica", slog.String("error", err.Error()), slog.String("node_id", u.node.ID), slog.String("blob_id", id))
				continue
			}
			committed++
		case u.err == nil:
			s.log.Error("sha256 mismatch", slog.String("node_id", u.node.ID), slog.String("blob_id", id), slog.String("node_sha256", u.sha), slog.String("gateway_sha256", sha))
		case isUnknownOutcome(u.err):
			s.log.Warn("unknown write outcome", slog.String("error", u.err.Error()), slog.String("node_id", u.node.ID), slog.String("blob_id", id))
		default:
			s.log.Warn("node could not write file", slog.String("error", u.err.Error()), slog.String("node_id", u.node.ID), slog.String("blob_id", id))
			s.dropReplica(ctx, id, u.node.ID)
		}
	}
	return committed
}

func isUnknownOutcome(err error) bool {
	switch status.Code(err) {
	case codes.DeadlineExceeded, codes.Canceled:
		return true
	}
	return false
}

func (s *Server) dropReplica(ctx context.Context, id, nodeID string) {
	if err := s.store.DeleteReplica(ctx, id, nodeID); err != nil && !errors.Is(err, metadata.ErrNotFound) {
		s.log.Error("could not delete replica", slog.String("error", err.Error()), slog.String("node_id", nodeID), slog.String("blob_id", id))
	}
}

func metadataContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), metadataTimeout)
}

type chunkReader struct {
	stream storagenodev1.StorageNode_PutBlobServer
	buf    []byte
}

func (r *chunkReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		req, err := r.stream.Recv()
		if err != nil {
			return 0, err
		}
		chunk, ok := req.GetPayload().(*storagenodev1.PutBlobRequest_Chunk)
		if !ok {
			return 0, status.Error(codes.InvalidArgument, "expected chunk")
		}
		r.buf = chunk.Chunk
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}
