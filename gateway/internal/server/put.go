package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"pastebin/gateway/internal/balancer"
	"pastebin/gateway/internal/nodeio"
	"pastebin/gateway/internal/nodepool"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const metadataTimeout = 2 * time.Second

func (s *Server) PutBlob(stream storagenodev1.StorageNode_PutBlobServer) error {
	hdr, err := recvHeader(stream)
	if err != nil {
		return err
	}
	id, size := hdr.GetBlobId(), hdr.GetSizeBytes()
	if size > s.cfg.MaxBlobSize {
		return status.Errorf(codes.InvalidArgument, "blob size %d is greater than max %d bytes", size, s.cfg.MaxBlobSize)
	}

	data, sha, err := s.readBody(stream, size)
	if err != nil {
		return err
	}

	nodes, err := s.pickNodes(size)
	if err != nil {
		return err
	}
	if err := s.store.StartUpload(stream.Context(), id, size, nodeIDs(nodes)); err != nil {
		return s.toStatus(err)
	}

	uploads, failed := s.writeToNodes(stream.Context(), id, data, nodes)
	if len(uploads) == 0 {
		s.dropReplicas(stream.Context(), id, failed)
		return status.Error(codes.Unavailable, "could not start upload on any node")
	}

	committed, dropped := s.settle(id, sha, size, uploads)
	failed = append(failed, dropped...)
	if len(committed) == 0 {
		s.dropReplicas(stream.Context(), id, failed)
		s.log.Error("all picked nodes could not write file", slog.String("blob_id", id))
		return status.Error(codes.Unavailable, "could not write blob to any node")
	}

	mctx, mcancel := metadataContext(stream.Context())
	defer mcancel()
	if err := s.store.FinishUpload(mctx, id, sha, committed, failed); err != nil {
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
	node *nodepool.Node
	sha  string
	err  error
}

func nodeIDs(nodes []*nodepool.Node) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

func (s *Server) readBody(stream storagenodev1.StorageNode_PutBlobServer, size int64) ([]byte, string, error) {
	r := &chunkReader{stream: stream}
	data := make([]byte, size)
	n, err := io.ReadFull(r, data)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, "", status.Errorf(codes.InvalidArgument, "size mismatch: header says %d, got %d", size, n)
	}
	if err != nil {
		return nil, "", s.toStatus(err)
	}
	extra, err := r.Read(make([]byte, 1))
	if extra > 0 {
		return nil, "", status.Errorf(codes.InvalidArgument, "size mismatch: header says %d, got more", size)
	}
	if !errors.Is(err, io.EOF) {
		return nil, "", s.toStatus(err)
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:]), nil
}

func (s *Server) writeToNodes(ctx context.Context, id string, data []byte, nodes []*nodepool.Node) ([]*upload, []string) {
	uploads := make([]*upload, 0, len(nodes))
	var failed []string
	var wg sync.WaitGroup
	for _, node := range nodes {
		client, err := s.transport.Client(node)
		if err != nil {
			s.log.Error("could not open client", slog.String("error", err.Error()), slog.String("node_id", node.ID), slog.String("blob_id", id))
			failed = append(failed, node.ID)
			continue
		}
		u := &upload{node: node}
		uploads = append(uploads, u)
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(ctx, s.cfg.WriteTimeout)
			defer cancel()
			u.sha, u.err = nodeio.Write(ctx, client, id, int64(len(data)), bytes.NewReader(data))
		})
	}
	wg.Wait()
	return uploads, failed
}

func (s *Server) settle(id, sha string, size int64, uploads []*upload) (committed, failed []string) {
	for _, u := range uploads {
		switch {
		case u.err == nil && u.sha == sha:
			u.node.DeleteFreeSpace(size)
			committed = append(committed, u.node.ID)
		case u.err == nil:
			s.log.Error("sha256 mismatch", slog.String("node_id", u.node.ID), slog.String("blob_id", id), slog.String("node_sha256", u.sha), slog.String("gateway_sha256", sha))
		case isUnknownOutcome(u.err):
			s.log.Warn("unknown write outcome", slog.String("error", u.err.Error()), slog.String("node_id", u.node.ID), slog.String("blob_id", id))
		default:
			s.log.Warn("node could not write file", slog.String("error", u.err.Error()), slog.String("node_id", u.node.ID), slog.String("blob_id", id))
			failed = append(failed, u.node.ID)
		}
	}
	return committed, failed
}

func isUnknownOutcome(err error) bool {
	switch status.Code(err) {
	case codes.DeadlineExceeded, codes.Canceled:
		return true
	}
	return false
}

func (s *Server) dropReplicas(ctx context.Context, id string, nodeIDs []string) {
	if len(nodeIDs) == 0 {
		return
	}
	ctx, cancel := metadataContext(ctx)
	defer cancel()
	if err := s.store.DropPendingReplicas(ctx, id, nodeIDs); err != nil {
		s.log.Error("could not delete replicas", slog.String("error", err.Error()), slog.Any("node_ids", nodeIDs), slog.String("blob_id", id))
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
