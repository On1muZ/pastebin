package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"pastebin/gateway/internal/balancer"
	"pastebin/gateway/internal/metadata"
	"pastebin/gateway/internal/nodeio"
	"pastebin/gateway/internal/nodepool"
	"pastebin/gateway/internal/transport"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	storagenodev1.UnimplementedStorageNodeServer
	log       *slog.Logger
	balancer  *balancer.Balancer
	pool      *nodepool.Pool
	transport *transport.Transport
	store     metadata.Store
}

const chunkSize = 256 << 10

func New(log *slog.Logger, balancer *balancer.Balancer, pool *nodepool.Pool, transport *transport.Transport, store metadata.Store) *Server {
	return &Server{
		log:       log,
		balancer:  balancer,
		pool:      pool,
		transport: transport,
		store:     store,
	}
}

func (s *Server) GetBlob(req *storagenodev1.GetBlobRequest, stream storagenodev1.StorageNode_GetBlobServer) error {
	ctx := stream.Context()
	if req.GetBlobId() == "" || req.GetLength() < 0 || req.GetOffset() < 0 {
		return status.Error(codes.InvalidArgument, "blob_id is required, offset and length must be >= 0")
	}

	blob, err := s.store.GetBlob(ctx, req.GetBlobId())
	if err != nil {
		return s.toStatus(err)
	}
	if blob.Status != metadata.Committed {
		return status.Error(codes.NotFound, "blob not found")
	}
	r, err := s.openReplica(ctx, req)
	if err != nil {
		return err
	}
	defer r.Close()
	_, err = io.CopyBuffer(&chunkWriter{stream: stream}, r, make([]byte, chunkSize))
	if err != nil {
		return s.toStatus(err)
	}
	return nil
}

func (s *Server) DeleteBlob(ctx context.Context, req *storagenodev1.DeleteBlobRequest) (*storagenodev1.DeleteBlobResponse, error) {
	if req.GetBlobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "blob id must be lenght of 32")
	}
	err := s.store.MarkDeleting(ctx, req.GetBlobId())
	if err != nil {
		return nil, s.toStatus(err)
	}
	return &storagenodev1.DeleteBlobResponse{}, nil
}

func (s *Server) toStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, metadata.ErrNotFound):
		return status.Error(codes.NotFound, "blob not found")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, metadata.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, "blob already exists")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	s.log.Error("internal error", slog.String("err", err.Error()))
	return status.Error(codes.Internal, "internal error")
}

func (s *Server) openReplica(ctx context.Context, req *storagenodev1.GetBlobRequest) (io.ReadCloser, error) {
	replicas, err := s.store.GetReplicas(ctx, req.GetBlobId())
	if err != nil {
		return nil, s.toStatus(err)
	}
	replicas = slices.DeleteFunc(replicas, func(r metadata.ReplicaInfo) bool {
		return r.Status != metadata.Committed
	})
	slices.SortStableFunc(replicas, func(a, b metadata.ReplicaInfo) int {
		return s.aliveRank(a.NodeID) - s.aliveRank(b.NodeID)
	})
	var lastErr error = errors.New("no committed replicas")
	for _, replica := range replicas {
		node, ok := s.pool.GetNodeByID(replica.NodeID)
		if !ok {
			lastErr = errors.New("node " + replica.NodeID + " is not in pool")
			continue
		}
		client, err := s.transport.Client(node)
		if err != nil {
			lastErr = err
			continue
		}
		r, err := nodeio.Read(ctx, client, req.GetBlobId(), req.GetOffset(), req.GetLength())
		if err != nil {
			if status.Code(err) == codes.OutOfRange || status.Code(err) == codes.InvalidArgument {
				return nil, err
			}
			s.log.Warn("replica unavailable, trying next", "blob_id", req.GetBlobId(), "node_id", node.ID, "err", err)
			lastErr = err
			continue
		}
		return r, nil
	}
	s.log.Error("no replica available", "blob_id", req.GetBlobId(), "err", lastErr)
	return nil, status.Errorf(codes.Unavailable, "no replica available: %v", lastErr)
}

func (s *Server) aliveRank(nodeID string) int {
	if node, ok := s.pool.GetNodeByID(nodeID); ok && node.IsAlive() {
		return 0
	}
	return 1
}

type chunkWriter struct {
	stream storagenodev1.StorageNode_GetBlobServer
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	if err := w.stream.Send(&storagenodev1.GetBlobResponse{Chunk: p}); err != nil {
		return 0, err
	}
	return len(p), nil
}
