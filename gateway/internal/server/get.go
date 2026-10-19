package server

import (
	"context"
	"errors"
	"io"
	"pastebin/gateway/internal/metadata"
	"pastebin/gateway/internal/nodeio"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) GetBlob(req *storagenodev1.GetBlobRequest, stream storagenodev1.StorageNode_GetBlobServer) error {
	ctx := stream.Context()
	if req.GetBlobId() == "" || req.GetLength() < 0 || req.GetOffset() < 0 {
		return status.Error(codes.InvalidArgument, "blob_id is required, offset and length must be >= 0")
	}

	replicas, err := s.store.GetCommittedReplicas(ctx, req.GetBlobId())
	if err != nil {
		return s.toStatus(err)
	}
	r, err := s.openReplica(ctx, req, replicas)
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

func (s *Server) openReplica(ctx context.Context, req *storagenodev1.GetBlobRequest, replicas []metadata.ReplicaInfo) (io.ReadCloser, error) {
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
