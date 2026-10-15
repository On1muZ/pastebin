package server

import (
	"context"

	storagenodev1 "pastebin/gen/go/storagenode/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
