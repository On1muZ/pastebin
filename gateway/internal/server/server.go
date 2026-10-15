package server

import (
	"context"
	"errors"
	"log/slog"
	"pastebin/gateway/internal/balancer"
	"pastebin/gateway/internal/metadata"
	"pastebin/gateway/internal/nodepool"
	"pastebin/gateway/internal/transport"
	storagenodev1 "pastebin/gen/go/storagenode/v1"

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
