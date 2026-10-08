package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"pastebin/storagenode/internal/blob"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const chunkSize = 256 << 10

var errUnexpectedMessage = errors.New("expected chunk")

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
			return 0, errUnexpectedMessage
		}
		r.buf = chunk.Chunk
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

type Server struct {
	storagenodev1.UnimplementedStorageNodeServer
	store *blob.Store
	log   *slog.Logger
}

func New(store *blob.Store, log *slog.Logger) *Server {
	return &Server{
		store: store,
		log:   log,
	}
}

func (s *Server) PutBlob(stream storagenodev1.StorageNode_PutBlobServer) error {
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "empty stream: header expected")
	}
	if err != nil {
		return err
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return status.Error(codes.InvalidArgument, "first message must be header")
	}
	if hdr.GetSizeBytes() < 0 {
		return status.Error(codes.InvalidArgument, "size_bytes must be > 0")
	}
	r := io.LimitReader(&chunkReader{stream: stream}, hdr.GetSizeBytes()+1)
	res, err := s.store.Put(hdr.GetBlobId(), r)
	if err != nil {
		return s.toStatus(stream.Context(), err)
	}
	if res.Size != hdr.GetSizeBytes() {
		if err := s.store.Delete(hdr.GetBlobId()); err != nil {
			s.log.Warn("delete blob after size mismatch", "blob_id", hdr.GetBlobId(), "err", err)
		}
		return status.Errorf(codes.InvalidArgument,
			"size mismatch: header says %d, got %d", hdr.GetSizeBytes(), res.Size)
	}
	return stream.SendAndClose(&storagenodev1.PutBlobResponse{
		BlobId:    hdr.GetBlobId(),
		SizeBytes: res.Size,
		Sha256:    res.SHA256,
	})
}

func (s *Server) GetBlob(req *storagenodev1.GetBlobRequest, stream storagenodev1.StorageNode_GetBlobServer) error {
	if req.GetOffset() < 0 || req.GetLength() < 0 {
		return status.Error(codes.InvalidArgument, "offset and length must be >= 0")
	}
	f, err := s.store.Open(req.GetBlobId())
	if err != nil {
		return s.toStatus(stream.Context(), err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return s.toStatus(stream.Context(), err)
	}
	size, offset := info.Size(), req.GetOffset()
	if offset > size {
		return status.Errorf(codes.OutOfRange, "offset %d is beyond blob size %d", offset, size)
	}
	length := size - offset
	if l := req.GetLength(); l > 0 && l < length {
		length = l
	}
	r := io.NewSectionReader(f, offset, length)

	for {
		buf := make([]byte, chunkSize)
		n, err := r.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&storagenodev1.GetBlobResponse{Chunk: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return s.toStatus(stream.Context(), err)
		}
	}
}

func (s *Server) DeleteBlob(ctx context.Context, req *storagenodev1.DeleteBlobRequest) (*storagenodev1.DeleteBlobResponse, error) {
	if err := s.store.Delete(req.GetBlobId()); err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &storagenodev1.DeleteBlobResponse{}, nil
}

func (s *Server) GetStatus(ctx context.Context, _ *storagenodev1.GetStatusRequest) (*storagenodev1.GetStatusResponse, error) {
	st, err := s.store.Stats()
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return &storagenodev1.GetStatusResponse{
		TotalBytes: st.TotalBytes,
		FreeBytes:  st.FreeBytes,
		BlobCount:  st.BlobCount,
	}, nil
}

func (s *Server) toStatus(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, blob.ErrInvalidID):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, blob.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, blob.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, errUnexpectedMessage):
		return status.Error(codes.InvalidArgument, "every message after header must be a chunk")
	case ctx.Err() != nil:
		return status.FromContextError(ctx.Err()).Err()
	default:
		s.log.Error("internal error", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}
