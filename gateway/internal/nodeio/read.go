package nodeio

import (
	"context"
	"io"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
)

type blobReader struct {
	buf    []byte
	cancel context.CancelFunc
	stream storagenodev1.StorageNode_GetBlobClient
}

func (r *blobReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		msg, err := r.stream.Recv()
		if err != nil {
			return 0, err
		}
		r.buf = msg.GetChunk()
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *blobReader) Close() error {
	r.cancel()
	return nil
}

func Read(ctx context.Context, client storagenodev1.StorageNodeClient, id string, offset, length int64) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(ctx)
	stream, err := client.GetBlob(ctx, &storagenodev1.GetBlobRequest{
		BlobId: id, Offset: offset, Length: length,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	first, err := stream.Recv()
	if err != nil && err != io.EOF {
		cancel()
		return nil, err
	}
	return &blobReader{
		stream: stream,
		buf:    first.GetChunk(),
		cancel: cancel,
	}, nil
}
