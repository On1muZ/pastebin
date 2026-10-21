package blobs

import (
	"context"
	"errors"
	"io"
	storagenodev1 "pastebin/gen/go/storagenode/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const chunkSize = 256 << 10

type Gateway struct {
	client storagenodev1.StorageNodeClient
}

func NewGateway(client storagenodev1.StorageNodeClient) Store {
	return &Gateway{client: client}
}

func (g *Gateway) Get(ctx context.Context, id string) ([]byte, error) {
	stream, err := g.client.GetBlob(ctx, &storagenodev1.GetBlobRequest{BlobId: id})
	if err != nil {
		return nil, toError(err)
	}
	var rawPaste []byte
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return rawPaste, nil
		}
		if err != nil {
			return nil, toError(err)
		}
		rawPaste = append(rawPaste, msg.GetChunk()...)
	}
}

func (g *Gateway) Put(ctx context.Context, id string, data []byte) (string, error) {
	stream, err := g.client.PutBlob(ctx)
	if err != nil {
		return "", toError(err)
	}
	err = stream.Send(&storagenodev1.PutBlobRequest{
		Payload: &storagenodev1.PutBlobRequest_Header{
			Header: &storagenodev1.BlobHeader{
				BlobId:    id,
				SizeBytes: int64(len(data)),
			},
		},
	})
	if err != nil && !errors.Is(err, io.EOF) {
		return "", toError(err)
	}
	for len(data) > 0 && err == nil {
		n := min(chunkSize, len(data))
		err = stream.Send(&storagenodev1.PutBlobRequest{
			Payload: &storagenodev1.PutBlobRequest_Chunk{
				Chunk: data[:n],
			},
		})
		data = data[n:]
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return "", toError(err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", toError(err)
	}
	return resp.GetSha256(), nil
}

func (g *Gateway) Delete(ctx context.Context, id string) error {
	_, err := g.client.DeleteBlob(ctx, &storagenodev1.DeleteBlobRequest{BlobId: id})
	return toError(err)
}

func toError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.NotFound {
		return ErrNotFound
	}
	return err
}
