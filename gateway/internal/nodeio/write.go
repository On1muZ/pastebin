package nodeio

import (
	"context"
	"errors"
	"io"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
)

const chunkSize = 256 << 10

func Write(ctx context.Context, client storagenodev1.StorageNodeClient, id string, size int64, r io.Reader) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.PutBlob(ctx)
	if err != nil {
		return "", err
	}
	err = stream.Send(&storagenodev1.PutBlobRequest{
		Payload: &storagenodev1.PutBlobRequest_Header{
			Header: &storagenodev1.BlobHeader{BlobId: id, SizeBytes: size},
		},
	})
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	buf := make([]byte, chunkSize)
	for err == nil {
		n, readErr := r.Read(buf)
		if n > 0 {
			err = stream.Send(&storagenodev1.PutBlobRequest{
				Payload: &storagenodev1.PutBlobRequest_Chunk{Chunk: buf[:n]},
			})
			if err != nil && !errors.Is(err, io.EOF) {
				return "", err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", readErr
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", err
	}
	return resp.GetSha256(), nil
}
