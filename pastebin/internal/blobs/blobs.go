package blobs

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("blob not found")

type Store interface {
	Put(ctx context.Context, id string, data []byte) (string, error)
	Get(ctx context.Context, id string) ([]byte, error)
	Delete(ctx context.Context, id string) error
}
