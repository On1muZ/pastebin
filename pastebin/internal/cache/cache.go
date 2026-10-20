package cache

import (
	"context"
	"time"
)

type PasteCache interface {
	Get(ctx context.Context, pasteID string) ([]byte, bool, error)
	Set(ctx context.Context, pasteID string, data []byte, ttl time.Duration) error
	Del(ctx context.Context, pasteID string) error
}
