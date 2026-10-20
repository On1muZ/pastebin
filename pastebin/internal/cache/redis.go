package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	client *redis.Client
}

func NewRedis(client *redis.Client) PasteCache {
	return &Redis{client: client}
}

func (r *Redis) Get(ctx context.Context, pasteID string) ([]byte, bool, error) {
	pasteBytes, err := r.client.Get(ctx, key(pasteID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return pasteBytes, true, nil
}

func (r *Redis) Set(ctx context.Context, pasteID string, data []byte, ttl time.Duration) error {
	return r.client.Set(ctx, key(pasteID), data, ttl).Err()
}

func (r *Redis) Del(ctx context.Context, pasteID string) error {
	return r.client.Del(ctx, key(pasteID)).Err()
}

func key(pasteID string) string {
	return "paste:" + pasteID
}
