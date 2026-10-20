package views

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const (
	viewsPending  = "views:pending"
	viewsFlushing = "views:flushing"
)

type Redis struct {
	client *redis.Client
}

func NewRedis(client *redis.Client) Counter {
	return &Redis{client: client}
}

func (r *Redis) Inc(ctx context.Context, pasteID string) error {
	return r.client.HIncrBy(ctx, viewsPending, pasteID, 1).Err()
}

func (r *Redis) Drain(ctx context.Context) (map[string]int64, error) {
	viewsInfo := make(map[string]int64)
	flushing, err := r.client.Exists(ctx, viewsFlushing).Result()
	if err != nil {
		return nil, err
	}
	if flushing == 0 {
		pending, err := r.client.Exists(ctx, viewsPending).Result()
		if err != nil {
			return nil, err
		}
		if pending == 0 {
			return viewsInfo, nil
		}
		if err := r.client.Rename(ctx, viewsPending, viewsFlushing).Err(); err != nil {
			return nil, err
		}
	}
	rawViews, err := r.client.HGetAll(ctx, viewsFlushing).Result()
	if err != nil {
		return nil, err
	}
	for k, v := range rawViews {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, err
		}
		viewsInfo[k] = n
	}
	return viewsInfo, nil
}

func (r *Redis) Ack(ctx context.Context) error {
	return r.client.Del(ctx, viewsFlushing).Err()
}
