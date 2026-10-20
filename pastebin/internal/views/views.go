package views

import "context"

type Counter interface {
	Inc(ctx context.Context, pasteID string) error
	Drain(ctx context.Context) (map[string]int64, error)
	Ack(ctx context.Context) error
}
