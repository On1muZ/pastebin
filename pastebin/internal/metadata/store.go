package metadata

import (
	"context"
	"errors"
	"time"
)

var ErrAlreadyExists = errors.New("already exists")
var ErrNotFound = errors.New("not found")

type PasteInfo struct {
	ID              string
	Addr            string
	Hash            string
	Size            int64
	DeleteAfterView bool
	Views           int64
	DeleteAt        *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type CreatePaste struct {
	ID              string
	Addr            string
	DeleteAfterView bool
	DeleteAt        *time.Time
	Size            int64
	Hash            string
}

type Store interface {
	GetPasteByAddr(ctx context.Context, addr string) (PasteInfo, error)
	ConsumePasteByAddr(ctx context.Context, addr string) (PasteInfo, error)
	GetPastesToDelete(ctx context.Context, limit int) ([]PasteInfo, error)
	CreatePaste(ctx context.Context, paste CreatePaste) (PasteInfo, error)
	DeletePaste(ctx context.Context, pasteID string) error
	IncViews(ctx context.Context, deltas map[string]int64) error
}
