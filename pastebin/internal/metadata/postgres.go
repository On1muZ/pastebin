package metadata

import (
	"context"
	"errors"
	"pastebin/pastebin/internal/metadata/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

func NewPostgres(pool *pgxpool.Pool) Store {
	return &Postgres{pool: pool, q: db.New(pool)}
}

func (p *Postgres) GetPasteByAddr(ctx context.Context, addr string) (PasteInfo, error) {
	paste, err := p.q.GetPasteByAddr(ctx, addr)
	if err != nil {
		return PasteInfo{}, toError(err)
	}
	return toPasteInfo(paste), nil
}

func (p *Postgres) ConsumePasteByAddr(ctx context.Context, addr string) (PasteInfo, error) {
	paste, err := p.q.ConsumePasteByAddr(ctx, addr)
	if err != nil {
		return PasteInfo{}, toError(err)
	}
	return toPasteInfo(paste), nil
}

func (p *Postgres) GetPastesToDelete(ctx context.Context, limit int) ([]PasteInfo, error) {
	pastes, err := p.q.GetPastesToDelete(ctx, int32(limit))
	if err != nil {
		return nil, toError(err)
	}
	pastesInfo := make([]PasteInfo, 0, len(pastes))
	for _, paste := range pastes {
		pastesInfo = append(pastesInfo, toPasteInfo(paste))
	}
	return pastesInfo, nil
}

func (p *Postgres) CreatePaste(ctx context.Context, paste CreatePaste) (PasteInfo, error) {
	stmt := db.CreatePasteParams{
		ID:              paste.ID,
		Addr:            paste.Addr,
		Hash:            paste.Hash,
		DeleteAt:        paste.DeleteAt,
		DeleteAfterView: paste.DeleteAfterView,
		Size:            paste.Size,
	}
	createdPaste, err := p.q.CreatePaste(ctx, stmt)
	if err != nil {
		return PasteInfo{}, toError(err)
	}
	return toPasteInfo(createdPaste), nil
}

func (p *Postgres) DeletePaste(ctx context.Context, pasteID string) error {
	_, err := p.q.DeletePaste(ctx, pasteID)
	return toError(err)
}

func (p *Postgres) IncViews(ctx context.Context, deltas map[string]int64) error {
	if len(deltas) == 0 {
		return nil
	}
	ids := make([]string, 0, len(deltas))
	d := make([]int64, 0, len(deltas))
	for id, delta := range deltas {
		ids = append(ids, id)
		d = append(d, delta)
	}
	stmt := db.IncViewsParams{
		Ids:    ids,
		Deltas: d,
	}
	_, err := p.q.IncViews(ctx, stmt)
	if err != nil {
		return toError(err)
	}
	return nil
}

func toPasteInfo(paste db.Paste) PasteInfo {
	return PasteInfo{
		ID:              paste.ID,
		Addr:            paste.Addr,
		Hash:            paste.Hash,
		Size:            paste.Size,
		DeleteAfterView: paste.DeleteAfterView,
		Views:           paste.Views,
		DeleteAt:        paste.DeleteAt,
		CreatedAt:       paste.CreatedAt,
		UpdatedAt:       paste.UpdatedAt,
	}
}

func toError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrNotFound
	case errors.As(err, &pgErr) && pgErr.Code == "23505":
		return ErrAlreadyExists
	case errors.As(err, &pgErr) && pgErr.Code == "23503":
		return ErrNotFound
	default:
		return err
	}
}
