package workers

import (
	"context"
	"errors"
	"log/slog"
	"pastebin/pastebin/internal/blobs"
	"pastebin/pastebin/internal/metadata"
	"time"
)

const limitToSelect = 500

type DeleteWorker struct {
	store             metadata.Store
	blobs             blobs.Store
	deleteOnceTimeout time.Duration
	log               *slog.Logger
}

func NewDeleteWorker(store metadata.Store, blobsStore blobs.Store, deleteOnceTimeout time.Duration, log *slog.Logger) *DeleteWorker {
	return &DeleteWorker{
		store:             store,
		blobs:             blobsStore,
		deleteOnceTimeout: deleteOnceTimeout,
		log:               log,
	}
}

func (d *DeleteWorker) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dCtx, cancel := context.WithTimeout(ctx, d.deleteOnceTimeout)
			d.DeleteOnce(dCtx)
			cancel()
		}
	}
}

func (d *DeleteWorker) DeleteOnce(ctx context.Context) {
	pastesToDelete, err := d.store.GetPastesToDelete(ctx, limitToSelect)
	if err != nil {
		d.log.Error("delete worker: could not get pastes to delete from store", slog.String("error", err.Error()))
		return
	}
	for _, p := range pastesToDelete {
		if ctx.Err() != nil {
			return
		}
		err := d.blobs.Delete(ctx, p.ID)
		if err != nil && !errors.Is(err, blobs.ErrNotFound) {
			d.log.Error("delete worker: could not delete paste from gateway", slog.String("paste_id", p.ID), slog.String("error", err.Error()))
			continue
		}
		err = d.store.DeletePaste(ctx, p.ID)
		if err != nil && !errors.Is(err, metadata.ErrNotFound) {
			d.log.Error("delete worker: could not delete paste from store", slog.String("paste_id", p.ID), slog.String("error", err.Error()))
		}
	}
}
