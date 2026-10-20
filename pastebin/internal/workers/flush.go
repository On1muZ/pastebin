package workers

import (
	"context"
	"log/slog"
	"pastebin/pastebin/internal/metadata"
	"pastebin/pastebin/internal/views"
	"time"
)

type FlushWorker struct {
	store                   metadata.Store
	viewsCounter            views.Counter
	log                     *slog.Logger
	gracefulShutdownTimeout time.Duration
	flushOnceTimeout        time.Duration
}

func NewFlushWorker(store metadata.Store, viewsCounter views.Counter, log *slog.Logger, flushOnceTimeout, gracefulShutdownTimeout time.Duration) *FlushWorker {
	return &FlushWorker{
		store:                   store,
		viewsCounter:            viewsCounter,
		log:                     log,
		gracefulShutdownTimeout: gracefulShutdownTimeout,
		flushOnceTimeout:        flushOnceTimeout,
	}
}

func (f *FlushWorker) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), f.gracefulShutdownTimeout)
			f.FlushOnce(fctx)
			cancel()
			return
		case <-ticker.C:
			fctx, cancel := context.WithTimeout(ctx, f.flushOnceTimeout)
			f.FlushOnce(fctx)
			cancel()
		}
	}
}

func (f *FlushWorker) FlushOnce(ctx context.Context) {
	viewsInfo, err := f.viewsCounter.Drain(ctx)
	if err != nil {
		f.log.Error("flush worker: could not drain views from counter", slog.String("error", err.Error()))
		return
	}
	if len(viewsInfo) == 0 {
		return
	}
	err = f.store.IncViews(ctx, viewsInfo)
	if err != nil {
		f.log.Error("flush worker: could not inc views in store", slog.String("error", err.Error()))
		return
	}
	err = f.viewsCounter.Ack(ctx)
	if err != nil {
		f.log.Error("flush worker: could not commit to counter that views has been pushed to store", slog.String("error", err.Error()))
	}
}
