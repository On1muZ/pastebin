package workers

import (
	"context"
	"log/slog"
	"pastebin/gateway/internal/metadata"
	"pastebin/gateway/internal/nodepool"
	"pastebin/gateway/internal/transport"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"sync"
	"time"
)

const (
	WorkerPoolLimit     = 50
	TimeoutToDeleteBlob = 5 * time.Second
)

type DeleteWorker struct {
	interval  time.Duration
	log       *slog.Logger
	store     metadata.Store
	transport *transport.Transport
	pool      *nodepool.Pool
}

func NewDeleteWorker(
	interval time.Duration,
	log *slog.Logger,
	store metadata.Store,
	transport *transport.Transport,
	pool *nodepool.Pool,
) *DeleteWorker {
	return &DeleteWorker{
		interval:  interval,
		log:       log,
		store:     store,
		transport: transport,
		pool:      pool,
	}
}

func (d *DeleteWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			d.log.Info("stopping delete worker cause of ctx is done")
			return
		case <-ticker.C:
			d.DeleteOnce(ctx)
		}
	}
}

func (d *DeleteWorker) DeleteOnce(ctx context.Context) {
	blobsToDelete, err := d.store.ListDeleting(ctx, 50)
	jobs := make(chan string)
	if err != nil {
		d.log.Error("delete worker could not get blobs to delete", slog.String("error", err.Error()))
		return
	}
	go func() {
		defer close(jobs)
		for _, b := range blobsToDelete {
			jobs <- b.ID
		}
	}()
	var wg sync.WaitGroup
	for range WorkerPoolLimit {
		wg.Go(func() {
			for blobID := range jobs {
				timeoutCtx, cancel := context.WithTimeout(ctx, TimeoutToDeleteBlob)
				d.deleteOneBlob(timeoutCtx, blobID)
				cancel()
			}
		})
	}
	wg.Wait()
}

func (d *DeleteWorker) deleteOneBlob(ctx context.Context, blobID string) {
	replicas, err := d.store.GetReplicas(ctx, blobID)
	wantToDelete := len(replicas)
	if err != nil {
		d.log.Error("delete worker could not get replicas of node", slog.String("error", err.Error()))
		return
	}
	for _, r := range replicas {
		node, ok := d.pool.GetNodeByID(r.NodeID)
		if !ok {
			d.log.Error("delete worker could not get node from pool", slog.String("node_id", r.NodeID))
			continue
		}
		client, err := d.transport.Client(node)
		if err != nil {
			d.log.Error("delete worker could not get the client of node", slog.String("node_id", r.NodeID), slog.String("error", err.Error()))
			continue
		}
		if _, err := client.DeleteBlob(ctx, &storagenodev1.DeleteBlobRequest{
			BlobId: blobID,
		}); err != nil {
			d.log.Error("delete worker could not delete replica from node", slog.String("error", err.Error()), slog.String("node_id", r.NodeID), slog.String("blob_id", blobID))
			continue
		}

		if err := d.store.DeleteReplica(ctx, blobID, r.NodeID); err != nil {
			d.log.Error("delete worker: replica was deleted from node, but there is the error while deleting it from store", slog.String("error", err.Error()), slog.String("blob_id", blobID), slog.String("node_id", r.NodeID))
			continue
		}
		wantToDelete--
	}
	if wantToDelete != 0 {
		d.log.Error("delete worker could not delete all replicas", slog.String("blob_id", blobID))
		return
	} else {
		if err := d.store.DeleteBlob(ctx, blobID); err != nil {
			d.log.Error("delete worker deleted all replicas of node, but could not delete it from store", slog.String("error", err.Error()))
		}
	}
}
