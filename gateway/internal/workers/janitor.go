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
	ReplicasToCleanLimit  = 500
	TimeoutToCleanReplica = 5 * time.Second
)

type JanitorWorker struct {
	interval  time.Duration
	stale     time.Duration
	log       *slog.Logger
	store     metadata.Store
	transport *transport.Transport
	pool      *nodepool.Pool
}

func NewJanitorWorker(
	interval time.Duration,
	stale time.Duration,
	log *slog.Logger,
	store metadata.Store,
	transport *transport.Transport,
	pool *nodepool.Pool,
) *JanitorWorker {
	return &JanitorWorker{
		interval:  interval,
		stale:     stale,
		log:       log,
		store:     store,
		transport: transport,
		pool:      pool,
	}
}

func (j *JanitorWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			j.log.Info("stopping janitor worker cause of ctx is done")
			return
		case <-ticker.C:
			j.CleanOnce(ctx)
		}
	}
}

func (j *JanitorWorker) CleanOnce(ctx context.Context) {
	marked, err := j.store.MarkStalePendingDeleting(ctx, j.stale)
	if err != nil {
		j.log.Error("janitor worker could not mark stale pending blobs as deleting", slog.String("error", err.Error()))
	} else if marked > 0 {
		j.log.Info("janitor worker marked stale pending blobs as deleting", slog.Int("count", marked))
	}

	replicas, err := j.store.ListStalePendingReplicas(ctx, ReplicasToCleanLimit, j.stale)
	if err != nil {
		j.log.Error("janitor worker could not get stale pending replicas", slog.String("error", err.Error()))
		return
	}
	jobs := make(chan metadata.ReplicaInfo)
	go func() {
		defer close(jobs)
		for _, r := range replicas {
			jobs <- r
		}
	}()
	var wg sync.WaitGroup
	for range WorkerPoolLimit {
		wg.Go(func() {
			for r := range jobs {
				timeoutCtx, cancel := context.WithTimeout(ctx, TimeoutToCleanReplica)
				j.cleanOneReplica(timeoutCtx, r)
				cancel()
			}
		})
	}
	wg.Wait()
}

func (j *JanitorWorker) cleanOneReplica(ctx context.Context, r metadata.ReplicaInfo) {
	node, ok := j.pool.GetNodeByID(r.NodeID)
	if !ok {
		j.log.Error("janitor worker could not get node from pool", slog.String("node_id", r.NodeID), slog.String("blob_id", r.BlobID))
		return
	}
	if !node.IsAlive() {
		j.log.Warn("janitor worker skipped stale replica on dead node", slog.String("node_id", r.NodeID), slog.String("blob_id", r.BlobID))
		return
	}
	client, err := j.transport.Client(node)
	if err != nil {
		j.log.Error("janitor worker could not get the client of node", slog.String("error", err.Error()), slog.String("node_id", r.NodeID))
		return
	}
	if _, err := client.DeleteBlob(ctx, &storagenodev1.DeleteBlobRequest{
		BlobId: r.BlobID,
	}); err != nil {
		j.log.Error("janitor worker could not delete stale replica from node", slog.String("error", err.Error()), slog.String("node_id", r.NodeID), slog.String("blob_id", r.BlobID))
		return
	}
	if err := j.store.DropPendingReplicas(ctx, r.BlobID, []string{r.NodeID}); err != nil {
		j.log.Error("janitor worker: stale replica was deleted from node, but there is the error while deleting it from store", slog.String("error", err.Error()), slog.String("blob_id", r.BlobID), slog.String("node_id", r.NodeID))
		return
	}
	j.log.Info("janitor worker cleaned stale pending replica", slog.String("blob_id", r.BlobID), slog.String("node_id", r.NodeID))
}
