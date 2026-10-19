package workers

import (
	"context"
	"log/slog"
	"pastebin/gateway/internal/balancer"
	"pastebin/gateway/internal/metadata"
	"pastebin/gateway/internal/nodeio"
	"pastebin/gateway/internal/nodepool"
	"pastebin/gateway/internal/transport"
	storagenodev1 "pastebin/gen/go/storagenode/v1"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const TimeoutToReplicateBlob = 10 * time.Second

type ReplicateWorker struct {
	pool      *nodepool.Pool
	transport *transport.Transport
	interval  time.Duration
	log       *slog.Logger
	store     metadata.Store
	balancer  *balancer.Balancer
}

func NewReplicateWorker(
	pool *nodepool.Pool,
	transport *transport.Transport,
	interval time.Duration,
	log *slog.Logger,
	store metadata.Store,
	balancer *balancer.Balancer,
) *ReplicateWorker {
	return &ReplicateWorker{
		pool:      pool,
		transport: transport,
		interval:  interval,
		log:       log,
		store:     store,
		balancer:  balancer,
	}
}

func (r *ReplicateWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.ReplicateOnce(ctx)
		}
	}
}

func (r *ReplicateWorker) ReplicateOnce(ctx context.Context) {
	w := len(r.pool.GetAllAliveNodes())
	if w == 0 {
		r.log.Error("replicate worker: no alive nodes")
		return
	}
	blobsToReplicate, err := r.store.ListUnderReplicated(ctx, 50, w)
	if err != nil {
		r.log.Error("replicate worker could not get blobs to replicate", slog.String("error", err.Error()))
		return
	}
	jobs := make(chan string)
	go func() {
		defer close(jobs)
		for _, b := range blobsToReplicate {
			jobs <- b.ID
		}
	}()
	var wg sync.WaitGroup
	for range WorkerPoolLimit {
		wg.Go(func() {
			for blobID := range jobs {
				r.replicateOneBlob(ctx, blobID)
			}
		})
	}
	wg.Wait()
}

func (r *ReplicateWorker) replicateOneBlob(ctx context.Context, blobID string) {
	blobInfo, err := r.store.GetBlob(ctx, blobID)
	if err != nil {
		r.log.Error("replicate worker could not get blob from store", slog.String("error", err.Error()))
		return
	}
	w := len(r.pool.GetAllAliveNodes())
	if w == 0 {
		r.log.Error("replicate worker: no alive nodes")
		return
	}
	currentReplicas, err := r.store.GetReplicas(ctx, blobID)
	if err != nil {
		r.log.Error("replicate worker could not get replicas of blob from store", slog.String("error", err.Error()), slog.String("blob_id", blobID))
		return
	}
	sources := make([]*nodepool.Node, 0, len(currentReplicas))
	exceptNode := make([]*nodepool.Node, 0, len(currentReplicas))
	committed := 0
	for _, cr := range currentReplicas {
		node, ok := r.pool.GetNodeByID(cr.NodeID)
		if !ok {
			r.log.Error("replicate worker could not get node from pool", slog.String("node_id", cr.NodeID))
			continue
		}
		exceptNode = append(exceptNode, node)
		if cr.Status != metadata.Committed {
			continue
		}
		committed++
		if node.IsAlive() {
			sources = append(sources, node)
		}
	}
	if len(sources) == 0 {
		r.log.Error("replicate worker could not get at least one node from pool to replicate from")
		return
	}
	wantToReplicate := w - committed
	for done := 0; done < wantToReplicate; {
		nodeToWrite, err := r.balancer.GetNode(blobInfo.Size, exceptNode)
		if err != nil {
			r.log.Error("replicate worker could not get node to write", slog.String("error", err.Error()))
			return
		}
		exceptNode = append(exceptNode, nodeToWrite)
		if r.copyToNode(ctx, blobInfo, sources, nodeToWrite) {
			done++
		}
	}
}

func (r *ReplicateWorker) copyToNode(ctx context.Context, blobInfo metadata.BlobInfo, sources []*nodepool.Node, nodeToWrite *nodepool.Node) bool {
	dstClient, err := r.transport.Client(nodeToWrite)
	if err != nil {
		r.log.Error("replicate worker could not get client of node", slog.String("error", err.Error()), slog.String("node_id", nodeToWrite.ID))
		return false
	}
	if err := r.store.AddReplica(ctx, blobInfo.ID, nodeToWrite.ID); err != nil {
		r.log.Error("replicate worker could not add replica to store", slog.String("error", err.Error()), slog.String("node_id", nodeToWrite.ID), slog.String("blob_id", blobInfo.ID))
		return false
	}
	var lastErr error
	for _, src := range sources {
		sha, err := r.copyFrom(ctx, blobInfo, src, dstClient)
		if err != nil {
			lastErr = err
			continue
		}
		if blobInfo.Hash == nil || sha != *blobInfo.Hash {
			r.log.Error("replicate worker got wrong sha256 after copy", slog.String("node_id", src.ID), slog.String("blob_id", blobInfo.ID), slog.String("sha256", sha))
			continue
		}
		nodeToWrite.DeleteFreeSpace(blobInfo.Size)
		if err := r.store.MarkReplicaCommitted(ctx, blobInfo.ID, nodeToWrite.ID); err != nil {
			r.log.Error("replicate worker could not commit replica in store", slog.String("error", err.Error()), slog.String("node_id", nodeToWrite.ID), slog.String("blob_id", blobInfo.ID))
			return false
		}
		return true
	}
	if status.Code(lastErr) == codes.DeadlineExceeded {
		return false
	}
	if err := r.store.DeleteReplica(ctx, blobInfo.ID, nodeToWrite.ID); err != nil {
		r.log.Error("replicate worker could not delete replica from store", slog.String("error", err.Error()), slog.String("node_id", nodeToWrite.ID), slog.String("blob_id", blobInfo.ID))
	}
	return false
}

func (r *ReplicateWorker) copyFrom(ctx context.Context, blobInfo metadata.BlobInfo, src *nodepool.Node, dstClient storagenodev1.StorageNodeClient) (string, error) {
	srcClient, err := r.transport.Client(src)
	if err != nil {
		r.log.Error("replicate worker could not get client of node", slog.String("error", err.Error()), slog.String("node_id", src.ID))
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, TimeoutToReplicateBlob)
	defer cancel()
	reader, err := nodeio.Read(ctx, srcClient, blobInfo.ID, 0, blobInfo.Size)
	if err != nil {
		r.log.Error("replicate worker could not open read stream from node", slog.String("error", err.Error()), slog.String("node_id", src.ID))
		return "", err
	}
	defer reader.Close()
	sha, err := nodeio.Write(ctx, dstClient, blobInfo.ID, blobInfo.Size, reader)
	if err != nil {
		r.log.Error("replicate worker could not write blob to node", slog.String("error", err.Error()), slog.String("node_id", src.ID), slog.String("blob_id", blobInfo.ID))
		return "", err
	}
	return sha, nil
}
