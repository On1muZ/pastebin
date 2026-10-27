package metadata

import (
	"context"
	"errors"
	"pastebin/gateway/internal/metadata/db"

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

func (p *Postgres) CreateBlob(ctx context.Context, id string, size int64) error {
	return createBlob(ctx, p.q, id, size)
}

func createBlob(ctx context.Context, q *db.Queries, id string, size int64) error {
	n, err := q.CreateBlob(ctx, db.CreateBlobParams{ID: id, Size: size})
	if err != nil {
		return toError(err)
	}
	if n > 0 {
		return nil
	}
	blob, err := q.GetBlob(ctx, id)
	if err != nil {
		return toError(err)
	}
	if blob.Status == string(Deleting) {
		return ErrConflict
	}
	if blob.Size != size {
		return ErrAlreadyExists
	}
	return nil
}

func (p *Postgres) GetBlob(ctx context.Context, id string) (BlobInfo, error) {
	blob, err := p.q.GetBlob(ctx, id)
	if err != nil {
		return BlobInfo{}, toError(err)
	}
	return toBlobInfo(blob), nil
}

func (p *Postgres) AddReplica(ctx context.Context, blobID, nodeID string) error {
	n, err := p.q.AddReplica(ctx, db.AddReplicaParams{BlobID: blobID, NodeID: nodeID})
	if err != nil {
		return toError(err)
	}
	if n > 0 {
		return nil
	}
	blob, err := p.q.GetBlob(ctx, blobID)
	if err != nil {
		return toError(err)
	}
	if blob.Status == string(Deleting) {
		return ErrConflict
	}
	return nil
}

func (p *Postgres) MarkReplicaCommitted(ctx context.Context, blobID, nodeID string) error {
	n, err := p.q.CommitReplica(ctx, db.CommitReplicaParams{BlobID: blobID, NodeID: nodeID})
	if err != nil {
		return toError(err)
	}
	if n > 0 {
		return nil
	}
	replica, err := p.q.GetReplica(ctx, db.GetReplicaParams{BlobID: blobID, NodeID: nodeID})
	if err != nil {
		return toError(err)
	}
	if replica.Status == string(Committed) {
		return nil
	}
	return ErrConflict
}

func (p *Postgres) CommitBlob(ctx context.Context, blobID, hash string) error {
	return commitBlob(ctx, p.q, blobID, hash)
}

func commitBlob(ctx context.Context, q *db.Queries, blobID, hash string) error {
	n, err := q.CommitBlob(ctx, db.CommitBlobParams{Hash: &hash, ID: blobID})
	if err != nil {
		return toError(err)
	}
	if n > 0 {
		return nil
	}
	blob, err := q.GetBlob(ctx, blobID)
	if err != nil {
		return toError(err)
	}
	if blob.Status == string(Committed) && blob.Hash != nil && *blob.Hash == hash {
		return nil
	}
	return ErrConflict
}

func (p *Postgres) GetReplicas(ctx context.Context, blobID string) ([]ReplicaInfo, error) {
	replicas, err := p.q.GetReplicas(ctx, blobID)
	if err != nil {
		return nil, toError(err)
	}
	replicasInfo := make([]ReplicaInfo, 0, len(replicas))
	for _, r := range replicas {
		replicasInfo = append(replicasInfo, toReplicaInfo(r))
	}
	return replicasInfo, nil
}

func (p *Postgres) MarkDeleting(ctx context.Context, blobID string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return toError(err)
	}
	defer tx.Rollback(ctx)
	qtx := p.q.WithTx(tx)
	n, err := qtx.MarkBlobDeleting(ctx, blobID)
	if err != nil {
		return toError(err)
	}
	if n == 0 {
		return ErrNotFound
	}
	if err := qtx.MarkReplicasDeleting(ctx, blobID); err != nil {
		return toError(err)
	}
	return toError(tx.Commit(ctx))
}

func (p *Postgres) DeleteReplica(ctx context.Context, blobID, nodeID string) error {
	n, err := p.q.DeleteReplica(ctx, db.DeleteReplicaParams{BlobID: blobID, NodeID: nodeID})
	if err != nil {
		return toError(err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (p *Postgres) DeleteBlob(ctx context.Context, blobID string) error {
	n, err := p.q.DeleteBlob(ctx, blobID)
	if err != nil {
		return toError(err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (p *Postgres) ListDeleting(ctx context.Context, limit int) ([]BlobInfo, error) {
	blobs, err := p.q.ListDeleting(ctx, int32(limit))
	if err != nil {
		return nil, toError(err)
	}
	blobsInfo := make([]BlobInfo, 0, len(blobs))
	for _, b := range blobs {
		blobsInfo = append(blobsInfo, toBlobInfo(b))
	}
	return blobsInfo, nil
}

func (p *Postgres) ListUnderReplicated(ctx context.Context, limit, want int) ([]BlobInfo, error) {
	blobs, err := p.q.ListUnderReplicated(ctx, db.ListUnderReplicatedParams{
		Want: int32(want),
		Lim:  int32(limit),
	})
	if err != nil {
		return nil, toError(err)
	}
	blobsInfo := make([]BlobInfo, 0, len(blobs))
	for _, b := range blobs {
		blobsInfo = append(blobsInfo, toBlobInfo(b))
	}
	return blobsInfo, nil
}

func (p *Postgres) SyncNodes(ctx context.Context, nodes []*NodeInfo) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return toError(err)
	}
	defer tx.Rollback(ctx)
	qtx := p.q.WithTx(tx)
	for _, n := range nodes {
		if err := qtx.UpsertNode(ctx, db.UpsertNodeParams{ID: n.ID, Addr: n.Addr}); err != nil {
			return toError(err)
		}
	}
	return toError(tx.Commit(ctx))
}

func (p *Postgres) StartUpload(ctx context.Context, id string, size int64, nodeIDs []string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return toError(err)
	}
	defer tx.Rollback(ctx)
	qtx := p.q.WithTx(tx)
	if err := qtx.SetLocalAsyncCommit(ctx); err != nil {
		return toError(err)
	}
	if err := createBlob(ctx, qtx, id, size); err != nil {
		return err
	}
	n, err := qtx.AddReplicas(ctx, db.AddReplicasParams{BlobID: id, NodeIds: nodeIDs})
	if err != nil {
		return toError(err)
	}
	if n == 0 && len(nodeIDs) > 0 {
		blob, err := qtx.GetBlob(ctx, id)
		if err != nil {
			return toError(err)
		}
		if blob.Status == string(Deleting) {
			return ErrConflict
		}
	}
	return toError(tx.Commit(ctx))
}

func (p *Postgres) FinishUpload(ctx context.Context, id, hash string, committed, failed []string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return toError(err)
	}
	defer tx.Rollback(ctx)
	qtx := p.q.WithTx(tx)
	if len(failed) > 0 {
		if err := qtx.DeletePendingReplicas(ctx, db.DeletePendingReplicasParams{BlobID: id, NodeIds: failed}); err != nil {
			return toError(err)
		}
	}
	if err := qtx.CommitReplicas(ctx, db.CommitReplicasParams{BlobID: id, NodeIds: committed}); err != nil {
		return toError(err)
	}
	if err := commitBlob(ctx, qtx, id, hash); err != nil {
		return err
	}
	return toError(tx.Commit(ctx))
}

func (p *Postgres) DropPendingReplicas(ctx context.Context, blobID string, nodeIDs []string) error {
	return toError(p.q.DeletePendingReplicas(ctx, db.DeletePendingReplicasParams{BlobID: blobID, NodeIds: nodeIDs}))
}

func (p *Postgres) GetCommittedReplicas(ctx context.Context, blobID string) ([]ReplicaInfo, error) {
	rows, err := p.q.GetCommittedReplicas(ctx, blobID)
	if err != nil {
		return nil, toError(err)
	}
	if len(rows) == 0 {
		blob, err := p.q.GetBlob(ctx, blobID)
		if err != nil {
			return nil, toError(err)
		}
		if blob.Status != string(Committed) {
			return nil, ErrNotFound
		}
	}
	replicasInfo := make([]ReplicaInfo, 0, len(rows))
	for _, r := range rows {
		replicasInfo = append(replicasInfo, toReplicaInfo(r.Replica))
	}
	return replicasInfo, nil
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

func toBlobInfo(b db.Blob) BlobInfo {
	return BlobInfo{
		ID:        b.ID,
		Size:      b.Size,
		Hash:      b.Hash,
		Status:    Status(b.Status),
		CreatedAt: b.CreatedAt,
		UpdatedAt: b.UpdatedAt,
	}
}

func toReplicaInfo(r db.Replica) ReplicaInfo {
	return ReplicaInfo{
		BlobID:    r.BlobID,
		NodeID:    r.NodeID,
		Status:    Status(r.Status),
		CreatedAt: r.CreatedAt,
		UpdatedAt: r.UpdatedAt,
	}
}
