package metadata

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("not found")
var ErrAlreadyExists = errors.New("already exists")

type Status string

const (
	Committed Status = "committed"
	Pending   Status = "pending"
	Deleting  Status = "deleting"
)

type NodeInfo struct {
	ID   string
	Addr string
}

type BlobInfo struct {
	ID        string
	Size      int64
	Hash      *string
	Status    Status
	CreatedAt time.Time
	UpdatedAt time.Time
}

type ReplicaInfo struct {
	BlobID    string
	NodeID    string
	Status    Status
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Store interface {
	CreateBlob(ctx context.Context, id string, size int64) error
	GetBlob(ctx context.Context, id string) (BlobInfo, error)
	AddReplica(ctx context.Context, blobID, nodeID string) error
	MarkReplicaCommitted(ctx context.Context, blobID, nodeID string) error
	CommitBlob(ctx context.Context, blobID, hash string) error
	GetReplicas(ctx context.Context, blobID string) ([]ReplicaInfo, error)
	MarkDeleting(ctx context.Context, blobID string) error
	DeleteReplica(ctx context.Context, blobID, nodeID string) error
	DeleteBlob(ctx context.Context, blobID string) error
	ListDeleting(ctx context.Context, limit int) ([]BlobInfo, error)
	ListUnderReplicated(ctx context.Context, limit, want int) ([]BlobInfo, error)
	SyncNodes(ctx context.Context, nodes []*NodeInfo) error
}
