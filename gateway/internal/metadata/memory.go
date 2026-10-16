package metadata

import (
	"context"
	"sync"
	"time"
)

type Memory struct {
	mu       sync.RWMutex
	Nodes    []*NodeInfo
	Blobs    []*BlobInfo
	Replicas []*ReplicaInfo
}

func NewMemory() Store {
	return &Memory{}
}

func (m *Memory) CreateBlob(ctx context.Context, id string, size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range m.Blobs {
		if b.ID != id {
			continue
		}
		if b.Status == Deleting {
			return ErrConflict
		}
		if b.Size != size {
			return ErrAlreadyExists
		}
		return nil
	}
	m.Blobs = append(m.Blobs, &BlobInfo{
		ID:        id,
		Size:      size,
		Status:    Pending,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	})
	return nil
}

func (m *Memory) GetBlob(ctx context.Context, id string) (BlobInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, b := range m.Blobs {
		if b.ID == id {
			return *b, nil
		}
	}
	return BlobInfo{}, ErrNotFound
}

func (m *Memory) AddReplica(ctx context.Context, blobID, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	candidateNode := false
	candidateBlob := false
	for _, b := range m.Blobs {
		if b.ID == blobID {
			if b.Status == Deleting {
				return ErrConflict
			}
			candidateBlob = true
			break
		}
	}
	for _, r := range m.Replicas {
		if blobID == r.BlobID && nodeID == r.NodeID {
			return nil
		}
	}
	for _, n := range m.Nodes {
		if n.ID == nodeID {
			candidateNode = true
		}
	}
	if candidateBlob && candidateNode {
		m.Replicas = append(m.Replicas, &ReplicaInfo{
			BlobID:    blobID,
			NodeID:    nodeID,
			Status:    Pending,
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		})
		return nil
	} else {
		return ErrNotFound
	}
}

func (m *Memory) MarkReplicaCommitted(ctx context.Context, blobID, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.Replicas {
		if r.BlobID != blobID || r.NodeID != nodeID {
			continue
		}
		switch r.Status {
		case Committed:
			return nil
		case Deleting:
			return ErrConflict
		}
		r.Status = Committed
		r.UpdatedAt = time.Now().UTC()
		return nil
	}
	return ErrNotFound
}

func (m *Memory) CommitBlob(ctx context.Context, blobID, hash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range m.Blobs {
		if blobID != b.ID {
			continue
		}
		switch b.Status {
		case Committed:
			if b.Hash != nil && *b.Hash == hash {
				return nil
			}
			return ErrConflict
		case Deleting:
			return ErrConflict
		}
		b.UpdatedAt = time.Now().UTC()
		b.Hash = &hash
		b.Status = Committed
		return nil
	}
	return ErrNotFound
}

func (m *Memory) GetReplicas(ctx context.Context, blobID string) ([]ReplicaInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	blobExists := false
	for _, b := range m.Blobs {
		if b.ID == blobID {
			blobExists = true
			break
		}
	}
	if !blobExists {
		return nil, ErrNotFound
	}
	replicas := make([]ReplicaInfo, 0)
	for _, r := range m.Replicas {
		if r.BlobID == blobID {
			replicas = append(replicas, *r)
		}
	}
	return replicas, nil
}

func (m *Memory) MarkDeleting(ctx context.Context, blobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	candidates := 0
	for _, b := range m.Blobs {
		if b.ID == blobID {
			b.Status = Deleting
			candidates++
			b.UpdatedAt = time.Now().UTC()
			break
		}
	}
	for _, r := range m.Replicas {
		if r.BlobID == blobID {
			r.Status = Deleting
			r.UpdatedAt = time.Now().UTC()
		}
	}
	if candidates == 0 {
		return ErrNotFound
	}
	return nil
}

func (m *Memory) DeleteReplica(ctx context.Context, blobID, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	candidates := 0
	for i := 0; i < len(m.Replicas); i++ {
		if m.Replicas[i].BlobID == blobID && m.Replicas[i].NodeID == nodeID {
			if i == 0 {
				m.Replicas = m.Replicas[1:]
			} else if i == len(m.Replicas)-1 {
				m.Replicas = m.Replicas[:len(m.Replicas)-1]
			} else {
				m.Replicas = append(m.Replicas[:i], m.Replicas[i+1:]...)
			}
			candidates++
		}
	}
	if candidates != 0 {
		return nil
	} else {
		return ErrNotFound
	}
}

func (m *Memory) DeleteBlob(ctx context.Context, blobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < len(m.Blobs); i++ {
		if m.Blobs[i].ID != blobID {
			continue
		}
		m.Blobs = append(m.Blobs[:i], m.Blobs[i+1:]...)
		kept := m.Replicas[:0]
		for _, r := range m.Replicas {
			if r.BlobID != blobID {
				kept = append(kept, r)
			}
		}
		m.Replicas = kept
		return nil
	}
	return ErrNotFound
}

func (m *Memory) ListDeleting(ctx context.Context, limit int) ([]BlobInfo, error) {
	deletingBlobs := make([]BlobInfo, 0)
	if limit == 0 {
		return deletingBlobs, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, b := range m.Blobs {
		if b.Status == Deleting {
			deletingBlobs = append(deletingBlobs, *b)
			limit--
		}
		if limit == 0 {
			break
		}
	}
	return deletingBlobs, nil
}

func (m *Memory) ListUnderReplicated(ctx context.Context, limit, want int) ([]BlobInfo, error) {
	blobs := make([]BlobInfo, 0)
	if limit == 0 {
		return blobs, nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, b := range m.Blobs {
		if b.Status != Committed {
			continue
		}
		countReplicas := 0
		for _, r := range m.Replicas {
			if r.BlobID == b.ID && r.Status == Committed {
				countReplicas++
			}
		}
		if countReplicas < want {
			blobs = append(blobs, *b)
			limit--
		}
		if limit == 0 {
			break
		}
	}
	return blobs, nil
}

func (m *Memory) SyncNodes(ctx context.Context, nodes []*NodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	nodesSet := make(map[string]struct{})
	for _, n := range m.Nodes {
		nodesSet[n.ID] = struct{}{}
	}
	for _, n := range nodes {
		_, ok := nodesSet[n.ID]
		if !ok {
			m.Nodes = append(m.Nodes, &NodeInfo{ID: n.ID, Addr: n.Addr})
		} else {
			for _, mn := range m.Nodes {
				if mn.ID == n.ID {
					mn.Addr = n.Addr
				}
			}
		}
	}
	return nil
}
