package blob

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"syscall"
)

type Stats struct {
	TotalBytes int64
	FreeBytes  int64
	BlobCount  int64
}

func (s *Store) Stats() (Stats, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(s.root, &st); err != nil {
		return Stats{}, fmt.Errorf("statfs: %w", err)
	}
	var count int64
	err := filepath.WalkDir(filepath.Join(s.root, "blobs"), func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			count++
		}
		return nil
	})
	if err != nil {
		return Stats{}, fmt.Errorf("count blobs: %w", err)
	}

	return Stats{
		TotalBytes: int64(st.Blocks) * int64(st.Bsize),
		FreeBytes:  int64(st.Bavail) * int64(st.Bsize),
		BlobCount:  count,
	}, nil
}
