package blob

import (
	"fmt"
	"syscall"
)

type Stats struct {
	TotalBytes int64
	FreeBytes  int64
}

func (s *Store) Stats() (Stats, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(s.root, &st); err != nil {
		return Stats{}, fmt.Errorf("statfs: %w", err)
	}
	return Stats{
		TotalBytes: int64(st.Blocks) * int64(st.Bsize),
		FreeBytes:  int64(st.Bavail) * int64(st.Bsize),
	}, nil
}
