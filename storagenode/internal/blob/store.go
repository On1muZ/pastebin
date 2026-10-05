package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

var (
	ErrAlreadyExists = errors.New("blob already exists")
	ErrNotFound      = errors.New("blob not found")
	ErrInvalidID     = errors.New("invalid blob id")
)

type Store struct {
	root string
}

func NewStore(root string) (*Store, error) {
	for _, dir := range []string{"blobs", "tmp"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			return nil, fmt.Errorf("create %s dir: %w", dir, err)
		}
	}
	return &Store{root: root}, nil
}

type PutResult struct {
	Size   int64
	SHA256 string
}

func (s *Store) Put(id string, r io.Reader) (PutResult, error) {
	if !validID(id) {
		return PutResult{}, ErrInvalidID
	}
	final := s.path(id)
	if _, err := os.Stat(final); err == nil {
		return PutResult{}, ErrAlreadyExists
	}
	tmp, err := os.CreateTemp(filepath.Join(s.root, "tmp"), id+"-*")
	if err != nil {
		return PutResult{}, fmt.Errorf("create tmp file: %w", err)
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()
	hasher := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, hasher), r)
	if err != nil {
		return PutResult{}, fmt.Errorf("write blob: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return PutResult{}, fmt.Errorf("sync blob: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return PutResult{}, fmt.Errorf("close blob: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return PutResult{}, fmt.Errorf("create blob dir: %w", err)
	}
	if err := os.Rename(tmp.Name(), final); err != nil {
		return PutResult{}, fmt.Errorf("rename blob: %w", err)
	}
	if err := syncDir(filepath.Dir(final)); err != nil {
		return PutResult{}, fmt.Errorf("sync blob dir: %w", err)
	}
	return PutResult{Size: n, SHA256: hex.EncodeToString(hasher.Sum(nil))}, nil
}

func (s *Store) Open(id string) (*os.File, error) {
	if !validID(id) {
		return nil, ErrInvalidID
	}
	f, err := os.Open(s.path(id))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return f, err
}

func (s *Store) Delete(id string) error {
	if !validID(id) {
		return ErrInvalidID
	}
	err := os.Remove(s.path(id))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *Store) path(id string) string {
	return filepath.Join(s.root, "blobs", id[:2], id[2:4], id)
}

func validID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
