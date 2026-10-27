package blob

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

const testID = "0123456789abcdef0123456789abcdef"

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPutThenOpen(t *testing.T) {
	s := newTestStore(t)
	data := []byte("hello, blob")
	res, err := s.Put(testID, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if res.Size != int64(len(data)) {
		t.Errorf("size = %d, want %d", res.Size, len(data))
	}
	f, err := s.Open(testID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	got, _ := io.ReadAll(f)
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestPutTwiseFalis(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Put(testID, bytes.NewReader([]byte("a"))); err != nil {
		t.Fatal(err)
	}
	_, err := s.Put(testID, bytes.NewReader([]byte("b")))
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("err = %v, want ErrAlreadyExists", err)
	}
}

func TestInvalidID(t *testing.T) {
	s := newTestStore(t)
	for _, id := range []string{"", "../../etc/passwd", "ABCDEF0123456789ABCDEF0123456789"} {
		if _, err := s.Put(id, bytes.NewReader(nil)); !errors.Is(err, ErrInvalidID) {
			t.Errorf("Put(%q): err = %v, want ErrInvalidID", id, err)
		}
	}
}

func TestOpenMissing(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Open(testID); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestDeleteIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Put(testID, bytes.NewReader([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(testID); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(testID); err != nil {
		t.Errorf("second Delete: %v", err)
	}
}

func TestFailedPutLeavesNothing(t *testing.T) {
	s := newTestStore(t)
	r := io.MultiReader(bytes.NewReader([]byte("part")), errReader{})

	if _, err := s.Put(testID, r); err == nil {
		t.Fatal("expected error")
	}
	if _, err := s.Open(testID); !errors.Is(err, ErrNotFound) {
		t.Errorf("blob exists after failed Put")
	}
	entries, _ := os.ReadDir(filepath.Join(s.root, "tmp"))
	if len(entries) != 0 {
		t.Errorf("tmp not cleaned: %d files left", len(entries))
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("connection lost") }
