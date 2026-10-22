package paste

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"log/slog"
	"pastebin/pastebin/internal/blobs"
	"pastebin/pastebin/internal/cache"
	"pastebin/pastebin/internal/metadata"
	"pastebin/pastebin/internal/views"
	"time"
)

var ErrIncorrectHash = errors.New("incorrect hash sum")
var ErrPasteIsEmpty = errors.New("paste is empty")
var ErrPasteSizeTooBig = errors.New("paste size too big")

type Paste struct {
	store        metadata.Store
	cache        cache.PasteCache
	counter      views.Counter
	client       blobs.Store
	log          *slog.Logger
	cacheTTL     time.Duration
	maxPasteSize int64
}

func New(store metadata.Store, cache cache.PasteCache, counter views.Counter, client blobs.Store, log *slog.Logger, cacheTTL time.Duration, maxPasteSize int64) *Paste {
	return &Paste{
		store:        store,
		cache:        cache,
		counter:      counter,
		client:       client,
		log:          log,
		cacheTTL:     cacheTTL,
		maxPasteSize: maxPasteSize,
	}
}

func (p *Paste) Create(ctx context.Context, paste []byte, deleteAfterView bool, deleteAt *time.Time) (*metadata.PasteInfo, error) {
	if len(paste) == 0 {
		return nil, ErrPasteIsEmpty
	}
	if int64(len(paste)) > p.maxPasteSize {
		return nil, ErrPasteSizeTooBig
	}
	sum := sha256.Sum256(paste)
	plainedHash := hex.EncodeToString(sum[:])
	id := genID()
	gotHash, err := p.client.Put(ctx, id, paste)
	if err != nil {
		p.dropBlob(ctx, id)
		return nil, err
	}
	if plainedHash != gotHash {
		p.dropBlob(ctx, id)
		return nil, ErrIncorrectHash
	}
	var pasteInfo metadata.PasteInfo
	for range 3 {
		addr := genAddr()
		pasteInfo, err = p.store.CreatePaste(ctx, metadata.CreatePaste{
			ID:              id,
			Addr:            addr,
			DeleteAfterView: deleteAfterView,
			DeleteAt:        deleteAt,
			Size:            int64(len(paste)),
			Hash:            gotHash,
		})
		if !errors.Is(err, metadata.ErrAlreadyExists) {
			break
		}
	}
	if err != nil {
		p.dropBlob(ctx, id)
		return nil, err
	}
	return &pasteInfo, nil
}

func (p *Paste) View(ctx context.Context, addr string) (*metadata.PasteInfo, []byte, error) {
	pasteInfo, err := p.store.GetPasteByAddr(ctx, addr)
	if err != nil {
		return nil, nil, err
	}
	if pasteInfo.DeleteAfterView {
		pasteBytes, err := p.client.Get(ctx, pasteInfo.ID)
		if err != nil {
			return nil, nil, err
		}
		pasteInfo, err = p.store.ConsumePasteByAddr(ctx, addr)
		if err != nil {
			return nil, nil, err
		}
		return &pasteInfo, pasteBytes, nil
	}
	pasteBytes, ok, err := p.cache.Get(ctx, pasteInfo.ID)
	if err != nil {
		p.log.Error("could not get paste from cache", slog.String("paste_id", pasteInfo.ID), slog.String("error", err.Error()))
	}
	if !ok {
		pasteBytes, err = p.client.Get(ctx, pasteInfo.ID)
		if err != nil {
			return nil, nil, err
		}
		ttl := p.cacheTTL
		if pasteInfo.DeleteAt != nil {
			ttl = min(ttl, time.Until(*pasteInfo.DeleteAt))
		}
		if ttl > 0 {
			if err := p.cache.Set(ctx, pasteInfo.ID, pasteBytes, ttl); err != nil {
				p.log.Error("could not set paste in cache", slog.String("paste_id", pasteInfo.ID), slog.String("error", err.Error()))
			}
		}
	}
	if err := p.counter.Inc(ctx, pasteInfo.ID); err != nil {
		p.log.Error("could not inc paste views in counter", slog.String("paste_id", pasteInfo.ID), slog.String("error", err.Error()))
	}
	return &pasteInfo, pasteBytes, nil
}

func genID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func genAddr() string {
	b := make([]byte, 6)
	rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func (p *Paste) dropBlob(ctx context.Context, id string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	_ = p.client.Delete(ctx, id)
}
