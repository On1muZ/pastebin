package httpapi

import (
	"errors"
	"log/slog"
	"net/http"
	"pastebin/pastebin/internal/blobs"
	"pastebin/pastebin/internal/metadata"
	"pastebin/pastebin/internal/paste"
)

const (
	jsonEscapeFactor = 6
	jsonOverhead     = 1 << 10
)

type Handler struct {
	pastes      *paste.Paste
	log         *slog.Logger
	maxBodySize int64
}

func NewHandler(pastes *paste.Paste, log *slog.Logger, maxPasteSize int64) *Handler {
	return &Handler{
		pastes:      pastes,
		log:         log,
		maxBodySize: maxPasteSize*jsonEscapeFactor + jsonOverhead,
	}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/pastes", h.create)
	mux.HandleFunc("GET /api/pastes/{addr}", h.view)
	return mux
}

func (h *Handler) errToStatusCode(err error) int {
	if err == nil {
		return http.StatusOK
	}
	switch {
	case errors.Is(err, metadata.ErrNotFound), errors.Is(err, blobs.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, paste.ErrPasteIsEmpty):
		return http.StatusBadRequest
	case errors.Is(err, paste.ErrPasteSizeTooBig):
		return http.StatusRequestEntityTooLarge
	default:
		return http.StatusInternalServerError
	}
}
