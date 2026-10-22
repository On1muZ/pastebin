package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

type createRequest struct {
	Content         string     `json:"content"`
	DeleteAfterView bool       `json:"delete_after_view"`
	DeleteAt        *time.Time `json:"delete_at"`
}

type createResponse struct {
	Addr     string     `json:"addr"`
	DeleteAt *time.Time `json:"delete_at,omitempty"`
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodySize)
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.DeleteAt != nil && !req.DeleteAt.After(time.Now()) {
		http.Error(w, "delete_at must be in the future", http.StatusBadRequest)
		return
	}
	pasteInfo, err := h.pastes.Create(r.Context(), []byte(req.Content), req.DeleteAfterView, req.DeleteAt)
	if err != nil {
		code := h.errToStatusCode(err)
		if code == http.StatusInternalServerError {
			h.log.Error("could not create paste", slog.String("error", err.Error()))
			http.Error(w, "internal error", code)
			return
		}
		http.Error(w, err.Error(), code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(createResponse{
		Addr:     pasteInfo.Addr,
		DeleteAt: pasteInfo.DeleteAt,
	})
}
