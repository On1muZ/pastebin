package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

type viewResponse struct {
	Addr            string     `json:"addr"`
	Content         string     `json:"content"`
	Views           int64      `json:"views"`
	DeleteAfterView bool       `json:"delete_after_view"`
	DeleteAt        *time.Time `json:"delete_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

func (h *Handler) view(w http.ResponseWriter, r *http.Request) {
	addr := r.PathValue("addr")
	pasteInfo, content, err := h.pastes.View(r.Context(), addr)
	if err != nil {
		code := h.errToStatusCode(err)
		if code == http.StatusInternalServerError {
			h.log.Error("could not view paste", slog.String("addr", addr), slog.String("error", err.Error()))
			http.Error(w, "internal error", code)
			return
		}
		http.Error(w, err.Error(), code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(viewResponse{
		Addr:            pasteInfo.Addr,
		Content:         string(content),
		Views:           pasteInfo.Views,
		DeleteAfterView: pasteInfo.DeleteAfterView,
		DeleteAt:        pasteInfo.DeleteAt,
		CreatedAt:       pasteInfo.CreatedAt,
	})
}
