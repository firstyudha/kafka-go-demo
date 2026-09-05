package httpserver

import (
	"encoding/json"
	"errors"
	"net/http"

	"kafka-go-demo/internal/event"
	"kafka-go-demo/internal/producer"
	"kafka-go-demo/internal/web"
)

// handleIndex renders the order form. Template is parsed once in web package.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := web.Templates.ExecuteTemplate(w, "index.html", nil); err != nil {
		s.log.Error("render index", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
}

// handleCreateOrder decodes an OrderCreatedPayload, calls Producer.PublishOrder,
// and maps the result to HTTP status codes per PRD §26.
func (s *Server) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	var payload event.OrderCreatedPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	ev, err := s.producer.PublishOrder(r.Context(), payload)
	if err != nil {
		if errors.Is(err, producer.ValidationError) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.log.Error("publish failed", "err", err)
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"eventId": ev.EventID,
		"status":  "published",
	})
}
