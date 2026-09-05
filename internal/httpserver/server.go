// Package httpserver exposes the Producer over HTTP: serves the order form,
// handles POST /api/orders, and serves static assets from an embedded FS.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"kafka-go-demo/internal/producer"
)

// Server is the HTTP front-end for the Producer. Construct with New,
// wire Routes into the http.Server, and call Start/Shutdown.
type Server struct {
	port     int
	producer *producer.Producer
	log      *slog.Logger
	mux      *http.ServeMux
	staticFS fs.FS
	srv      *http.Server
}

// New constructs a Server. The staticFS argument is typically web.StaticFS().
// Routes and the http.Server are wired here so the caller just calls Start.
func New(port int, prod *producer.Producer, staticFS fs.FS, log *slog.Logger) *Server {
	mux := http.NewServeMux()
	s := &Server{
		port:     port,
		producer: prod,
		log:      log,
		mux:      mux,
		staticFS: staticFS,
	}
	s.routes()
	s.srv = &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// routes registers handlers. Kept private; the mux is built once in New.
func (s *Server) routes() {
	// Index page — exact match.
	s.mux.HandleFunc("GET /{$}", s.handleIndex)

	// Order creation API — exact match.
	s.mux.HandleFunc("POST /api/orders", s.handleCreateOrder)

	// Static assets — subtree match under /static/.
	s.mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(s.staticFS))))
}

// Routes returns the underlying mux. Exposed for testing.
func (s *Server) Routes() *http.ServeMux {
	return s.mux
}

// Start blocks while serving HTTP. Returns http.ErrServerClosed on graceful
// shutdown; any other error is a real failure.
func (s *Server) Start() error {
	if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return http.ErrServerClosed
}

// Shutdown gracefully stops the server. Pass a context with a timeout.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
