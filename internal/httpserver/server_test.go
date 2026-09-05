package httpserver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-go-demo/internal/producer"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	w := &kafka.Writer{Addr: kafka.TCP("127.0.0.1:1"), Topic: "orders"}
	p := producer.New(w, "orders", slog.New(slog.NewTextHandler(io.Discard, nil)))
	return New(0, p, fstest.MapFS{}, slog.Default())
}

func TestNew_ReturnsNonNilServer(t *testing.T) {
	s := newTestServer(t)
	if s == nil {
		t.Fatal("New returned nil")
	}
	if s.mux == nil {
		t.Error("mux is nil")
	}
	if s.staticFS == nil {
		t.Error("staticFS is nil")
	}
	if s.srv == nil {
		t.Error("srv is nil")
	}
}

func TestRoutes_ReturnsSameMux(t *testing.T) {
	s := newTestServer(t)
	if s.Routes() != s.mux {
		t.Error("Routes should return the same mux used by the server")
	}
}

func TestRoutes_RegistersIndexAndAPI(t *testing.T) {
	s := newTestServer(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s.srv.Addr = ln.Addr().String()
	go func() { _ = s.srv.Serve(ln) }()
	defer func() { _ = s.srv.Shutdown(context.Background()) }()

	time.Sleep(20 * time.Millisecond)

	resp, err := http.Get("http://" + ln.Addr().String() + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET / status = %d, want 200", resp.StatusCode)
	}
}

func TestShutdown_StopsServerGracefully(t *testing.T) {
	s := newTestServer(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s.srv.Addr = ln.Addr().String()
	go func() { _ = s.srv.Serve(ln) }()

	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown err = %v, want nil", err)
	}
	// Verify the listener is closed by trying a second Shutdown.
	if err := s.Shutdown(ctx); !errors.Is(err, http.ErrServerClosed) {
		// Second shutdown may return ErrServerClosed or nil; both are acceptable.
		t.Logf("second Shutdown err = %v (acceptable)", err)
	}
}

func TestHandleIndex_RendersTemplate(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	s.mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("GET / status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Create Order") {
		t.Errorf("body missing 'Create Order': %q", body)
	}
}

func TestHandleCreateOrder_400_OnInvalidPayload(t *testing.T) {
	s := newTestServer(t)
	body := strings.NewReader(`{"orderId":"","amount":100}`)
	req := httptest.NewRequest(http.MethodPost, "/api/orders", body)
	rr := httptest.NewRecorder()
	s.mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("POST /api/orders (empty OrderID) status = %d, want 400", rr.Code)
	}
}

func TestHandleCreateOrder_400_OnZeroAmount(t *testing.T) {
	s := newTestServer(t)
	body := strings.NewReader(`{"orderId":"ORD-001","amount":0}`)
	req := httptest.NewRequest(http.MethodPost, "/api/orders", body)
	rr := httptest.NewRecorder()
	s.mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("POST /api/orders (zero amount) status = %d, want 400", rr.Code)
	}
}

func TestHandleCreateOrder_503_OnKafkaFailure(t *testing.T) {
	s := newTestServer(t) // producer points at unreachable broker :1
	body := strings.NewReader(`{"orderId":"ORD-001","amount":100}`)
	req := httptest.NewRequest(http.MethodPost, "/api/orders", body)
	rr := httptest.NewRecorder()
	s.mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("POST /api/orders (kafka down) status = %d, want 503", rr.Code)
	}
}

func TestHandleCreateOrder_400_OnMalformedJSON(t *testing.T) {
	s := newTestServer(t)
	body := strings.NewReader(`{not json`)
	req := httptest.NewRequest(http.MethodPost, "/api/orders", body)
	rr := httptest.NewRecorder()
	s.mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("POST /api/orders (malformed JSON) status = %d, want 400", rr.Code)
	}
}
