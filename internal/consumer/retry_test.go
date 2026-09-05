package consumer

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-go-demo/internal/event"
)

func newTestRetryService(t *testing.T, handler Handler, simulate bool) (*Service, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	log := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return &Service{
		reader:          &fakeReader{},
		log:             log,
		consumerID:      "test-consumer",
		handler:         handler,
		retryCfg:        RetryConfig{MaxAttempts: 3, Delay: 1 * time.Millisecond},
		simulateFailure: simulate,
	}, buf
}

func TestProcessWithRetry_FirstAttemptSucceeds(t *testing.T) {
	called := 0
	h := func(ctx context.Context, ev event.Event) error {
		called++
		return nil
	}
	s, buf := newTestRetryService(t, h, false)
	err := s.processWithRetry(context.Background(), kafka.Message{Partition: 0, Offset: 1}, event.Event{EventID: "evt-1"})
	if err != nil {
		t.Fatalf("processWithRetry: %v", err)
	}
	if called != 1 {
		t.Errorf("handler calls = %d, want 1", called)
	}
	if !strings.Contains(buf.String(), "attempt=1") {
		t.Errorf("logs missing attempt=1: %s", buf.String())
	}
}

func TestProcessWithRetry_SecondAttemptSucceeds(t *testing.T) {
	called := 0
	h := func(ctx context.Context, ev event.Event) error {
		called++
		if called == 1 {
			return errors.New("transient")
		}
		return nil
	}
	s, buf := newTestRetryService(t, h, false)
	err := s.processWithRetry(context.Background(), kafka.Message{Partition: 0, Offset: 2}, event.Event{EventID: "evt-2"})
	if err != nil {
		t.Fatalf("processWithRetry: %v", err)
	}
	if called != 2 {
		t.Errorf("handler calls = %d, want 2", called)
	}
	logs := buf.String()
	if !strings.Contains(logs, "attempt=1") || !strings.Contains(logs, "attempt=2") {
		t.Errorf("logs missing attempt=1 and attempt=2: %s", logs)
	}
}

func TestProcessWithRetry_Exhaustion(t *testing.T) {
	called := 0
	h := func(ctx context.Context, ev event.Event) error {
		called++
		return errors.New("always fails")
	}
	s, _ := newTestRetryService(t, h, false)
	err := s.processWithRetry(context.Background(), kafka.Message{Partition: 0, Offset: 3}, event.Event{EventID: "evt-3"})
	if err == nil {
		t.Fatal("expected error after exhaustion, got nil")
	}
	if called != 3 {
		t.Errorf("handler calls = %d, want 3 (one per attempt)", called)
	}
	if !strings.Contains(err.Error(), "max attempts (3) exceeded") {
		t.Errorf("error = %q, want it to contain 'max attempts (3) exceeded'", err.Error())
	}
	if !strings.Contains(err.Error(), "always fails") {
		t.Errorf("error = %q, want it to wrap the last handler error", err.Error())
	}
}

func TestProcessWithRetry_ContextCanceledDuringSleep(t *testing.T) {
	called := 0
	h := func(ctx context.Context, ev event.Event) error {
		called++
		return errors.New("force retry")
	}
	s, _ := newTestRetryService(t, h, false)
	// Use a longer delay so the ctx cancel beats the sleep.
	s.retryCfg.Delay = 1 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := s.processWithRetry(ctx, kafka.Message{Partition: 0, Offset: 4}, event.Event{EventID: "evt-4"})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("processWithRetry err = %v, want context.Canceled", err)
	}
	if called != 1 {
		t.Errorf("handler calls = %d, want 1 (cancel should prevent second attempt)", called)
	}
}

func TestProcessWithRetry_SimulateFailure_ShortCircuit(t *testing.T) {
	called := 0
	h := func(ctx context.Context, ev event.Event) error {
		called++
		return nil
	}
	s, buf := newTestRetryService(t, h, true)
	err := s.processWithRetry(context.Background(), kafka.Message{Partition: 0, Offset: 5}, event.Event{EventID: "evt-5"})
	if err != nil {
		t.Fatalf("processWithRetry: %v", err)
	}
	if called != 1 {
		t.Errorf("handler calls = %d, want 1 (attempt 1 was simulated, attempt 2 called handler)", called)
	}
	if !strings.Contains(buf.String(), "simulated failure") {
		t.Errorf("logs missing 'simulated failure': %s", buf.String())
	}
}

func TestProcessWithRetry_SimulateFailure_Off(t *testing.T) {
	called := 0
	h := func(ctx context.Context, ev event.Event) error {
		called++
		return nil
	}
	s, buf := newTestRetryService(t, h, false)
	err := s.processWithRetry(context.Background(), kafka.Message{Partition: 0, Offset: 6}, event.Event{EventID: "evt-6"})
	if err != nil {
		t.Fatalf("processWithRetry: %v", err)
	}
	if called != 1 {
		t.Errorf("handler calls = %d, want 1 (no simulation, first attempt succeeds)", called)
	}
	if strings.Contains(buf.String(), "simulated failure") {
		t.Errorf("logs should NOT contain 'simulated failure' when simulateFailure=false: %s", buf.String())
	}
}
