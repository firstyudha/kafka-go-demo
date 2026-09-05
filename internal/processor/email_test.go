package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"kafka-go-demo/internal/event"
)

func newTestEmailLog(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})), buf
}

func TestEmailHandler_HappyPath(t *testing.T) {
	log, buf := newTestEmailLog(t)
	h := EmailHandler(log)

	payload, _ := json.Marshal(event.OrderCreatedPayload{OrderID: "ORD-001", Amount: 500000})
	ev := event.Event{EventID: "evt-1", EventType: "OrderCreated", Payload: payload}

	if err := h(context.Background(), ev); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	logs := buf.String()
	if !strings.Contains(logs, "processing email") {
		t.Errorf("logs missing 'processing email': %s", logs)
	}
	if !strings.Contains(logs, "email confirmation sent") {
		t.Errorf("logs missing 'email confirmation sent': %s", logs)
	}
	if !strings.Contains(logs, "order_id=ORD-001") {
		t.Errorf("logs missing order_id=ORD-001: %s", logs)
	}
	if !strings.Contains(logs, "amount=500000") {
		t.Errorf("logs missing amount=500000: %s", logs)
	}
}

func TestEmailHandler_BadPayload(t *testing.T) {
	log, buf := newTestEmailLog(t)
	h := EmailHandler(log)

	ev := event.Event{EventID: "evt-bad", EventType: "OrderCreated", Payload: []byte("not json")}
	err := h(context.Background(), ev)
	if err == nil {
		t.Fatal("expected error on bad payload, got nil")
	}
	if !strings.Contains(err.Error(), "decode order payload") {
		t.Errorf("error = %q, want it to contain 'decode order payload'", err.Error())
	}
	if strings.Contains(buf.String(), "email confirmation sent") {
		t.Errorf("logs should NOT contain 'email confirmation sent' on error: %s", buf.String())
	}
}
