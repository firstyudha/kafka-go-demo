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

func newTestInventoryLog(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})), buf
}

func TestInventoryHandler_HappyPath(t *testing.T) {
	log, buf := newTestInventoryLog(t)
	h := InventoryHandler(log)

	payload, _ := json.Marshal(event.OrderCreatedPayload{OrderID: "ORD-002", Amount: 750000})
	ev := event.Event{EventID: "evt-2", EventType: "OrderCreated", Payload: payload}

	if err := h(context.Background(), ev); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	logs := buf.String()
	if !strings.Contains(logs, "reserving inventory") {
		t.Errorf("logs missing 'reserving inventory': %s", logs)
	}
	if !strings.Contains(logs, "inventory reserved") {
		t.Errorf("logs missing 'inventory reserved': %s", logs)
	}
	if !strings.Contains(logs, "order_id=ORD-002") {
		t.Errorf("logs missing order_id=ORD-002: %s", logs)
	}
	if !strings.Contains(logs, "amount=750000") {
		t.Errorf("logs missing amount=750000: %s", logs)
	}
}

func TestInventoryHandler_BadPayload(t *testing.T) {
	log, buf := newTestInventoryLog(t)
	h := InventoryHandler(log)

	ev := event.Event{EventID: "evt-bad", EventType: "OrderCreated", Payload: []byte("not json")}
	err := h(context.Background(), ev)
	if err == nil {
		t.Fatal("expected error on bad payload, got nil")
	}
	if !strings.Contains(err.Error(), "decode order payload") {
		t.Errorf("error = %q, want it to contain 'decode order payload'", err.Error())
	}
	if strings.Contains(buf.String(), "inventory reserved") {
		t.Errorf("logs should NOT contain 'inventory reserved' on error: %s", buf.String())
	}
}
