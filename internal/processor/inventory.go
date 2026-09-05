package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"kafka-go-demo/internal/consumer"
	"kafka-go-demo/internal/event"
)

// InventoryHandler returns a consumer.Handler that simulates reserving
// inventory for an order. Same log shape as EmailHandler. The handler is
// unaware of simulateFailure — that is honored at the consumer.Service
// level (processWithRetry short-circuits attempt 1).
func InventoryHandler(log *slog.Logger) consumer.Handler {
	return func(ctx context.Context, ev event.Event) error {
		var p event.OrderCreatedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return fmt.Errorf("decode order payload: %w", err)
		}
		log.Info("reserving inventory",
			"event_id", ev.EventID,
			"event_type", ev.EventType,
			"order_id", p.OrderID,
			"amount", p.Amount,
		)
		log.Info("inventory reserved", "order_id", p.OrderID)
		return nil
	}
}
