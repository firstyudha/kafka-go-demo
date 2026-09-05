// Package processor provides the per-consumer side-effect functions.
// Both consumers use the same shared consumer loop; they differ only in
// the Handler closure returned by EmailHandler and InventoryHandler.
package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"kafka-go-demo/internal/consumer"
	"kafka-go-demo/internal/event"
)

// EmailHandler returns a consumer.Handler that simulates sending a
// confirmation email. The handler unmarshals the OrderCreated payload to
// extract order_id/amount for the log lines. Returns a wrapped error if
// the payload cannot be decoded (treated as a processing failure -> retry).
func EmailHandler(log *slog.Logger) consumer.Handler {
	return func(ctx context.Context, ev event.Event) error {
		var p event.OrderCreatedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return fmt.Errorf("decode order payload: %w", err)
		}
		log.Info("processing email",
			"event_id", ev.EventID,
			"event_type", ev.EventType,
			"order_id", p.OrderID,
			"amount", p.Amount,
		)
		log.Info("email confirmation sent", "order_id", p.OrderID)
		return nil
	}
}
