package event

import "errors"

// OrderCreatedPayload is the payload for OrderCreated events.
type OrderCreatedPayload struct {
	OrderID string  `json:"orderId"`
	Amount  float64 `json:"amount"`
}

// EventTypeOrderCreated is the eventType value for OrderCreated events.
// Use this constant rather than the literal "OrderCreated".
const EventTypeOrderCreated = "OrderCreated"

// Validate returns an error if the payload is missing required fields.
// OrderID must be non-empty; Amount must be positive.
func (p OrderCreatedPayload) Validate() error {
	if p.OrderID == "" {
		return errors.New("orderId is required")
	}
	if p.Amount <= 0 {
		return errors.New("amount must be greater than 0")
	}
	return nil
}
