// Package event defines the JSON event envelope and helpers for the demo.
// All events flowing through Kafka use the Event type defined here.
package event

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// Event is the envelope around every event on the Kafka topic.
// Payload is kept as json.RawMessage so the envelope stays generic —
// concrete payload types (e.g. OrderCreatedPayload) are unmarshalled
// by the consumer after reading the envelope.
type Event struct {
	EventID   string          `json:"eventId"`
	EventType string          `json:"eventType"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// NewEvent builds an Event with a unique ID and a UTC timestamp.
// payload is anything json.Marshal can handle; it is stored as raw JSON.
func NewEvent(eventType string, payload any) (Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	return Event{
		EventID:   generateEventID(),
		EventType: eventType,
		Timestamp: time.Now().UTC(),
		Payload:   raw,
	}, nil
}

// generateEventID returns "evt-" followed by 13 base36 chars drawn from
// crypto/rand (~64 bits of entropy). Sufficient for a demo; not a UUID.
// Note: Go stdlib has no encoding/base36, so we use math/big.Text(36).
func generateEventID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is catastrophic on any supported platform;
		// fall back to nanosecond timestamp so we still produce a unique id.
		ts := time.Now().UTC().UnixNano()
		return "evt-" + big.NewInt(ts).Text(36)
	}
	n := binary.BigEndian.Uint64(b[:])
	// Mask off the high bit so big.NewInt sees a non-negative int64;
	// this also limits us to ~63 bits, which fits in 13 base36 chars
	// (36^13 ≈ 2^68, but ~63 bits avoids the leading-minus edge case).
	n &^= 1 << 63
	suffix := big.NewInt(int64(n)).Text(36)
	suffix = strings.Repeat("0", 13-len(suffix)) + suffix
	if len(suffix) > 13 {
		suffix = suffix[len(suffix)-13:]
	}
	return "evt-" + suffix
}

// Decode parses a JSON-encoded envelope and returns the typed Event.
// The Payload field is left as json.RawMessage so callers can unmarshal
// it into the specific payload type (e.g. OrderCreatedPayload) themselves.
func Decode(raw []byte) (Event, error) {
	var ev Event
	if err := json.Unmarshal(raw, &ev); err != nil {
		return Event{}, fmt.Errorf("decode event: %w", err)
	}
	return ev, nil
}
