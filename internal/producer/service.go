// Package producer wraps a kafka.Writer to publish OrderCreated events.
// It validates payloads and wraps validation failures in a sentinel
// ValidationError so the HTTP layer can map to HTTP 400.
package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-go-demo/internal/event"
)

// ValidationError is returned (wrapped) by PublishOrder when the payload
// fails event.OrderCreatedPayload.Validate(). Use errors.Is to detect.
var ValidationError = errors.New("validation error")

// messageWriter is the minimal Kafka writer surface Producer needs.
// *kafka.Writer satisfies it via WriteMessages. Tests pass a
// fakeMessageWriter to drive the per-message ack delivery directly.
//
// kafka-go v0.4.47 does NOT expose a per-message Completion field
// (Message.Completion does not exist). The Writer has a single
// Completion func(messages []Message, err error) called per batch.
// We install that callback at New() time via a type-assert to
// *kafka.Writer (see installWriterCompletion). Tests bypass that
// mechanism and drive the ack channel directly via the fake.
type messageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// pendingAck is carried in Message.WriterData. The Writer-level
// Completion callback delivers the broker-acked Message back to the
// PublishOrder goroutine through the buffered channel.
type pendingAck struct {
	done chan kafka.Message // buffered size 1
}

// Producer publishes OrderCreated events to a single Kafka topic.
type Producer struct {
	writer messageWriter
	topic  string
	log    *slog.Logger
}

// New constructs a Producer. The caller owns the writer's lifecycle;
// close it after Shutdown.
//
// For *kafka.Writer, this installs a batch-level Completion callback
// that delivers the broker-assigned partition/offset to each in-flight
// PublishOrder via the message's WriterData slot. For other writers
// (e.g. test fakes), this is a no-op — the fake drives acks directly.
func New(w messageWriter, topic string, log *slog.Logger) *Producer {
	p := &Producer{writer: w, topic: topic, log: log}
	if real, ok := w.(*kafka.Writer); ok {
		real.Completion = func(messages []kafka.Message, err error) {
			if err != nil {
				// WriteMessages already returned the error to PublishOrder;
				// per-message acks here are skipped.
				return
			}
			for _, m := range messages {
				pa, ok := m.WriterData.(*pendingAck)
				if !ok || pa == nil {
					continue
				}
				// Non-blocking send; ack is delivered exactly once.
				select {
				case pa.done <- m:
				default:
				}
			}
		}
	}
	return p
}

// PublishOrder validates the payload, builds an envelope, writes to Kafka,
// and returns the published event. Returns an error wrapping ValidationError
// on invalid input, or a wrapped kafka error on publish failure.
func (p *Producer) PublishOrder(ctx context.Context, payload event.OrderCreatedPayload) (event.Event, error) {
	if err := payload.Validate(); err != nil {
		// Preserve the underlying validation message in the error string
		// so the HTTP layer can surface it to the client (e.g., "orderId
		// is required") while errors.Is(err, ValidationError) still works.
		return event.Event{}, fmt.Errorf("invalid order: %s: %w", err, ValidationError)
	}

	p.log.Info("order created",
		"order_id", payload.OrderID,
		"amount", payload.Amount,
	)

	ev, err := event.NewEvent(event.EventTypeOrderCreated, payload)
	if err != nil {
		return event.Event{}, fmt.Errorf("build event: %w", err)
	}

	raw, err := json.Marshal(ev)
	if err != nil {
		return event.Event{}, fmt.Errorf("marshal event: %w", err)
	}

	// kafka-go's WriteMessages does NOT mutate msg.Partition/msg.Offset
	// on the caller's slice; the broker-assigned partition/offset only
	// surface in the Writer's batch-level Completion callback. For each
	// PublishOrder call we stash a pendingAck in Message.WriterData so
	// the callback can deliver the ack back through a buffered channel.
	//
	// 2s timeout is a defensive cap; Completion normally fires in
	// milliseconds.
	pa := &pendingAck{done: make(chan kafka.Message, 1)}
	msg := kafka.Message{
		Key:        []byte(ev.EventID),
		Value:      raw,
		WriterData: pa,
	}
	writeErr := p.writer.WriteMessages(ctx, msg)

	// If WriteMessages returned an error, the publish failed; return early
	// without waiting for Completion. The HTTP layer maps this to 503.
	if writeErr != nil {
		return event.Event{}, fmt.Errorf("publish to kafka: %w", writeErr)
	}

	select {
	case m := <-pa.done:
		p.log.Info("event published",
			"event_id", ev.EventID,
			"event_type", ev.EventType,
			"topic", p.topic,
			"partition", m.Partition,
			"offset", m.Offset,
		)
	case <-time.After(2 * time.Second):
		// Should be unreachable in practice. If it happens, the publish
		// already succeeded; we just couldn't capture metadata for logging.
		p.log.Warn("event published (partition/offset unavailable)",
			"event_id", ev.EventID,
			"event_type", ev.EventType,
			"topic", p.topic,
		)
	}

	return ev, nil
}
