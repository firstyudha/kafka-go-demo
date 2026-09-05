// Package consumer provides a shared read/deserialize/log/commit loop for
// the demo's Kafka consumers. Per-consumer side effects are supplied as a
// Handler callback; both Email and Inventory consumers use this loop.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-go-demo/internal/event"
)

// Handler is the per-message processing function. Return nil to commit the
// offset; return a non-nil error to skip commit and trigger retry (until the
// retry budget is exhausted).
type Handler func(ctx context.Context, ev event.Event) error

// RetryConfig controls per-message retry behavior. Total attempts = MaxAttempts.
// Delay is applied between attempts (fixed backoff, no jitter).
type RetryConfig struct {
	MaxAttempts int
	Delay       time.Duration
}

// reader is the minimal Kafka reader surface Service needs. *kafka.Reader
// satisfies it. The unexported interface keeps the test seam tight — tests
// pass a fakeReader without depending on the concrete kafka.Reader type.
type reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Service drives the consumer loop. Construct via NewService, run via Run.
type Service struct {
	reader          reader
	log             *slog.Logger
	consumerID      string
	handler         Handler
	retryCfg        RetryConfig
	simulateFailure bool
	seenPartitions  map[int]struct{} // first-message-per-partition discovery log
}

// NewService wires a Service. Returns an error if consumerID is empty,
// handler is nil, or MaxAttempts is less than 1.
func NewService(r reader, consumerID string, handler Handler, rc RetryConfig, simulate bool, log *slog.Logger) (*Service, error) {
	if consumerID == "" {
		return nil, errors.New("consumerID must not be empty")
	}
	if handler == nil {
		return nil, errors.New("handler must not be nil")
	}
	if rc.MaxAttempts < 1 {
		return nil, fmt.Errorf("MaxAttempts must be >= 1, got %d", rc.MaxAttempts)
	}
	return &Service{
		reader:          r,
		log:             log,
		consumerID:      consumerID,
		handler:         handler,
		retryCfg:        rc,
		simulateFailure: simulate,
		seenPartitions:  make(map[int]struct{}),
	}, nil
}

// Run blocks until ctx is cancelled or an unrecoverable error occurs.
// Returns nil on graceful shutdown (ctx cancelled); returns a wrapped
// error on fetch/commit failures so the caller can decide to exit.
func (s *Service) Run(ctx context.Context) error {
	for {
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		ev, derr := event.Decode(msg.Value)
		if derr != nil {
			s.log.Error("malformed event skipped",
				"consumer_id", s.consumerID,
				"err", derr,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"raw_bytes", len(msg.Value),
			)
			if cerr := s.reader.CommitMessages(ctx, msg); cerr != nil {
				return fmt.Errorf("commit poison-pill: %w", cerr)
			}
			continue
		}

		s.log.Info("received event",
			"consumer_id", s.consumerID,
			"event_id", ev.EventID,
			"event_type", ev.EventType,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)

		// First-message-per-partition announcement: lets the presenter see
		// which partitions this consumer instance owns after a rebalance.
		// Cheap: one map lookup + one log line per partition, then no-op.
		if _, ok := s.seenPartitions[msg.Partition]; !ok {
			s.seenPartitions[msg.Partition] = struct{}{}
			s.log.Info("assigned partition",
				"consumer_id", s.consumerID,
				"partition", msg.Partition,
			)
		}

		if perr := s.processWithRetry(ctx, msg, ev); perr != nil {
			s.log.Error("max attempts exceeded",
				"consumer_id", s.consumerID,
				"event_id", ev.EventID,
				"event_type", ev.EventType,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"err", perr,
			)
			continue
		}

		if cerr := s.reader.CommitMessages(ctx, msg); cerr != nil {
			s.log.Error("commit failed",
				"consumer_id", s.consumerID,
				"event_id", ev.EventID,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"err", cerr,
			)
			return fmt.Errorf("commit message: %w", cerr)
		}

		s.log.Info("event processed",
			"consumer_id", s.consumerID,
			"event_id", ev.EventID,
			"event_type", ev.EventType,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)
	}
}
