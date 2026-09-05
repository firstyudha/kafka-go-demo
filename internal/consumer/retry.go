package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-go-demo/internal/event"
)

// processWithRetry invokes the handler up to RetryConfig.MaxAttempts times,
// sleeping RetryConfig.Delay between attempts. Returns nil on first success;
// returns a wrapped last-error if all attempts fail.
//
// When simulateFailure is true, attempt 1 is short-circuited to a simulated
// error WITHOUT invoking the handler. Email handlers run with
// simulateFailure=false, so this path is inert for them.
func (s *Service) processWithRetry(ctx context.Context, msg kafka.Message, ev event.Event) error {
	var lastErr error
	for attempt := 1; attempt <= s.retryCfg.MaxAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.retryCfg.Delay):
			}
		}

		s.log.Info("processing attempt",
			"consumer_id", s.consumerID,
			"event_id", ev.EventID,
			"attempt", attempt,
			"partition", msg.Partition,
			"offset", msg.Offset,
		)

		if attempt == 1 && s.simulateFailure {
			s.log.Error("processing failed",
				"consumer_id", s.consumerID,
				"event_id", ev.EventID,
				"attempt", attempt,
				"err", "simulated failure",
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
			lastErr = errors.New("simulated failure")
			continue
		}

		if err := s.handler(ctx, ev); err != nil {
			s.log.Error("processing failed",
				"consumer_id", s.consumerID,
				"event_id", ev.EventID,
				"attempt", attempt,
				"err", err,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)
			lastErr = err
			continue
		}

		s.log.Info("processing succeeded",
			"consumer_id", s.consumerID,
			"event_id", ev.EventID,
			"attempt", attempt,
		)
		return nil
	}
	return fmt.Errorf("max attempts (%d) exceeded: %w", s.retryCfg.MaxAttempts, lastErr)
}
