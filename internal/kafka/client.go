// Package kafkaclient is a thin factory over segmentio/kafka-go.
// It exposes constructors for Writer, Reader, and a one-shot
// topic-ensure helper. Callers use the underlying kafka-go types
// directly — no custom interfaces.
package kafkaclient

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// NewWriter returns a *kafka.Writer configured for synchronous production
// with the LeastBytes partition balancer. RequiredAcks=All is safe on a
// single broker for a demo and surfaces errors immediately.
func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 10 * time.Millisecond,
	}
}

// NewReader returns a *kafka.Reader in consumer-group mode.
// CommitInterval is 0 so commits must be explicit — required by PRD §16
// (commit only after successful processing).
func NewReader(brokers []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
}

// EnsureTopic creates the topic with the given partition count if it does
// not already exist. Idempotent: a second call for an already-existing
// topic with matching config returns nil.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int) error {
	if len(brokers) == 0 {
		return fmt.Errorf("EnsureTopic: no brokers configured")
	}

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial broker %s: %w", brokers[0], err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafka.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		return fmt.Errorf("dial controller %s: %w", controllerAddr, err)
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
}
