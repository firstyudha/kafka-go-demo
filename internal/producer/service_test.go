package producer

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-go-demo/internal/event"
)

// fakeMessageWriter is a test fake for messageWriter. It implements the
// Writer surface PublishOrder uses: WriteMessages accepts the message
// (capturing the per-message pendingAck from WriterData) and exposes a
// test hook (deliver) so the test can drive the per-message ack after
// WriteMessages returns.
type fakeMessageWriter struct {
	// lastPendingAck is the *pendingAck extracted from the most recent
	// WriteMessages call's first message. nil until the first publish.
	lastPendingAck *pendingAck
}

func (f *fakeMessageWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	// Capture the pendingAck from the first message. PublishOrder writes
	// exactly one message per call, so this is the one we care about.
	if len(msgs) > 0 {
		if pa, ok := msgs[0].WriterData.(*pendingAck); ok {
			f.lastPendingAck = pa
		}
	}
	return nil
}

// deliver hands a fake broker-acked message back to the PublishOrder
// goroutine through the pendingAck channel.
func (f *fakeMessageWriter) deliver(m kafka.Message) {
	if f.lastPendingAck == nil {
		return
	}
	f.lastPendingAck.done <- m
}

func TestNew_ReturnsProducer(t *testing.T) {
	w := &kafka.Writer{Topic: "orders"}
	p := New(w, "orders", slog.Default())
	if p == nil {
		t.Fatal("New returned nil")
	}
	if p.topic != "orders" {
		t.Errorf("topic = %q, want orders", p.topic)
	}
}

func TestPublishOrder_ValidationErrorOnEmptyOrderID(t *testing.T) {
	fake := &fakeMessageWriter{}
	log := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	p := New(fake, "orders", log)
	_, err := p.PublishOrder(context.Background(), event.OrderCreatedPayload{OrderID: "", Amount: 100})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ValidationError) {
		t.Errorf("err = %v, want errors.Is(err, ValidationError)", err)
	}
}

func TestPublishOrder_ValidationErrorOnZeroAmount(t *testing.T) {
	fake := &fakeMessageWriter{}
	log := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	p := New(fake, "orders", log)
	_, err := p.PublishOrder(context.Background(), event.OrderCreatedPayload{OrderID: "ORD-001", Amount: 0})
	if !errors.Is(err, ValidationError) {
		t.Errorf("err = %v, want errors.Is(err, ValidationError)", err)
	}
}

// TestPublishOrder_KafkaErrorOnUnreachableBroker verifies that when the
// underlying writer returns an error from WriteMessages, PublishOrder
// surfaces it as a non-validation error so the HTTP layer can map to 503.
func TestPublishOrder_KafkaErrorOnUnreachableBroker(t *testing.T) {
	w := &kafka.Writer{
		Addr:         kafka.TCP("127.0.0.1:1"), // unreachable
		Topic:        "orders",
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 10,
	}
	defer w.Close()
	log := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	p := New(w, "orders", log)

	_, err := p.PublishOrder(context.Background(), event.OrderCreatedPayload{OrderID: "ORD-001", Amount: 100})
	if err == nil {
		t.Fatal("expected error from unreachable broker, got nil")
	}
	if errors.Is(err, ValidationError) {
		t.Errorf("err should be a publish error, not validation: %v", err)
	}
}

// TestPublishOrder_PartitionOffsetTimeout verifies that when no ack is
// delivered (the 2s timeout fires), PublishOrder logs a WARN line
// without partition/offset and still returns success.
func TestPublishOrder_PartitionOffsetTimeout(t *testing.T) {
	fake := &fakeMessageWriter{}
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	p := New(fake, "orders", log)

	payload := event.OrderCreatedPayload{OrderID: "ORD-T1", Amount: 100}
	ev, err := p.PublishOrder(context.Background(), payload)
	if err != nil {
		t.Fatalf("PublishOrder returned error: %v", err)
	}
	if ev.EventID == "" {
		t.Fatalf("expected non-empty event ID")
	}

	// Fake never calls deliver(); wait for the 2s timeout + buffer.
	time.Sleep(2500 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("expected WARN log line; got: %s", out)
	}
	if !strings.Contains(out, "event published (partition/offset unavailable)") {
		t.Fatalf("expected 'partition/offset unavailable' message; got: %s", out)
	}
}

// TestPublishOrder_LogsPartitionOffsetOnCompletion verifies that when
// the ack is delivered with a populated Partition/Offset, PublishOrder
// logs those values in the 'event published' INFO line.
func TestPublishOrder_LogsPartitionOffsetOnCompletion(t *testing.T) {
	fake := &fakeMessageWriter{}
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	p := New(fake, "orders", log)

	payload := event.OrderCreatedPayload{OrderID: "ORD-T2", Amount: 250}

	// Run PublishOrder in a goroutine so the test goroutine can deliver
	// the ack before PublishOrder's 2s timeout fires.
	type result struct {
		ev  event.Event
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		ev, err := p.PublishOrder(context.Background(), payload)
		resCh <- result{ev: ev, err: err}
	}()

	// Wait briefly for WriteMessages to run and stash the pendingAck.
	deadline := time.Now().Add(500 * time.Millisecond)
	for fake.lastPendingAck == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fake.lastPendingAck == nil {
		t.Fatal("WriteMessages did not capture a pendingAck")
	}

	// Drive the ack with a fake broker response.
	fake.deliver(kafka.Message{
		Topic:     "orders",
		Partition: 1,
		Offset:    42,
	})

	// Wait for PublishOrder to return.
	select {
	case r := <-resCh:
		if r.err != nil {
			t.Fatalf("PublishOrder returned error: %v", r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("PublishOrder did not return after ack was delivered")
	}

	// Let any deferred log emission settle.
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "level=INFO") {
		t.Fatalf("expected INFO log line; got: %s", out)
	}
	if !strings.Contains(out, "event published") {
		t.Fatalf("expected 'event published' message; got: %s", out)
	}
	if !strings.Contains(out, "partition=1") {
		t.Fatalf("expected 'partition=1' in log; got: %s", out)
	}
	if !strings.Contains(out, "offset=42") {
		t.Fatalf("expected 'offset=42' in log; got: %s", out)
	}
	if strings.Contains(out, "partition/offset unavailable") {
		t.Fatalf("unexpected WARN 'partition/offset unavailable' line; got: %s", out)
	}
}
