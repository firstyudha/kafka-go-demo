package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-go-demo/internal/event"
)

// fakeReader satisfies the unexported reader interface used by Service.
// Tests enqueue kafka.Message values for FetchMessage to return and
// inspect CommitMessages / Close calls afterward.
type fakeReader struct {
	fetchQueue []kafka.Message
	fetchIdx   int
	fetchErr   error // if non-nil, FetchMessage returns this immediately

	commits   []kafka.Message
	commitErr error // if non-nil, CommitMessages returns this

	closeCount int
	closeErr   error
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if f.fetchErr != nil {
		return kafka.Message{}, f.fetchErr
	}
	if f.fetchIdx >= len(f.fetchQueue) {
		// Block forever until the test's ctx is cancelled.
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
	msg := f.fetchQueue[f.fetchIdx]
	f.fetchIdx++
	return msg, nil
}

func (f *fakeReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	if f.commitErr != nil {
		return f.commitErr
	}
	f.commits = append(f.commits, msgs...)
	return nil
}

func (f *fakeReader) Close() error {
	f.closeCount++
	return f.closeErr
}

// newTestService wires a Service backed by a fakeReader and a logger
// writing to the returned buffer. Tests can assert on the buffer's
// contents to verify log lines.
func newTestService(t *testing.T, r reader, h Handler, retry RetryConfig, simulate bool) (*Service, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	log := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	svc, err := NewService(r, "test-consumer", h, retry, simulate, log)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, buf
}

// newTestEvent builds a JSON-encoded OrderCreated event for use in fake message Values.
func newTestEvent(t *testing.T, eventID string) []byte {
	t.Helper()
	ev, err := event.NewEvent(event.EventTypeOrderCreated, event.OrderCreatedPayload{OrderID: "ORD-001", Amount: 500000})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	// Override the auto-generated EventID so tests can assert on it.
	ev.EventID = eventID
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return raw
}

func TestNewService_RejectsEmptyConsumerID(t *testing.T) {
	_, err := NewService(&fakeReader{}, "", func(context.Context, event.Event) error { return nil }, RetryConfig{MaxAttempts: 1, Delay: time.Millisecond}, false, slog.Default())
	if err == nil {
		t.Fatal("expected error on empty consumerID, got nil")
	}
}

func TestNewService_RejectsNilHandler(t *testing.T) {
	_, err := NewService(&fakeReader{}, "id", nil, RetryConfig{MaxAttempts: 1, Delay: time.Millisecond}, false, slog.Default())
	if err == nil {
		t.Fatal("expected error on nil handler, got nil")
	}
}

func TestNewService_RejectsInvalidRetryConfig(t *testing.T) {
	_, err := NewService(&fakeReader{}, "id", func(context.Context, event.Event) error { return nil }, RetryConfig{MaxAttempts: 0, Delay: time.Millisecond}, false, slog.Default())
	if err == nil {
		t.Fatal("expected error on MaxAttempts < 1, got nil")
	}
}

// runService starts the Service in a goroutine, returns when ctx is cancelled
// (or Run returns an error), and returns the error.
func runService(t *testing.T, svc *Service, ctx context.Context) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(ctx)
	}()
	return done
}

func TestService_Run_HappyPath(t *testing.T) {
	raw := newTestEvent(t, "evt-happy")
	r := &fakeReader{fetchQueue: []kafka.Message{{Value: raw, Partition: 1, Offset: 42}}}
	called := 0
	h := func(ctx context.Context, ev event.Event) error {
		called++
		if ev.EventID != "evt-happy" {
			t.Errorf("EventID = %q, want evt-happy", ev.EventID)
		}
		return nil
	}
	svc, buf := newTestService(t, r, h, RetryConfig{MaxAttempts: 3, Delay: time.Millisecond}, false)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runService(t, svc, ctx)
	// Give the goroutine a moment to process the one queued message and reach FetchMessage block.
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if called != 1 {
		t.Errorf("handler call count = %d, want 1", called)
	}
	if len(r.commits) != 1 {
		t.Errorf("commits = %d, want 1", len(r.commits))
	}
	if r.commits[0].Offset != 42 {
		t.Errorf("committed offset = %d, want 42", r.commits[0].Offset)
	}
	logs := buf.String()
	if !strings.Contains(logs, "received event") {
		t.Errorf("logs missing 'received event': %s", logs)
	}
	if !strings.Contains(logs, "event processed") {
		t.Errorf("logs missing 'event processed': %s", logs)
	}
}

func TestService_Run_HandlerAlwaysFails(t *testing.T) {
	raw := newTestEvent(t, "evt-fail")
	// Two messages: the first always fails; the second is never reached
	// because after exhaustion we continue, but the test's second message
	// is a different one to verify we don't commit the failed one.
	raw2 := newTestEvent(t, "evt-ok")
	r := &fakeReader{fetchQueue: []kafka.Message{{Value: raw, Offset: 10}, {Value: raw2, Offset: 11}}}
	h := func(ctx context.Context, ev event.Event) error {
		if ev.EventID == "evt-fail" {
			return errors.New("forced failure")
		}
		return nil
	}
	svc, buf := newTestService(t, r, h, RetryConfig{MaxAttempts: 3, Delay: time.Millisecond}, false)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runService(t, svc, ctx)
	time.Sleep(50 * time.Millisecond) // allow both messages to process
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	// Only the second message should have been committed; the first's commit was skipped.
	if len(r.commits) != 1 {
		t.Fatalf("commits = %d, want 1 (failed message must not be committed)", len(r.commits))
	}
	if r.commits[0].Offset != 11 {
		t.Errorf("committed offset = %d, want 11 (the successful message)", r.commits[0].Offset)
	}
	if !strings.Contains(buf.String(), "max attempts exceeded") {
		t.Errorf("logs missing 'max attempts exceeded': %s", buf.String())
	}
}

func TestService_Run_SimulateFailure(t *testing.T) {
	raw := newTestEvent(t, "evt-sim")
	r := &fakeReader{fetchQueue: []kafka.Message{{Value: raw, Offset: 5}}}
	called := 0
	h := func(ctx context.Context, ev event.Event) error {
		called++
		return nil // always succeed when actually called
	}
	svc, buf := newTestService(t, r, h, RetryConfig{MaxAttempts: 3, Delay: time.Millisecond}, true)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runService(t, svc, ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	// Attempt 1 is simulated (handler NOT called); attempt 2 calls handler.
	if called != 1 {
		t.Errorf("handler call count = %d, want 1 (attempt 1 was simulated)", called)
	}
	if len(r.commits) != 1 {
		t.Errorf("commits = %d, want 1", len(r.commits))
	}
	if !strings.Contains(buf.String(), "simulated failure") {
		t.Errorf("logs missing 'simulated failure': %s", buf.String())
	}
}

func TestService_Run_FetchCanceledReturnsNil(t *testing.T) {
	r := &fakeReader{fetchErr: context.Canceled}
	svc, _ := newTestService(t, r, func(context.Context, event.Event) error { return nil }, RetryConfig{MaxAttempts: 1, Delay: time.Millisecond}, false)
	err := svc.Run(context.Background())
	if err != nil {
		t.Errorf("Run with FetchMessage=context.Canceled returned err = %v, want nil", err)
	}
}

func TestService_Run_FetchOtherErrorReturns(t *testing.T) {
	r := &fakeReader{fetchErr: errors.New("broker disconnected")}
	svc, _ := newTestService(t, r, func(context.Context, event.Event) error { return nil }, RetryConfig{MaxAttempts: 1, Delay: time.Millisecond}, false)
	err := svc.Run(context.Background())
	if err == nil {
		t.Fatal("Run with FetchMessage=other error returned nil, want error")
	}
	if !strings.Contains(err.Error(), "broker disconnected") {
		t.Errorf("error = %q, want it to wrap 'broker disconnected'", err.Error())
	}
}

func TestService_Run_MalformedMessageCommitSkipped(t *testing.T) {
	r := &fakeReader{fetchQueue: []kafka.Message{{Value: []byte("not json"), Offset: 7}}}
	called := 0
	h := func(ctx context.Context, ev event.Event) error {
		called++
		return nil
	}
	svc, buf := newTestService(t, r, h, RetryConfig{MaxAttempts: 1, Delay: time.Millisecond}, false)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runService(t, svc, ctx)
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if called != 0 {
		t.Errorf("handler call count = %d, want 0 (malformed message should not invoke handler)", called)
	}
	if len(r.commits) != 1 {
		t.Errorf("commits = %d, want 1 (poison-pill must be commit-skipped)", len(r.commits))
	}
	if !strings.Contains(buf.String(), "malformed event skipped") {
		t.Errorf("logs missing 'malformed event skipped': %s", buf.String())
	}
}

func TestService_Run_CommitErrorReturns(t *testing.T) {
	raw := newTestEvent(t, "evt-commit-fail")
	r := &fakeReader{fetchQueue: []kafka.Message{{Value: raw}}, commitErr: errors.New("commit failed")}
	h := func(ctx context.Context, ev event.Event) error { return nil }
	svc, buf := newTestService(t, r, h, RetryConfig{MaxAttempts: 1, Delay: time.Millisecond}, false)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runService(t, svc, ctx)
	time.Sleep(20 * time.Millisecond)
	cancel()
	err := <-errCh
	if err == nil {
		t.Fatal("Run returned nil, want error from CommitMessages")
	}
	if !strings.Contains(buf.String(), "commit failed") {
		t.Errorf("logs missing 'commit failed': %s", buf.String())
	}
}

func TestService_Run_DiscoversPartitionOnce(t *testing.T) {
	raw0 := newTestEvent(t, "evt-0a")
	raw1 := newTestEvent(t, "evt-1a")
	raw0again := newTestEvent(t, "evt-0b")
	r := &fakeReader{fetchQueue: []kafka.Message{
		{Value: raw0, Partition: 0, Offset: 0},
		{Value: raw1, Partition: 1, Offset: 0},
		{Value: raw0again, Partition: 0, Offset: 1},
	}}
	h := func(ctx context.Context, ev event.Event) error { return nil }
	svc, buf := newTestService(t, r, h, RetryConfig{MaxAttempts: 1, Delay: time.Millisecond}, false)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runService(t, svc, ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	logs := buf.String()
	if got := strings.Count(logs, "assigned partition"); got != 2 {
		t.Errorf("'assigned partition' count = %d, want 2 (one per unique partition)", got)
	}
	if !strings.Contains(logs, "assigned partition") || !strings.Contains(logs, "partition=0") {
		t.Errorf("logs missing 'assigned partition partition=0': %s", logs)
	}
	if !strings.Contains(logs, "partition=1") {
		t.Errorf("logs missing 'partition=1': %s", logs)
	}
}

func TestService_Run_DiscoveryIncludesConsumerID(t *testing.T) {
	raw := newTestEvent(t, "evt-disc")
	r := &fakeReader{fetchQueue: []kafka.Message{{Value: raw, Partition: 2, Offset: 7}}}
	h := func(ctx context.Context, ev event.Event) error { return nil }
	svc, buf := newTestService(t, r, h, RetryConfig{MaxAttempts: 1, Delay: time.Millisecond}, false)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := runService(t, svc, ctx)
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	logs := buf.String()
	if !strings.Contains(logs, "assigned partition") {
		t.Errorf("logs missing 'assigned partition': %s", logs)
	}
	if !strings.Contains(logs, "consumer_id=test-consumer") {
		t.Errorf("logs missing 'consumer_id=test-consumer': %s", logs)
	}
	if !strings.Contains(logs, "partition=2") {
		t.Errorf("logs missing 'partition=2': %s", logs)
	}
}
