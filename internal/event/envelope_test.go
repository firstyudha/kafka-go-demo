package event

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type samplePayload struct {
	Foo string `json:"foo"`
	Bar int    `json:"bar"`
}

func TestNewEvent_FillsEnvelopeFields(t *testing.T) {
	before := time.Now().UTC()
	e, err := NewEvent("OrderCreated", samplePayload{Foo: "hello", Bar: 42})
	if err != nil {
		t.Fatalf("NewEvent error: %v", err)
	}
	after := time.Now().UTC()

	if e.EventType != "OrderCreated" {
		t.Errorf("EventType = %q, want OrderCreated", e.EventType)
	}
	if !strings.HasPrefix(e.EventID, "evt-") {
		t.Errorf("EventID = %q, want evt- prefix", e.EventID)
	}
	if e.Timestamp.Location() != time.UTC {
		t.Errorf("Timestamp location = %v, want UTC", e.Timestamp.Location())
	}
	if e.Timestamp.Before(before) || e.Timestamp.After(after) {
		t.Errorf("Timestamp = %v, want between %v and %v", e.Timestamp, before, after)
	}
	if len(e.Payload) == 0 {
		t.Error("Payload is empty")
	}
}

func TestNewEvent_UniqueEventIDs(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		e, err := NewEvent("OrderCreated", samplePayload{})
		if err != nil {
			t.Fatalf("NewEvent error: %v", err)
		}
		if seen[e.EventID] {
			t.Fatalf("duplicate EventID %q after %d iterations", e.EventID, i)
		}
		seen[e.EventID] = true
	}
}

func TestEvent_RoundTripPreservesPayload(t *testing.T) {
	orig, err := NewEvent("OrderCreated", samplePayload{Foo: "x", Bar: 7})
	if err != nil {
		t.Fatalf("NewEvent error: %v", err)
	}

	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var got Event
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if got.EventID != orig.EventID {
		t.Errorf("EventID round-trip mismatch: %q vs %q", got.EventID, orig.EventID)
	}
	if got.EventType != orig.EventType {
		t.Errorf("EventType round-trip mismatch: %q vs %q", got.EventType, orig.EventType)
	}

	var gotPayload samplePayload
	if err := json.Unmarshal(got.Payload, &gotPayload); err != nil {
		t.Fatalf("Payload unmarshal error: %v", err)
	}
	if gotPayload != (samplePayload{Foo: "x", Bar: 7}) {
		t.Errorf("Payload round-trip mismatch: %+v", gotPayload)
	}
}

func TestEvent_GeneratedIDFormat(t *testing.T) {
	e, err := NewEvent("X", samplePayload{})
	if err != nil {
		t.Fatalf("NewEvent error: %v", err)
	}
	suffix := strings.TrimPrefix(e.EventID, "evt-")
	if len(suffix) != 13 {
		t.Errorf("EventID suffix length = %d, want 13 (full id: %q)", len(suffix), e.EventID)
	}
	for _, r := range suffix {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'z')) {
			t.Errorf("EventID suffix contains non-base36 char %q (full id: %q)", r, e.EventID)
			break
		}
	}
}

func TestEvent_Decode(t *testing.T) {
	orig, err := NewEvent("OrderCreated", samplePayload{Foo: "hello", Bar: 42})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}

	raw, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if got.EventID != orig.EventID {
		t.Errorf("EventID = %q, want %q", got.EventID, orig.EventID)
	}
	if got.EventType != orig.EventType {
		t.Errorf("EventType = %q, want %q", got.EventType, orig.EventType)
	}
	if !got.Timestamp.Equal(orig.Timestamp) {
		t.Errorf("Timestamp = %v, want %v", got.Timestamp, orig.Timestamp)
	}
	if string(got.Payload) != string(orig.Payload) {
		t.Errorf("Payload = %s, want %s", got.Payload, orig.Payload)
	}
}

func TestEvent_Decode_MalformedJSON(t *testing.T) {
	_, err := Decode([]byte("not valid json"))
	if err == nil {
		t.Fatal("expected error on malformed JSON, got nil")
	}
}
