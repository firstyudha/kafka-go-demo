package kafkaclient

import (
	"testing"
)

func TestNewWriter_ReturnsNonNil(t *testing.T) {
	w := NewWriter([]string{"localhost:9092"}, "orders")
	if w == nil {
		t.Fatal("NewWriter returned nil")
	}
	defer w.Close()
}

func TestNewReader_ReturnsNonNil(t *testing.T) {
	r := NewReader([]string{"localhost:9092"}, "orders", "test-group")
	if r == nil {
		t.Fatal("NewReader returned nil")
	}
	defer r.Close()
}
