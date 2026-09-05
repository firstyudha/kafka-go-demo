package logger

import (
	"testing"
)

func TestNew_ReturnsNonNilLogger(t *testing.T) {
	l := New()
	if l == nil {
		t.Fatal("New() returned nil")
	}
}
