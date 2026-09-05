package config

import (
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear all known env vars so defaults apply.
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("KAFKA_GROUP_ID", "")
	t.Setenv("SIMULATE_FAILURE", "")
	t.Setenv("RETRY_MAX_ATTEMPTS", "")
	t.Setenv("RETRY_DELAY", "")
	t.Setenv("PORT", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	if got, want := cfg.KafkaBrokers, []string{"localhost:9094"}; !equalSlices(got, want) {
		t.Errorf("KafkaBrokers = %v, want %v", got, want)
	}
	if cfg.KafkaTopic != "orders" {
		t.Errorf("KafkaTopic = %q, want %q", cfg.KafkaTopic, "orders")
	}
	if cfg.KafkaGroupID != "" {
		t.Errorf("KafkaGroupID = %q, want empty", cfg.KafkaGroupID)
	}
	if cfg.SimulateFailure {
		t.Error("SimulateFailure = true, want false")
	}
	if cfg.RetryMaxAttempts != 3 {
		t.Errorf("RetryMaxAttempts = %d, want 3", cfg.RetryMaxAttempts)
	}
	if cfg.RetryDelay != time.Second {
		t.Errorf("RetryDelay = %v, want 1s", cfg.RetryDelay)
	}
	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want 8080", cfg.Port)
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("KAFKA_TOPIC", "events")
	t.Setenv("KAFKA_GROUP_ID", "test-group")
	t.Setenv("SIMULATE_FAILURE", "true")
	t.Setenv("RETRY_MAX_ATTEMPTS", "5")
	t.Setenv("RETRY_DELAY", "2s")
	t.Setenv("PORT", "9090")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	wantBrokers := []string{"broker1:9092", "broker2:9092"}
	if !equalSlices(cfg.KafkaBrokers, wantBrokers) {
		t.Errorf("KafkaBrokers = %v, want %v", cfg.KafkaBrokers, wantBrokers)
	}
	if cfg.KafkaTopic != "events" {
		t.Errorf("KafkaTopic = %q, want %q", cfg.KafkaTopic, "events")
	}
	if cfg.KafkaGroupID != "test-group" {
		t.Errorf("KafkaGroupID = %q, want %q", cfg.KafkaGroupID, "test-group")
	}
	if !cfg.SimulateFailure {
		t.Error("SimulateFailure = false, want true")
	}
	if cfg.RetryMaxAttempts != 5 {
		t.Errorf("RetryMaxAttempts = %d, want 5", cfg.RetryMaxAttempts)
	}
	if cfg.RetryDelay != 2*time.Second {
		t.Errorf("RetryDelay = %v, want 2s", cfg.RetryDelay)
	}
	if cfg.Port != 9090 {
		t.Errorf("Port = %d, want 9090", cfg.Port)
	}
}

func TestLoad_CSVSplitTrimEmpty(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "a:1, b:2 ,, c:3")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() returned error: %v", err)
	}

	want := []string{"a:1", "b:2", "c:3"}
	if !equalSlices(cfg.KafkaBrokers, want) {
		t.Errorf("KafkaBrokers = %v, want %v", cfg.KafkaBrokers, want)
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
