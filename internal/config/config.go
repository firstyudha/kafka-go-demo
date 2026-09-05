// Package config loads demo configuration from environment variables.
// All env vars have defaults except KAFKA_GROUP_ID, which is set per-consumer-app.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the demo.
type Config struct {
	KafkaBrokers     []string
	KafkaTopic       string
	KafkaGroupID     string
	SimulateFailure  bool
	RetryMaxAttempts int
	RetryDelay       time.Duration
	Port             int
}

// Load reads configuration from the environment, applying defaults for any
// missing or empty values.
//
// KAFKA_BROKERS default: localhost:9094. The local docker-compose exposes
// Kafka on port 9094 (the EXTERNAL listener) for host clients and on port
// 9092 (the PLAINTEXT listener, advertised as kafka:9092) for in-network
// clients like kafka-ui. Connecting to localhost:9092 from the host
// triggers a metadata redirect to "kafka:9092" which the host can't
// resolve — so we go straight to the EXTERNAL listener instead. Set
// KAFKA_BROKERS=kafka:9092 when running the producer inside the same
// docker network.
func Load() (Config, error) {
	return Config{
		KafkaBrokers:     splitCSV(getEnv("KAFKA_BROKERS", "localhost:9094")),
		KafkaTopic:       getEnv("KAFKA_TOPIC", "orders"),
		KafkaGroupID:     os.Getenv("KAFKA_GROUP_ID"),
		SimulateFailure:  getEnv("SIMULATE_FAILURE", "false") == "true",
		RetryMaxAttempts: getEnvInt("RETRY_MAX_ATTEMPTS", 3),
		RetryDelay:       getEnvDuration("RETRY_DELAY", time.Second),
		Port:             getEnvInt("PORT", 8080),
	}, nil
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// splitCSV splits a comma-separated string, trimming whitespace and dropping
// empty entries.
func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
