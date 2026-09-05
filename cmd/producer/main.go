// Command producer is the Go event-driven architecture demo's HTTP front-end.
// It serves the order form and publishes OrderCreated events to Kafka.
//
// Usage:
//
//	KAFKA_BROKERS=localhost:9094 KAFKA_TOPIC=orders PORT=8080 \
//	  go run ./cmd/producer
//
// The default KAFKA_BROKERS points at the EXTERNAL listener (port 9094) of
// the local docker-compose stack, which advertises localhost:9094 to host
// clients. Connecting to localhost:9092 (the PLAINTEXT listener) triggers a
// metadata redirect to "kafka:9092" that the host cannot resolve. When
// running the producer inside the docker network, set KAFKA_BROKERS=kafka:9092.
//
// Graceful shutdown on SIGINT/SIGTERM: HTTP server stops accepting requests
// (drains in-flight ones), then the Kafka writer is closed.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafka-go-demo/internal/config"
	"kafka-go-demo/internal/httpserver"
	kafkaclient "kafka-go-demo/internal/kafka"
	"kafka-go-demo/internal/logger"
	"kafka-go-demo/internal/producer"
	"kafka-go-demo/internal/web"
)

func main() {
	log := logger.New()

	cfg, err := config.Load()
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Ensure the topic exists. Idempotent; fail-fast if Kafka is unreachable.
	if err := kafkaclient.EnsureTopic(ctx, cfg.KafkaBrokers, cfg.KafkaTopic, 3); err != nil {
		log.Error("ensure topic failed", "err", err, "brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic)
		os.Exit(1)
	}

	writer := kafkaclient.NewWriter(cfg.KafkaBrokers, cfg.KafkaTopic)
	prod := producer.New(writer, cfg.KafkaTopic, log)
	srv := httpserver.New(cfg.Port, prod, web.StaticFS(), log)

	// Serve HTTP in a goroutine so we can block on the signal below.
	go func() {
		if err := srv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			stop()
		}
	}()

	log.Info("producer ready",
		"port", cfg.Port,
		"brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
	)

	<-ctx.Done()
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown failed", "err", err)
	}
	if err := writer.Close(); err != nil {
		log.Error("writer close failed", "err", err)
	}

	log.Info("shutdown complete")
}
