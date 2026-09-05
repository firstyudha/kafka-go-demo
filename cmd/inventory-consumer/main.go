// Command inventory-consumer is the demo's inventory-side Kafka consumer.
// It subscribes to the orders topic on the inventory-consumer-group and
// logs a simulated inventory reservation for every OrderCreated event.
//
// Usage:
//
//	KAFKA_BROKERS=localhost:9094 KAFKA_TOPIC=orders \
//	  KAFKA_GROUP_ID=inventory-consumer-group \
//	  go run ./cmd/inventory-consumer --id=inventory-1
//
// --id is REQUIRED (no default) so multi-instance demos (Phase 4) can
// distinguish inventory-1 from inventory-2 in the logs.
//
// SIMULATE_FAILURE=true triggers the retry path: every event's first
// attempt fails, second succeeds. Used in Phase 5's retry demo.
//
// Graceful shutdown on SIGINT/SIGTERM.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"kafka-go-demo/internal/config"
	"kafka-go-demo/internal/consumer"
	kafkaclient "kafka-go-demo/internal/kafka"
	"kafka-go-demo/internal/logger"
	"kafka-go-demo/internal/processor"
)

func main() {
	log := logger.New()

	cfg, err := config.Load()
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}
	if cfg.KafkaGroupID == "" {
		cfg.KafkaGroupID = "inventory-consumer-group"
	}

	id := flag.String("id", "", "consumer instance ID (required, e.g. 'inventory-1')")
	flag.Parse()
	if *id == "" {
		log.Error("--id is required for inventory consumer (e.g. --id=inventory-1)")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reader := kafkaclient.NewReader(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	svc, err := consumer.NewService(
		reader, *id,
		processor.InventoryHandler(log),
		consumer.RetryConfig{MaxAttempts: cfg.RetryMaxAttempts, Delay: cfg.RetryDelay},
		cfg.SimulateFailure, // from SIMULATE_FAILURE env
		log,
	)
	if err != nil {
		log.Error("new service", "err", err)
		os.Exit(1)
	}

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		if rerr := svc.Run(ctx); rerr != nil {
			log.Error("consumer loop failed", "err", rerr)
			stop()
		}
	}()

	log.Info("consumer ready",
		"consumer_id", *id,
		"group_id", cfg.KafkaGroupID,
		"brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"simulate_failure", cfg.SimulateFailure,
	)

	<-ctx.Done()
	log.Info("shutdown signal received")
	<-runDone
	log.Info("stopping consumer")

	if err := reader.Close(); err != nil {
		log.Error("reader close failed", "err", err)
	}
	log.Info("shutdown complete")
}
