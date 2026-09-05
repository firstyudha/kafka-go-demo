// Command email-consumer is the demo's email-side Kafka consumer. It
// subscribes to the orders topic on the email-consumer-group and logs
// a simulated email confirmation for every OrderCreated event.
//
// Usage:
//
//	KAFKA_BROKERS=localhost:9094 KAFKA_TOPIC=orders \
//	  KAFKA_GROUP_ID=email-consumer-group go run ./cmd/email-consumer
//
// --id overrides the instance ID (default 'email-1'). Per-instance IDs
// matter in Phase 4 when two inventory consumers run side-by-side; the
// email consumer typically uses the default.
//
// Graceful shutdown on SIGINT/SIGTERM: the consumer loop exits, then
// the Kafka reader is closed.
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
		cfg.KafkaGroupID = "email-consumer-group"
	}

	id := flag.String("id", "email-1", "consumer instance ID (default 'email-1')")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reader := kafkaclient.NewReader(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
	svc, err := consumer.NewService(
		reader, *id,
		processor.EmailHandler(log),
		consumer.RetryConfig{MaxAttempts: cfg.RetryMaxAttempts, Delay: cfg.RetryDelay},
		false, // simulateFailure — email never simulates
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
