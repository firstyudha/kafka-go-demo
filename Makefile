.PHONY: help infra-up infra-down up up-scale up-scale-failure down down-v logs logs-service ps build-images test tidy fmt vet build producer email-consumer inventory-consumer demo-scale demo-stop kafka-tail kafka-groups kafka-describe kafka-topics webinar webinar-html webinar-pdf webinar-pptx install-marp

# ---- help ----
help: ## Show this help
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---- infrastructure (host mode) ----
# Host mode = Kafka in Docker, Go apps on the host via `go run`.
# Use this when you want fast code iteration, native logs, or individual process control.

infra-up: ## Start Kafka + kafka-ui in Docker
	docker compose up -d

infra-down: ## Stop Kafka + kafka-ui (also removes the kafka_data volume)
	docker compose down -v

# ---- full Docker stack ----
# Docker mode = everything in containers. Single-command end-to-end.

up: ## Build + start the full stack (kafka + kafka-ui + producer + email + inventory-1)
	docker compose up -d --build
	@echo ""
	@echo "Stack up. Submit an order:"
	@echo "  curl -X POST http://localhost:8080/api/orders \\"
	@echo "    -H 'Content-Type: application/json' \\"
	@echo "    -d '{\"orderId\":\"ORD-001\",\"amount\":500000}'"
	@echo ""
	@echo "Open kafka-ui at http://localhost:8081"
	@echo "Watch logs: make logs"
	@echo "Stop:        make down"

up-scale: ## Start the second inventory consumer (Phase 4 — scaling demo)
	docker compose --profile scale up -d inventory-consumer-2
	@echo "inventory-consumer-2 up. Submit orders; watch 'assigned partition' lines."

up-scale-failure: ## Start inventory-consumer-2 with SIMULATE_FAILURE=true (Phase 5 — retry demo)
	docker compose --profile scale up -d -e SIMULATE_FAILURE=true inventory-consumer-2
	@echo "inventory-consumer-2 up with SIMULATE_FAILURE=true. Submit an order; tail with: make logs | grep inventory-consumer-2"

down: ## Stop the full stack (keeps kafka_data volume)
	docker compose down

down-v: ## Stop the full stack AND remove the kafka_data volume
	docker compose down -v

logs: ## Tail logs from all app services
	docker compose logs -f producer email-consumer inventory-consumer-1 inventory-consumer-2

logs-service: ## Tail logs from a single service (SERVICE=producer)
	docker compose logs -f $(SERVICE)

ps: ## Show docker compose service status
	docker compose ps

build-images: ## Build only the 3 Go app images (no start)
	docker compose build producer email-consumer inventory-consumer-1

# ---- code quality ----

test: ## go test ./...
	go test ./...

tidy: ## go mod tidy
	go mod tidy

fmt: ## gofmt -w .
	gofmt -w .

vet: ## go vet ./...
	go vet ./...

build: ## go build ./...
	go build ./...

# ---- host-mode binaries (require infra-up first) ----

producer: ## Run the Producer on the host (port 8080)
	go run ./cmd/producer

email-consumer: ## Run the Email Consumer on the host
	go run ./cmd/email-consumer

# --id is required (e.g. --id=inventory-1) for the multi-instance demo.
# Override --id with INVENTORY_ID=inventory-2 for the scaling demo.
# Set SIMULATE_FAILURE=true in the environment to enable the retry path.
inventory-consumer: ## Run an Inventory Consumer (INVENTORY_ID=inventory-2 to override; SIMULATE_FAILURE=true for retry)
	go run ./cmd/inventory-consumer --id=$(or $(INVENTORY_ID),inventory-1)

# ---- host-mode background demo (4 processes) ----

demo-scale: build ## Start 1 producer + 1 email + 2 inventory consumers in the background
	mkdir -p .run
	bash -c 'go run ./cmd/producer > .run/producer.log 2>&1 & echo $$! > .run/producer.pid'
	bash -c 'go run ./cmd/email-consumer > .run/email-consumer.log 2>&1 & echo $$! > .run/email-consumer.pid'
	bash -c 'go run ./cmd/inventory-consumer --id=inventory-1 > .run/inventory-consumer-1.log 2>&1 & echo $$! > .run/inventory-consumer-1.pid'
	bash -c 'go run ./cmd/inventory-consumer --id=inventory-2 > .run/inventory-consumer-2.log 2>&1 & echo $$! > .run/inventory-consumer-2.pid'
	@echo "demo-scale: 4 processes started. PIDs in .run/*.pid, logs in .run/*.log"
	@echo "tail -f .run/inventory-consumer-1.log to watch"
	@echo "make demo-stop to terminate all 4"

demo-stop: ## Terminate all processes started by demo-scale
	@for f in .run/*.pid; do \
		if [ -f "$$f" ]; then \
			pid=$$(cat "$$f"); \
			kill $$pid 2>/dev/null || true; \
			rm -f "$$f"; \
		fi; \
	done
	@echo "demo-stop: signals sent"

# ---- kafka introspection (run inside the broker container) ----

kafka-tail: ## Tail messages on the orders topic from inside the broker (Ctrl-C to stop)
	docker compose exec kafka kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic orders --from-beginning

kafka-groups: ## List consumer groups
	docker compose exec kafka kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 --list

kafka-describe: ## Describe a consumer group (override with GROUP=...; default inventory-consumer-group)
	docker compose exec kafka kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 \
		--group $(or $(GROUP),inventory-consumer-group) --describe

kafka-topics: ## List topics + partitions
	docker compose exec kafka kafka-topics.sh \
		--bootstrap-server localhost:9092 --list
