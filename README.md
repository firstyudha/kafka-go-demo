# Go Event-Driven Architecture Demo

A demonstration of an event-driven architecture in Go, using Apache Kafka as the broker.

The Producer publishes `OrderCreated` events to a Kafka topic. Two independent consumers (Email and Inventory) react to every event in their own consumer groups. A second Inventory instance shows horizontal scaling via partition distribution; a `SIMULATE_FAILURE` toggle demonstrates the retry path.

Every command in this README is a `make` target — run `make help` for the full list.

---

## What it demonstrates

- **Event envelope** — JSON envelope with `eventId`, `eventType`, `timestamp`, and `payload`.
- **Producer** — HTTP API + Kafka publisher. Knows nothing about consumers. Logs `event_id`, `event_type`, `topic`, `partition`, `offset` on publish.
- **Consumer** — shared read/deserialize/log/commit loop with a pluggable per-message handler.
- **Consumer groups** — Email (`email-consumer-group`) and Inventory (`inventory-consumer-group`) each receive every event independently.
- **Scaling** — Two Inventory instances sharing `inventory-consumer-group` have partitions distributed between them by Kafka.
- **Retry** — Fixed-delay retry loop; commit-after-success; `simulateFailure` toggle for the demo.
- **Graceful shutdown** — SIGINT/SIGTERM handling on all three binaries.

---

## Architecture

```
                    EVENT
                      │
                      ▼
┌───────────┐    ┌─────────┐    ┌──────────────┐
│  Producer │───>│  Kafka  │───>│   Consumer   │
└───────────┘    └────┬────┘    └──────────────┘
                      │
                      └────────>┌──────────────┐
                                │   Consumer   │
                                └──────────────┘
```

Topic `orders` is configured with **3 partitions**, **replication factor 1**, running in **KRaft mode**. The Producer creates the topic on first publish.

---

## Prerequisites

- **Docker Desktop** (or Docker Engine on Linux).
- **Go 1.22+** (only needed for Make/host mode — Docker mode builds inside containers).
- Free ports: **8080** (producer HTTP), **8081** (kafka-ui), **9092/9094** (Kafka listeners).
- On Windows: WSL2 or Git Bash for shell scripts.

---

## Quick start


```bash
# Make (host) mode — fast iteration, native logs
make infra-up && make demo-scale    # Kafka + 4 background processes
tail -f .run/inventory-consumer-1.log
make demo-stop && make infra-down

# Docker mode — single-command end-to-end, no Go on the host
make up                              # builds images, starts everything
make logs
make down
```

---

## Webinar Demo

Two modes, same talking points. Docker mode is more reliable on stage; Make mode is best when showing a code change live.

### 1. Make mode

#### 1.1 — Start Kafka

```bash
make infra-up
make ps        # wait for kafka + kafka-ui to show "(healthy)"
```

kafka-ui is at **http://localhost:8081** — use it during the talk to peek at the `orders` topic.

#### 1.2 — Start the apps


| Terminal | Command | First log line you should see |
|---|---|---|
| **T1** producer | `make producer` | `INFO producer ready port=8080 brokers=localhost:9094 topic=orders` |
| **T2** email | `make email-consumer` | `INFO consumer ready consumer_id=email-1 group_id=email-consumer-group` |
| **T3** inventory-1 | `make inventory-consumer` | `INFO consumer ready ...` followed by **3×** `assigned partition consumer_id=inventory-1 partition=N` |

(With one consumer in the group, it owns all 3 partitions — that's why you see three `assigned partition` lines.)

#### 1.3 — Submit an order

```bash
# macOS / Linux / Git Bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORD-001","amount":500000}'
```
```powershell
# PowerShell (Windows)
Invoke-RestMethod -Method POST -Uri http://localhost:8080/api/orders `
  -ContentType "application/json" `
  -Body '{"orderId":"ORD-001","amount":500000}'
```

Or open **http://localhost:8080** in a browser and use the form. Expected response:

```json
{"eventId":"evt-7z3k9m1q4r2x8","status":"published"}
```

#### 1.4 — Run the 4 phases

**Phase A — Producer alone (~1 min)**

T1 only. Submit one order. Show T1's log line:

```
INFO order created  order_id=ORD-001 amount=500000
INFO event published event_id=evt-7z3k9m1q4r2x8 event_type=OrderCreated
                    topic=orders partition=1 offset=0
```

Open **http://localhost:8081** → topic `orders` → partition 1 → see the message body. This is the "see it in the broker" moment.

> **Talking point:** *"The producer published one event. It returned an `eventId`, and the log line tells me it landed in `partition=1 offset=0`. Producer doesn't know who's listening."*

**Phase B — Two consumers (~1 min)**

Start T2 and T3. Submit two more orders (`ORD-002`, `ORD-003`).

Watch T2 and T3 receive the same `event_id` simultaneously.

> **Talking point:** *"Same event, two different consumer groups, two different `consumer_id`s. Each group gets every event — that's the independence guarantee."*

**Phase C — Scaling (T4) (~2 min)**

```bash
# T4
make inventory-consumer INVENTORY_ID=inventory-2
```

Kafka rebalances. Watch the `assigned partition` log lines redistribute the 3 partitions between `inventory-1` and `inventory-2` (2/1 split is typical).

Submit 3 more orders. Each one goes to **exactly one** of the two inventory consumers.

> **Talking point:** *"Same group, two instances, partitions get split. Notice email still gets every event — it's in its own group."*

**Phase D — Retry with `SIMULATE_FAILURE` (~1 min)**

Ctrl-C `inventory-2` (T4). Restart it with the failure flag:

```bash
# T4
SIMULATE_FAILURE=true make inventory-consumer INVENTORY_ID=inventory-2
```

Submit one more order. Expected log in T4:

```
INFO  received event          consumer_id=inventory-2 event_id=evt-... offset=N
INFO  processing attempt      consumer_id=inventory-2 attempt=1
ERROR processing failed       consumer_id=inventory-2 attempt=1 err="simulated failure"
                              (1s delay)
INFO  processing attempt      consumer_id=inventory-2 attempt=2
INFO  processing succeeded     consumer_id=inventory-2 attempt=2
INFO  event processed          consumer_id=inventory-2 ... offset=N
```

> **Talking point:** *"Attempt 1 fails. The offset is NOT committed. We sleep, retry, succeed. Only now do we commit. At-least-once delivery, bounded delay, no framework."*

#### 1.5 — Tear down

```bash
# Ctrl-C in every terminal
make demo-stop        # if you used demo-scale instead of terminals
make infra-down       # stops Docker + removes kafka_data volume
```

### 2. Docker mode

#### 2.1 — Build + start

```bash
make up               # ≈60–90s the first time (mostly image build)
make ps               # wait for all services to show "(healthy)" / "(running)"
```

#### 2.2 — Tail the logs

```bash
make logs                          # all app services
make logs-service SERVICE=producer # just one service
```

kafka-ui is at **http://localhost:8081** for browsing the broker.

#### 2.3 — Submit an order

Same as [§1.3](#13--submit-an-order) — the producer is exposed on `localhost:8080` from the host.

#### 2.4 — Run the 4 phases

**Phase A** and **Phase B** are identical to [§1.4](#14--run-the-4-phases).

**Phase C — Scaling:**

```bash
make up-scale        # starts inventory-consumer-2 in the scale profile
make logs | grep "assigned partition"   # watch the 3 partitions split 2/1
```

**Phase D — Retry:**

```bash
make up-scale-failure # restarts inventory-consumer-2 with SIMULATE_FAILURE=true
make logs | grep -E "inventory-consumer-2|attempt|simulated"
```

#### 2.5 — Tear down

```bash
make down       # stop everything (keeps kafka_data volume)
make down-v     # stop AND remove the volume (full reset)
```

---

## Project layout

```
cmd/
  producer/             HTTP server + Kafka publisher + web UI
  email-consumer/       Subscribes via email-consumer-group
  inventory-consumer/   Subscribes via inventory-consumer-group; --id flag

internal/
  event/                Event envelope + OrderCreated payload + Decode
  config/               Env-var loading (KAFKA_BROKERS, KAFKA_TOPIC, etc.)
  kafka/                Factory wrapping segmentio/kafka-go (package kafkaclient)
  logger/               slog text-handler wrapper
  httpserver/           HTTP routing for the Producer web UI
  producer/             Producer business logic + validation
  consumer/             Shared Service + Run loop + processWithRetry
  processor/            Per-consumer side-effect handlers (Email, Inventory)

web/                    HTML templates + static assets for the producer UI

docker-compose.yml      Kafka (KRaft) + kafka-ui
Makefile                Reference targets (infra-up/down, individual binaries, demo-scale/stop)
```
