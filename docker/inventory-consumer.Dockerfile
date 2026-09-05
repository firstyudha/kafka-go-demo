# docker/inventory-consumer.Dockerfile — multi-stage build for the Inventory Consumer.
#
# The --id flag is required at runtime (e.g. --id=inventory-1). For
# SIMULATE_FAILURE, set the env var to "true" before starting.
#
# Build:   docker build -f docker/inventory-consumer.Dockerfile -t kafka-go-demo/inventory-consumer .
# Run:     docker run --rm \
#            -e KAFKA_BROKERS=kafka:9092 \
#            -e KAFKA_GROUP_ID=inventory-consumer-group \
#            kafka-go-demo/inventory-consumer --id=inventory-1

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build \
        -trimpath \
        -ldflags="-s -w" \
        -o /out/inventory-consumer \
        ./cmd/inventory-consumer

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/inventory-consumer /app/inventory-consumer
USER nonroot:nonroot
ENTRYPOINT ["/app/inventory-consumer"]
