# docker/email-consumer.Dockerfile — multi-stage build for the Email Consumer.
#
# Build:   docker build -f docker/email-consumer.Dockerfile -t kafka-go-demo/email-consumer .
# Run:     docker run --rm \
#            -e KAFKA_BROKERS=kafka:9092 \
#            -e KAFKA_GROUP_ID=email-consumer-group \
#            kafka-go-demo/email-consumer

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build \
        -trimpath \
        -ldflags="-s -w" \
        -o /out/email-consumer \
        ./cmd/email-consumer

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/email-consumer /app/email-consumer
USER nonroot:nonroot
ENTRYPOINT ["/app/email-consumer"]
