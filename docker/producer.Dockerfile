# docker/producer.Dockerfile — multi-stage build for the Producer binary.
# Static assets (HTML/CSS/JS) are embedded into the binary via //go:embed,
# so the runtime image only needs the compiled binary.
#
# Build:   docker build -f docker/producer.Dockerfile -t kafka-go-demo/producer .
# Run:     docker run --rm -p 8080:8080 \
#            -e KAFKA_BROKERS=kafka:9092 \
#            kafka-go-demo/producer

# ---- build stage ----
FROM golang:1.22-alpine AS build
WORKDIR /src

# Cache go.mod/go.sum first so dependency download is skipped on code-only changes.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source.
COPY . .

# CGO=0 + static binary so it runs on distroless. -trimpath strips build paths.
# -ldflags="-s -w" strips debug info to shrink the binary.
RUN CGO_ENABLED=0 go build \
        -trimpath \
        -ldflags="-s -w" \
        -o /out/producer \
        ./cmd/producer

# ---- runtime stage ----
# Distroless: no shell, no package manager, no busybox — minimal attack surface.
# nonroot user because the demo's HTTP server binds to :8080, no privilege needed.
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/producer /app/producer
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app/producer"]
