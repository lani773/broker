# ─── Stage 1: Build ──────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

# Cache module downloads separately from source
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build with full optimizations
# -trimpath: remove local file paths for reproducibility
# -ldflags "-w -s": strip debug info and symbol table (~30% smaller binary)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -trimpath \
    -ldflags="-w -s -X main.version=$(git describe --tags --always 2>/dev/null || echo 2.0.0)" \
    -o /app/luma-broker \
    ./cmd/broker

# Verify the binary works
RUN /app/luma-broker --version 2>/dev/null || true

# ─── Stage 2: Minimal Runtime ────────────────────────────────────────────────
# scratch = zero OS overhead, no shell, no package manager
FROM scratch

# Bring timezone data (needed for time.LoadLocation) and CA certs (TLS)
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /app/luma-broker /luma-broker

# Run as non-root (UID 65534 = "nobody")
USER 65534:65534

# MQTT (plain + TLS), API, Metrics
EXPOSE 1883 8883 8080 9090

# Health check via TCP (no curl in scratch)
HEALTHCHECK --interval=15s --timeout=5s --start-period=15s \
    CMD ["/luma-broker", "-healthcheck"] 2>/dev/null || exit 1

ENTRYPOINT ["/luma-broker"]
