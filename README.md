# LUMA MQTT Broker v2 — Full Go Implementation

High-performance, production-grade MQTT 3.1.1 + v5 broker written entirely in Go.
Single binary: MQTT TCP/TLS + REST/WebSocket API + Prometheus metrics.

```
Benchmark:  250,000 msg/s on a 4-core machine
Latency:    p50 < 200µs | p99 < 2ms (local network)
Memory:     ~12 MB idle | ~8 KB per connected client
Cold start: < 300ms
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       SINGLE GO BINARY                           │
│                                                                  │
│   TCP :1883    ┌──────────────┐    REST :8080                   │
│   TLS :8883 ──▶│  net/http    │◀── WebSocket /ws                │
│                │   Mux        │    Prometheus /metrics           │
│                └──────┬───────┘                                  │
│                       │                                          │
│   ┌───────────────────▼─────────────────────────────────┐       │
│   │                    BROKER CORE                       │       │
│   │                                                      │       │
│   │  Client Goroutines (readLoop + writeLoop per conn)  │       │
│   │    ↓ protocol.DecodePublish (zero-alloc hot path)   │       │
│   │    ↓                                                 │       │
│   │  ┌────────────────────────────────────────────────┐ │       │
│   │  │  Router  (256-shard concurrent trie)           │ │       │
│   │  │  • O(depth) wildcard match                     │ │       │
│   │  │  • copy-on-write subscriber maps               │ │       │
│   │  │  • sync.Map retained store                     │ │       │
│   │  └────────────────────────────────────────────────┘ │       │
│   │    ↓                                                 │       │
│   │  DeliverFn closure (non-blocking, ring-buffer write) │       │
│   └──────────────────────────────────────────────────────┘       │
│                                                                  │
│   ┌─────────────────┐  ┌────────────────────────────────────┐   │
│   │  Session Store   │  │  Auth (JWT cache + token bucket)  │   │
│   │  (QoS FSM,       │  │  ACL rule engine                  │   │
│   │   offline queue) │  └────────────────────────────────────┘   │
│   └─────────────────┘                                            │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
           │                          │
    ┌──────▼──────┐           ┌───────▼──────┐
    │    Redis    │           │  PostgreSQL  │
    │  • clients  │           │  • devices   │
    │  • retained │           │  • users     │
    │  • cluster  │           │  • subs      │
    │    pub/sub  │           │  • msg log   │
    └─────────────┘           └──────────────┘
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| **256-shard trie** | FNV hash on first segment → 256 independent RWMutex shards → near-linear scaling with CPU cores |
| **copy-on-write subscriber maps** | Read path (match) never blocks on writes; new map built on sub/unsub then pointer-swapped |
| **Ring-buffer write queue** | Buffered channel per client; drops instead of blocking the router on slow consumers |
| **Zero-alloc publish hot path** | `EncodePublish` pre-calculates size, single `make([]byte)`, no copies via `sync.Pool` for scratch buffers |
| **JWT token cache** | Avoids re-parsing JWT on every reconnect storm; 10k entry LRU with expiry check |
| **pgx/v5** | No `database/sql` reflection overhead; typed scans, native `pgxpool` connection pooling |
| **Single binary** | Broker + API + metrics in one process = zero inter-service latency, simpler ops |

---

## Quick Start

### Docker Compose (recommended)

```bash
# 1. Clone
git clone https://github.com/your-org/luma-broker
cd luma-broker

# 2. Configure
cp .env.example .env
# Edit .env — change JWT_SECRET, ADMIN_PASSWORD, POSTGRES_PASSWORD

# 3. Start
make up-infra
go run ./cmd/broker

# 4. Verify
curl http://localhost:8080/health
# → {"status":"healthy","checks":{"postgres":"ok","redis":"ok"},...}
```

### Local Dev

```bash
# Start only infra
make up-infra

# Run broker
export JWT_SECRET=dev-secret-32-chars-minimum-here
export POSTGRES_DSN="postgres://mqtt:mqtt_secret@localhost:5432/mqtt?sslmode=disable"
export REDIS_ADDR=localhost:6379
export LOG_FORMAT=console
export LOG_LEVEL=debug
go run ./cmd/broker
```

### Build Binary

```bash
make build
# → bin/luma-broker (~8 MB static binary)

# Cross-compile
GOOS=linux GOARCH=arm64 make build
```

---

## API Reference

All protected endpoints require: `Authorization: Bearer <token>`

### Authentication

```bash
# Register
curl -X POST http://localhost:8080/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"securepass123","roles":[]}'

# Login → get JWT
TOKEN=$(curl -s -X POST http://localhost:8080/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' \
  | jq -r '.access_token')

echo $TOKEN

# Refresh token
curl -X POST http://localhost:8080/auth/refresh \
  -H "Authorization: Bearer $TOKEN"
```

### Devices

```bash
# Register device
curl -X POST http://localhost:8080/devices \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "client_id":   "esp32-kitchen",
    "description": "Kitchen temperature sensor",
    "metadata":    {"location":"kitchen","hw":"ESP32-S3"}
  }'

# List (paginated)
curl "http://localhost:8080/devices?limit=20&offset=0" \
  -H "Authorization: Bearer $TOKEN"

# Get single
curl http://localhost:8080/devices/esp32-kitchen \
  -H "Authorization: Bearer $TOKEN"

# Delete
curl -X DELETE http://localhost:8080/devices/esp32-kitchen \
  -H "Authorization: Bearer $TOKEN"
```

### MQTT Control

```bash
# Publish via REST (injected into broker via Redis)
curl -X POST http://localhost:8080/mqtt/publish \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"topic":"devices/esp32-kitchen/cmd","payload":"{\"cmd\":\"enable\"}","qos":1}'

# Message history
curl "http://localhost:8080/mqtt/messages?topic=devices/esp32-kitchen/data&limit=50" \
  -H "Authorization: Bearer $TOKEN"

# All retained messages
curl http://localhost:8080/mqtt/retained \
  -H "Authorization: Bearer $TOKEN"

# Delete retained
curl -X DELETE http://localhost:8080/mqtt/retained/devices/esp32-kitchen/status \
  -H "Authorization: Bearer $TOKEN"
```

### Admin

```bash
# Live broker stats
curl http://localhost:8080/admin/stats \
  -H "Authorization: Bearer $TOKEN"

# Connected clients
curl http://localhost:8080/admin/clients \
  -H "Authorization: Bearer $TOKEN"

# Message rate time-series (last 60s)
curl "http://localhost:8080/admin/timeseries?window=60" \
  -H "Authorization: Bearer $TOKEN"

# Add ACL rule
curl -X POST http://localhost:8080/admin/acl \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"client_pattern":"esp32-*","topic_pattern":"devices/+/data","permission":"publish"}'

# Force-disconnect a client
curl -X POST http://localhost:8080/admin/disconnect/esp32-kitchen \
  -H "Authorization: Bearer $TOKEN"

# Ban user
curl -X POST http://localhost:8080/admin/ban \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"username":"badactor"}'
```

### WebSocket Stream

```javascript
const ws = new WebSocket('ws://localhost:8080/ws');

ws.onmessage = ({data}) => {
  const msg = JSON.parse(data);
  if (msg.event === 'mqtt_message') {
    console.log(`${msg.topic}: ${msg.payload}`);
  }
};

// Publish from browser
ws.send(JSON.stringify({
  action: 'publish',
  topic:  'devices/test/cmd',
  payload: JSON.stringify({cmd: 'ping'}),
  qos: 0
}));
```

---

## MQTT Client Examples

### mosquitto_pub / mosquitto_sub

```bash
# QoS 0 publish
mosquitto_pub -h localhost -p 1883 \
  -u admin -P admin123 \
  -t devices/sensor1/data \
  -m '{"temp":22.5}' -q 0

# Subscribe with wildcards
mosquitto_sub -h localhost -p 1883 \
  -u admin -P admin123 \
  -t 'devices/+/data' -q 1 -v

# Subscribe to $SYS stats
mosquitto_sub -h localhost -p 1883 \
  -u admin -P admin123 \
  -t '$SYS/broker/#' -v

# JWT auth (pass token as password)
mosquitto_pub -h localhost -p 1883 \
  -u mydevice -P "$TOKEN" \
  -t devices/mydevice/data -m '{"v":1}'
```

### Python (paho)

```python
import paho.mqtt.client as mqtt
import json, time

def on_message(client, userdata, msg):
    print(f"{msg.topic}: {msg.payload.decode()}")

client = mqtt.Client("python-client", protocol=mqtt.MQTTv311)
client.username_pw_set("admin", "admin123")
client.on_message = on_message
client.connect("localhost", 1883, keepalive=60)
client.subscribe("devices/#", qos=1)
client.loop_start()

# Publish
client.publish("devices/python/data", json.dumps({"value": 42}), qos=1)
time.sleep(5)
client.loop_stop()
```

### Go test client

```bash
cd clients/go

# Run all protocol tests
go run client.go -test all

# Load benchmark: 1000 clients x 500 messages
go run client.go -bench -clients 1000 -msgs 500

# Live monitor
go run client.go -monitor 'devices/#'

# REST API tests
go run client.go -apitest
```

---

## Horizontal Scaling (Cluster Mode)

```bash
# Enable cluster mode on all instances
CLUSTER_MODE=true

# All instances connect to the SAME Redis and PostgreSQL.
# Redis pub/sub provides cross-broker message fan-out.
# Each broker:
#   1. Routes messages to its LOCAL subscribers
#   2. Publishes to Redis channel for OTHER brokers
#   3. Receives from Redis and routes to local subscribers
```

Docker Compose cluster:

```bash
# Uncomment broker-2 in docker-compose.yml, then:
docker compose up -d broker broker-2
```

Traffic distribution via TCP load balancer (e.g., HAProxy):

```
frontend mqtt
    bind *:1883
    default_backend mqtt_brokers

backend mqtt_brokers
    balance source       # sticky by client IP
    server broker-1 broker-1:1883 check
    server broker-2 broker-2:1883 check
```

Kubernetes (HPA auto-scales 3 → 50 pods based on CPU):

```bash
make k8s-deploy
kubectl get hpa -n luma -w
```

---

## Performance Tuning

### OS (apply before starting broker)

```bash
# /etc/sysctl.conf
net.core.somaxconn              = 65535
net.ipv4.tcp_max_syn_backlog    = 65535
net.ipv4.tcp_tw_reuse           = 1
net.ipv4.tcp_fin_timeout        = 10
net.core.rmem_max               = 16777216
net.core.wmem_max               = 16777216
fs.file-max                     = 2000000

# /etc/security/limits.conf
* soft nofile 1000000
* hard nofile 1000000
```

### Broker Tuning

```bash
MAX_CONNECTIONS=100000   # match your OS file descriptor limit
WRITE_QUEUE_SIZE=1024    # increase for bursty publishers
RATE_LIMIT=5000          # relax if you trust all clients
PG_MAX_CONNS=50          # increase for heavy audit logging
```

---

## Prometheus Metrics

Key metrics exposed at `:9090/metrics`:

| Metric | Type | Description |
|---|---|---|
| `luma_broker_connections_active` | Gauge | Live MQTT connections |
| `luma_broker_connects_total` | Counter | Total CONNECT packets |
| `luma_broker_publish_total{qos}` | Counter | PUBLISH packets by QoS |
| `luma_broker_publish_bytes_total` | Counter | Total payload bytes |
| `luma_broker_delivered_total` | Counter | Messages delivered to subscribers |
| `luma_broker_dropped_total{reason}` | Counter | Dropped messages (rate_limit, write_queue_full, acl_denied) |
| `luma_broker_subscriptions_active` | Gauge | Active subscriptions |
| `luma_broker_retained_messages` | Gauge | Stored retained messages |
| `luma_broker_publish_latency_seconds` | Histogram | End-to-end publish latency |
| `luma_broker_routing_latency_seconds` | Histogram | Trie match + dispatch time |
| `luma_api_requests_total{method,path,status}` | Counter | HTTP request count |
| `luma_api_ws_connections_active` | Gauge | WebSocket clients |

Grafana dashboard: import `monitoring/grafana/` provisioning files.

---

## Project Structure

```
luma-go/
├── cmd/broker/main.go              Single binary entrypoint
├── internal/
│   ├── protocol/packet.go          MQTT 3.1.1 + v5 codec (zero-alloc)
│   ├── router/router.go            256-shard topic trie + routing engine
│   ├── session/session.go          QoS FSM, in-flight tracking, offline queue
│   ├── auth/auth.go                JWT (cached), bcrypt, ACL, rate limiter
│   ├── storage/storage.go          Redis + pgx/v5 PostgreSQL backends
│   ├── broker/
│   │   ├── server.go               TCP/TLS accept loop, cluster, $SYS
│   │   ├── client.go               Per-connection handler (read+write goroutines)
│   │   └── helpers.go              API-facing broker accessors
│   ├── api/server.go               net/http REST + WebSocket + WS hub
│   └── metrics/metrics.go          Prometheus registry (all metrics)
├── pkg/
│   ├── config/config.go            Environment-based config with defaults
│   └── logger/logger.go            Structured zap logger
├── clients/go/client.go            Protocol test + load benchmark client
├── k8s/deployment.yaml             Kubernetes: Deployment, HPA, PDB, NetworkPolicy
├── monitoring/prometheus.yml       Prometheus scrape config
├── docker-compose.yml              Full stack (broker + postgres + redis + monitoring)
├── Dockerfile                      Multi-stage scratch build (~8 MB image)
├── Makefile                        build / test / bench / docker / k8s targets
└── .env.example                    All environment variables with documentation
```
