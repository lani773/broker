// Package metrics defines all Prometheus metrics for the LUMA broker.
// Single registry shared between broker and API processes.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Broker-level counters and gauges.
var (
	// ─── Connections ─────────────────────────────────────────────────────────
	ConnectTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "connects_total",
		Help:      "Total MQTT CONNECT packets received.",
	})
	DisconnectTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "disconnects_total",
		Help:      "Total disconnections, labelled by reason.",
	}, []string{"reason"})
	ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "connections_active",
		Help:      "Current number of connected MQTT clients.",
	})
	AuthFailTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "auth_failures_total",
		Help:      "Authentication failures by reason.",
	}, []string{"reason"})

	// ─── Messages ─────────────────────────────────────────────────────────────
	PublishTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "publish_total",
		Help:      "Total PUBLISH packets received, labelled by QoS.",
	}, []string{"qos"})
	PublishBytes = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "publish_bytes_total",
		Help:      "Total payload bytes received via PUBLISH.",
	})
	DeliveredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "delivered_total",
		Help:      "Total messages delivered to subscribers.",
	})
	DroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "dropped_total",
		Help:      "Messages dropped, labelled by reason.",
	}, []string{"reason"})

	// ─── Subscriptions ────────────────────────────────────────────────────────
	ActiveSubscriptions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "subscriptions_active",
		Help:      "Current number of active topic subscriptions.",
	})
	SubscribeTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "subscribes_total",
		Help:      "Total successful SUBSCRIBE operations.",
	})
	RetainedMessages = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "retained_messages",
		Help:      "Number of retained messages stored.",
	})

	// ─── Sessions ─────────────────────────────────────────────────────────────
	ActiveSessions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "sessions_active",
		Help:      "Number of active sessions (including persistent offline).",
	})
	OfflineQueuedMessages = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "offline_queued_messages",
		Help:      "Messages queued for offline persistent-session clients.",
	})

	// ─── Network I/O ─────────────────────────────────────────────────────────
	BytesReceived = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "bytes_received_total",
		Help:      "Total bytes received from clients.",
	})
	BytesSent = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "bytes_sent_total",
		Help:      "Total bytes sent to clients.",
	})

	// ─── Latency ─────────────────────────────────────────────────────────────
	PublishLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "publish_latency_seconds",
		Help:      "Time from PUBLISH receipt to final delivery to all subscribers.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 16), // 100µs to ~3s
	})
	RoutingLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "luma",
		Subsystem: "broker",
		Name:      "routing_latency_seconds",
		Help:      "Topic trie match + dispatch time.",
		Buckets:   prometheus.ExponentialBuckets(0.000001, 4, 12), // 1µs to ~4ms
	})

	// ─── API ──────────────────────────────────────────────────────────────────
	APIRequestTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "luma",
		Subsystem: "api",
		Name:      "requests_total",
		Help:      "Total HTTP requests, labelled by method, path, status.",
	}, []string{"method", "path", "status"})
	APIRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "luma",
		Subsystem: "api",
		Name:      "request_duration_seconds",
		Help:      "HTTP request latency distribution.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "path"})
	WSConnectionsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "luma",
		Subsystem: "api",
		Name:      "ws_connections_active",
		Help:      "Active WebSocket connections.",
	})
	SLOViolationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "luma",
		Subsystem: "sre",
		Name:      "slo_violations_total",
		Help:      "Total SLO violations by objective.",
	}, []string{"objective"})
	CanaryStage = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "luma",
		Subsystem: "sre",
		Name:      "canary_stage",
		Help:      "Current canary rollout stage.",
	})
)

// Handler returns the Prometheus HTTP handler.
func Handler() http.Handler {
	return promhttp.Handler()
}
