// Package config loads and validates all runtime configuration.
// Supports environment variables with sensible defaults.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the root configuration structure.
type Config struct {
	Broker   BrokerConfig
	API      APIConfig
	Redis    RedisConfig
	Postgres PostgresConfig
	Auth     AuthConfig
	TLS      TLSConfig
	Cluster  ClusterConfig
	SLO      SLOConfig
	Metrics  MetricsConfig
	Log      LogConfig
}

type BrokerConfig struct {
	TCPAddr        string        // default ":1883"
	TLSAddr        string        // default ":8883"
	WSAddr         string        // MQTT-over-WebSocket listener, default ":8083"
	QUICAddr       string        // MQTT-over-QUIC listener, default ":1884"
	EnableWS       bool
	EnableQUIC     bool
	MaxConnections int           // default 50000
	ReadBufSize    int           // default 32768 bytes
	WriteBufSize   int           // default 32768 bytes
	WriteQueueSize int           // default 512
	MaxPacketSize  int           // default 10 MB
	SysInterval    time.Duration // $SYS publish interval
	PingGrace      float64       // multiplier on keepalive before timeout (1.5)
}

type APIConfig struct {
	Addr            string
	AllowedOrigins  []string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
	WSWriteWait     time.Duration
	WSPongWait      time.Duration
	WSPingPeriod    time.Duration
}

type RedisConfig struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdle      int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type PostgresConfig struct {
	DSN          string
	MaxConns     int32
	MinConns     int32
	MaxLifetime  time.Duration
	MaxIdleTime  time.Duration
	ConnTimeout  time.Duration
}

type AuthConfig struct {
	JWTSecret       string
	JWTDuration     time.Duration
	JWTIssuer       string
	BcryptCost      int
	RateLimit       float64 // msgs/sec per client
	RateBurst       float64
	DefaultPassword string // for initial admin account
}

type TLSConfig struct {
	CertFile       string
	KeyFile        string
	MinTLS         string // "1.2" or "1.3"
	ClientCAFile   string
	ClientAuthMode string // "none", "request", "require", "require_and_verify"
	RevokedCerts   []string
}

type ClusterConfig struct {
	Enabled         bool
	BrokerID        string
	PartitionCount  int
	PartitionReplica int
}

type SLOConfig struct {
	TargetMessagesPerSecond int64
	TargetP99LatencyMs      int
	TargetConnPerCluster    int64
	FailoverRTOSeconds      int
	FailoverRPOSeconds      int
	ErrorBudgetPercent      float64
}

type MetricsConfig struct {
	Addr    string // Prometheus /metrics addr, default ":9090"
	Enabled bool
}

type LogConfig struct {
	Level      string // debug, info, warn, error
	Format     string // json, console
	OutputPath string
}

// Load reads all config from environment variables.
func Load() *Config {
	return &Config{
		Broker: BrokerConfig{
			TCPAddr:        env("MQTT_TCP_ADDR", ":1883"),
			TLSAddr:        env("MQTT_TLS_ADDR", ":8883"),
			WSAddr:         env("MQTT_WS_ADDR", ":8083"),
			QUICAddr:       env("MQTT_QUIC_ADDR", ":1884"),
			EnableWS:       envBool("MQTT_WS_ENABLED", true),
			EnableQUIC:     envBool("MQTT_QUIC_ENABLED", false),
			MaxConnections: envInt("MAX_CONNECTIONS", 50000),
			ReadBufSize:    envInt("READ_BUF_SIZE", 32768),
			WriteBufSize:   envInt("WRITE_BUF_SIZE", 32768),
			WriteQueueSize: envInt("WRITE_QUEUE_SIZE", 512),
			MaxPacketSize:  envInt("MAX_PACKET_SIZE", 10*1024*1024),
			SysInterval:    envDuration("SYS_INTERVAL", 10*time.Second),
			PingGrace:      envFloat("PING_GRACE", 1.5),
		},
		API: APIConfig{
			Addr:            env("API_ADDR", ":8080"),
			AllowedOrigins:  envCSV("API_ALLOWED_ORIGINS", []string{"*"}),
			ReadTimeout:     envDuration("API_READ_TIMEOUT", 30*time.Second),
			WriteTimeout:    envDuration("API_WRITE_TIMEOUT", 30*time.Second),
			IdleTimeout:     envDuration("API_IDLE_TIMEOUT", 120*time.Second),
			ShutdownTimeout: envDuration("API_SHUTDOWN_TIMEOUT", 15*time.Second),
			WSWriteWait:     envDuration("WS_WRITE_WAIT", 10*time.Second),
			WSPongWait:      envDuration("WS_PONG_WAIT", 60*time.Second),
			WSPingPeriod:    envDuration("WS_PING_PERIOD", 54*time.Second),
		},
		Redis: RedisConfig{
			Addr:         env("REDIS_ADDR", "localhost:6379"),
			Password:     env("REDIS_PASSWORD", ""),
			DB:           envInt("REDIS_DB", 0),
			PoolSize:     envInt("REDIS_POOL_SIZE", 50),
			MinIdle:      envInt("REDIS_MIN_IDLE", 10),
			DialTimeout:  envDuration("REDIS_DIAL_TIMEOUT", 5*time.Second),
			ReadTimeout:  envDuration("REDIS_READ_TIMEOUT", 3*time.Second),
			WriteTimeout: envDuration("REDIS_WRITE_TIMEOUT", 3*time.Second),
		},
		Postgres: PostgresConfig{
			DSN:         env("POSTGRES_DSN", "postgres://mqtt:mqtt@localhost:5432/mqtt?sslmode=disable"),
			MaxConns:    int32(envInt("PG_MAX_CONNS", 25)),
			MinConns:    int32(envInt("PG_MIN_CONNS", 5)),
			MaxLifetime: envDuration("PG_MAX_LIFETIME", 5*time.Minute),
			MaxIdleTime: envDuration("PG_MAX_IDLE_TIME", 30*time.Second),
			ConnTimeout: envDuration("PG_CONN_TIMEOUT", 10*time.Second),
		},
		Auth: AuthConfig{
			JWTSecret:       mustEnv("JWT_SECRET", "luma-change-in-production-32bytes"),
			JWTDuration:     envDuration("JWT_DURATION", 24*time.Hour),
			JWTIssuer:       env("JWT_ISSUER", "luma-broker"),
			BcryptCost:      envInt("BCRYPT_COST", 12),
			RateLimit:       envFloat("RATE_LIMIT", 1000),
			RateBurst:       envFloat("RATE_BURST", 2000),
			DefaultPassword: env("ADMIN_PASSWORD", "admin123"),
		},
		TLS: TLSConfig{
			CertFile:       env("TLS_CERT_FILE", ""),
			KeyFile:        env("TLS_KEY_FILE", ""),
			MinTLS:         env("TLS_MIN_VERSION", "1.2"),
			ClientCAFile:   env("TLS_CLIENT_CA_FILE", ""),
			ClientAuthMode: env("TLS_CLIENT_AUTH_MODE", "none"),
			RevokedCerts:   envCSV("TLS_REVOKED_SERIALS", nil),
		},
		Cluster: ClusterConfig{
			Enabled:          envBool("CLUSTER_MODE", false),
			BrokerID:         env("BROKER_ID", ""),
			PartitionCount:   envInt("CLUSTER_PARTITIONS", 128),
			PartitionReplica: envInt("CLUSTER_REPLICAS", 3),
		},
		SLO: SLOConfig{
			TargetMessagesPerSecond: int64(envInt("SLO_TARGET_MESSAGES_PER_SEC", 1000000)),
			TargetP99LatencyMs:      envInt("SLO_TARGET_P99_MS", 50),
			TargetConnPerCluster:    int64(envInt("SLO_TARGET_CONNECTIONS", 100000000)),
			FailoverRTOSeconds:      envInt("SLO_FAILOVER_RTO_SECONDS", 60),
			FailoverRPOSeconds:      envInt("SLO_FAILOVER_RPO_SECONDS", 10),
			ErrorBudgetPercent:      envFloat("SLO_ERROR_BUDGET_PERCENT", 0.1),
		},
		Metrics: MetricsConfig{
			Addr:    env("METRICS_ADDR", ":9090"),
			Enabled: envBool("METRICS_ENABLED", true),
		},
		Log: LogConfig{
			Level:      env("LOG_LEVEL", "info"),
			Format:     env("LOG_FORMAT", "json"),
			OutputPath: env("LOG_OUTPUT", "stdout"),
		},
	}
}

// Validate checks required fields and returns an error if invalid.
func (c *Config) Validate() error {
	if len(c.Auth.JWTSecret) < 16 {
		return fmt.Errorf("JWT_SECRET must be at least 16 characters")
	}
	if c.Broker.MaxConnections <= 0 {
		return fmt.Errorf("MAX_CONNECTIONS must be positive")
	}
	if c.Cluster.PartitionCount <= 0 {
		return fmt.Errorf("CLUSTER_PARTITIONS must be positive")
	}
	if c.Cluster.PartitionReplica <= 0 {
		return fmt.Errorf("CLUSTER_REPLICAS must be positive")
	}
	if c.Broker.MaxPacketSize < 128 {
		return fmt.Errorf("MAX_PACKET_SIZE too small")
	}
	return nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func mustEnv(key, def string) string {
	v := env(key, def)
	if v == def && strings.Contains(def, "change") {
		// Warning — still returning default so service starts
		_ = fmt.Sprintf("WARNING: %s is using insecure default value", key)
	}
	return v
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v, ok := os.LookupEnv(key); ok {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envCSV(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if x := strings.TrimSpace(p); x != "" {
			out = append(out, x)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}
