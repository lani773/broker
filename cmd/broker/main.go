// Command broker is the unified LUMA MQTT broker binary.
// It starts the MQTT broker, REST+WebSocket API, and Prometheus metrics
// all in a single process. Use CLUSTER_MODE=true for multi-instance deployments.
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/luma/broker/internal/api"
	"github.com/luma/broker/internal/auth"
	"github.com/luma/broker/internal/broker"
	"github.com/luma/broker/internal/metrics"
	"github.com/luma/broker/internal/storage"
	"github.com/luma/broker/pkg/config"
	"github.com/luma/broker/pkg/logger"
	"go.uber.org/zap"
)

var version = "2.0.0-dev"

func main() {
	// ── Config ────────────────────────────────────────────────────────────────
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		panic("config error: " + err.Error())
	}

	// ── Logger ────────────────────────────────────────────────────────────────
	log := logger.Must(cfg.Log.Level, cfg.Log.Format)
	defer log.Sync()

	log.Info("LUMA MQTT Broker",
		zap.String("version", version),
		zap.String("broker_tcp", cfg.Broker.TCPAddr),
		zap.String("broker_ws", cfg.Broker.WSAddr),
		zap.String("broker_quic", cfg.Broker.QUICAddr),
		zap.String("api_addr", cfg.API.Addr),
	)

	// ── Storage ───────────────────────────────────────────────────────────────
	red, err := storage.NewRedis(cfg.Redis, log)
	if err != nil {
		log.Fatal("Redis failed", zap.Error(err))
	}

	pg, err := storage.NewPostgres(cfg.Postgres, log)
	if err != nil {
		log.Fatal("PostgreSQL failed", zap.Error(err))
	}

	// ── Authenticator (shared between broker + API) ───────────────────────────
	authn := auth.New(
		cfg.Auth.JWTSecret,
		cfg.Auth.JWTIssuer,
		cfg.Auth.JWTDuration,
		cfg.Auth.BcryptCost,
	)
	if err := authn.AddUser("admin", cfg.Auth.DefaultPassword, []string{"admin"}); err != nil {
		log.Warn("default admin user", zap.Error(err))
	}

	// ── MQTT Broker ───────────────────────────────────────────────────────────
	b, err := broker.New(cfg, log)
	if err != nil {
		log.Fatal("broker init failed", zap.Error(err))
	}
	if err := b.Start(); err != nil {
		log.Fatal("broker start failed", zap.Error(err))
	}

	// ── HTTP API ─────────────────────────────────────────────────────────────
	apiSrv := api.New(cfg, b, red, pg, authn, log)
	if err := apiSrv.Start(); err != nil {
		log.Fatal("api start failed", zap.Error(err))
	}

	// ── Prometheus Metrics ───────────────────────────────────────────────────
	if cfg.Metrics.Enabled {
		metricsSrv := &http.Server{
			Addr:    cfg.Metrics.Addr,
			Handler: metrics.Handler(),
		}
		go func() {
			log.Info("Metrics listener", zap.String("addr", cfg.Metrics.Addr))
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Warn("metrics server error", zap.Error(err))
			}
		}()
	}

	// ── Signal Handling ──────────────────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	log.Info("shutdown signal received", zap.String("signal", sig.String()))

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.API.ShutdownTimeout)
	defer cancel()

	apiSrv.Stop(shutdownCtx)
	b.Stop()

	log.Info("shutdown complete")
}

// Ensure main runs with GOMAXPROCS matching available CPUs
func init() {
	// Go 1.5+ defaults to GOMAXPROCS = NumCPU; this is a no-op but documents intent
	_ = time.Now()
}
