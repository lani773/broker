// Package api implements the LUMA REST + WebSocket API in pure Go.
// Uses net/http with a lightweight router — no external framework required.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/luma/broker/internal/auth"
	"github.com/luma/broker/internal/broker"
	"github.com/luma/broker/internal/metrics"
	"github.com/luma/broker/internal/storage"
	"github.com/luma/broker/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

// ─── Server ───────────────────────────────────────────────────────────────────

// Server is the HTTP API server.
type Server struct {
	cfg     *config.Config
	broker  *broker.Broker
	authn   *auth.Authenticator
	redis   *storage.Redis
	pg      *storage.Postgres
	hub     *WSHub
	logger  *zap.Logger
	httpSrv *http.Server
}

// New constructs and wires the API server.
func New(cfg *config.Config, b *broker.Broker, red *storage.Redis, pg *storage.Postgres,
	authn *auth.Authenticator, logger *zap.Logger) *Server {

	s := &Server{
		cfg:    cfg,
		broker: b,
		authn:  authn,
		redis:  red,
		pg:     pg,
		logger: logger,
		hub:    newWSHub(logger),
	}

	mux := http.NewServeMux()
	s.register(mux)

	s.httpSrv = &http.Server{
		Addr:         cfg.API.Addr,
		Handler:      s.chain(mux),
		ReadTimeout:  cfg.API.ReadTimeout,
		WriteTimeout: cfg.API.WriteTimeout,
		IdleTimeout:  cfg.API.IdleTimeout,
	}
	return s
}

// Start begins serving HTTP requests.
func (s *Server) Start() error {
	go s.hub.run(context.Background())
	go s.ingestRedisMessages(context.Background())

	ln, err := net.Listen("tcp", s.cfg.API.Addr)
	if err != nil {
		return fmt.Errorf("api listen: %w", err)
	}
	s.logger.Info("API server listening", zap.String("addr", s.cfg.API.Addr))
	go s.httpSrv.Serve(ln)
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop(ctx context.Context) {
	s.httpSrv.Shutdown(ctx)
}

// ─── Route Registration ───────────────────────────────────────────────────────

func (s *Server) register(mux *http.ServeMux) {
	// Root
	mux.HandleFunc("GET /", s.handleRoot)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)

	// Auth
	mux.HandleFunc("POST /auth/register", s.handleRegister)
	mux.HandleFunc("POST /auth/login", s.handleLogin)
	mux.HandleFunc("GET /auth/me", s.authed(s.handleMe))
	mux.HandleFunc("POST /auth/refresh", s.authed(s.handleRefresh))

	// Devices
	mux.HandleFunc("POST /devices", s.authed(s.handleDeviceCreate))
	mux.HandleFunc("GET /devices", s.authed(s.handleDeviceList))
	mux.HandleFunc("GET /devices/{id}", s.authed(s.handleDeviceGet))
	mux.HandleFunc("DELETE /devices/{id}", s.authed(s.handleDeviceDelete))

	// MQTT Control
	mux.HandleFunc("POST /mqtt/publish", s.authed(s.handlePublish))
	mux.HandleFunc("POST /mqtt/subscribe", s.authed(s.handleSubscribe))
	mux.HandleFunc("GET /mqtt/messages", s.authed(s.handleMessages))
	mux.HandleFunc("GET /mqtt/retained", s.authed(s.handleRetained))
	mux.HandleFunc("DELETE /mqtt/retained/{topic...}", s.authed(s.handleRetainedDelete))

	// Admin
	mux.HandleFunc("GET /admin/stats", s.authed(s.handleStats))
	mux.HandleFunc("GET /admin/slo", s.admin(s.handleSLO))
	mux.HandleFunc("GET /admin/readiness", s.admin(s.handleReadiness))
	mux.HandleFunc("GET /admin/clients", s.authed(s.handleClients))
	mux.HandleFunc("GET /admin/logs", s.admin(s.handleLogs))
	mux.HandleFunc("POST /admin/acl", s.admin(s.handleACLAdd))
	mux.HandleFunc("POST /admin/disconnect/{id}", s.admin(s.handleDisconnect))
	mux.HandleFunc("POST /admin/ban", s.admin(s.handleBan))
	mux.HandleFunc("GET /admin/timeseries", s.authed(s.handleTimeseries))

	// WebSocket
	mux.HandleFunc("GET /ws", s.handleWebSocket)

	// Prometheus metrics (internal)
	mux.Handle("GET /metrics", metrics.Handler())
}

// ─── Middleware Chain ────────────────────────────────────────────────────────

func (s *Server) chain(next http.Handler) http.Handler {
	return s.corsMiddleware(s.loggingMiddleware(s.rateLimitMiddleware(next)))
}

// corsMiddleware adds CORS headers.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		allowOrigin := s.resolveOrigin(origin)
		w.Header().Set("Access-Control-Allow-Origin", allowOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
		w.Header().Set("Vary", "Origin")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// loggingMiddleware records request metrics.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		dur := time.Since(start)
		status := fmt.Sprintf("%d", rw.status)
		metrics.APIRequestTotal.WithLabelValues(r.Method, r.URL.Path, status).Inc()
		metrics.APIRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(dur.Seconds())
	})
}

// rateLimitMiddleware enforces per-IP limits.
var apiRateLimiter = auth.NewRateLimiter(200, 400)

func (s *Server) rateLimitMiddleware(next http.Handler) http.Handler {
	exempt := map[string]bool{"/health": true, "/ready": true, "/metrics": true, "/": true}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !exempt[r.URL.Path] {
			ip, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				ip = r.RemoteAddr
			}
			if !apiRateLimiter.Allow(ip) {
				writeJSON(w, http.StatusTooManyRequests, errBody("rate_limit_exceeded", "Too many requests"))
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// responseWriter captures the status code for metrics.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// ─── Auth Middleware ──────────────────────────────────────────────────────────

type claimsKey struct{}

func (s *Server) authed(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if bearer == "" {
			writeJSON(w, http.StatusUnauthorized, errBody("unauthorized", "Bearer token required"))
			return
		}
		claims, err := s.authn.AuthJWT(bearer)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, errBody("unauthorized", err.Error()))
			return
		}
		ctx := context.WithValue(r.Context(), claimsKey{}, claims)
		h(w, r.WithContext(ctx))
	}
}

func (s *Server) admin(h http.HandlerFunc) http.HandlerFunc {
	return s.authed(func(w http.ResponseWriter, r *http.Request) {
		claims := r.Context().Value(claimsKey{}).(*auth.Claims)
		for _, role := range claims.Roles {
			if role == "admin" {
				h(w, r)
				return
			}
		}
		writeJSON(w, http.StatusForbidden, errBody("forbidden", "Admin role required"))
	})
}

func getClaims(r *http.Request) *auth.Claims {
	v, _ := r.Context().Value(claimsKey{}).(*auth.Claims)
	return v
}

// ─── Root & Health ────────────────────────────────────────────────────────────

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"service": "LUMA MQTT Broker API",
		"version": "2.0.0",
		"status":  "operational",
		"docs":    "/health",
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	checks := map[string]string{}
	code := http.StatusOK

	if _, err := s.pg.Pool().Exec(ctx, "SELECT 1"); err != nil {
		checks["postgres"] = "error: " + err.Error()
		code = http.StatusServiceUnavailable
	} else {
		checks["postgres"] = "ok"
	}

	if err := s.redis.Client().Ping(ctx).Err(); err != nil {
		checks["redis"] = "error: " + err.Error()
		code = http.StatusServiceUnavailable
	} else {
		checks["redis"] = "ok"
	}

	status := "healthy"
	if code != http.StatusOK {
		status = "degraded"
	}
	writeJSON(w, code, map[string]interface{}{
		"status":         status,
		"checks":         checks,
		"ws_connections": s.hub.Count(),
		"go_routines":    runtime.NumGoroutine(),
		"timestamp":      time.Now().Unix(),
	})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	checks := map[string]string{}
	depsReady := true
	if _, err := s.pg.Pool().Exec(ctx, "SELECT 1"); err != nil {
		checks["postgres"] = "error: " + err.Error()
		depsReady = false
	} else {
		checks["postgres"] = "ok"
	}
	if err := s.redis.Client().Ping(ctx).Err(); err != nil {
		checks["redis"] = "error: " + err.Error()
		depsReady = false
	} else {
		checks["redis"] = "ok"
	}

	stats := s.broker.Stats(ctx)
	currMPS := int64FromAny(stats["messages_per_second"])
	clusterConns := int64FromAny(stats["connected_clients"])

	sloOK := depsReady &&
		currMPS <= s.cfg.SLO.TargetMessagesPerSecond &&
		(s.cfg.SLO.TargetConnPerCluster <= 0 || clusterConns <= s.cfg.SLO.TargetConnPerCluster)

	if depsReady && currMPS > s.cfg.SLO.TargetMessagesPerSecond {
		metrics.SLOViolationTotal.WithLabelValues("messages_per_second").Inc()
	}
	if depsReady && s.cfg.SLO.TargetConnPerCluster > 0 && clusterConns > s.cfg.SLO.TargetConnPerCluster {
		metrics.SLOViolationTotal.WithLabelValues("connections_cluster").Inc()
	}

	code := http.StatusOK
	if !depsReady || !sloOK {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]interface{}{
		"ready":                          sloOK,
		"dependencies_ok":                depsReady,
		"slo_throughput_ok":              currMPS <= s.cfg.SLO.TargetMessagesPerSecond,
		"slo_connections_ok":             s.cfg.SLO.TargetConnPerCluster <= 0 || clusterConns <= s.cfg.SLO.TargetConnPerCluster,
		"checks":                         checks,
		"slo_target_mps":                 s.cfg.SLO.TargetMessagesPerSecond,
		"slo_target_cluster_connections": s.cfg.SLO.TargetConnPerCluster,
		"current_mps":                    currMPS,
		"cluster_connected_clients":      clusterConns,
	})
}

func int64FromAny(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case uint64:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}

// ─── Auth Handlers ────────────────────────────────────────────────────────────

type registerReq struct {
	Username string   `json:"username"`
	Password string   `json:"password"`
	Roles    []string `json:"roles"`
}

type loginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req registerReq
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Username) < 3 || len(req.Password) < 8 {
		writeJSON(w, http.StatusBadRequest, errBody("validation", "username≥3, password≥8 chars"))
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), s.cfg.Auth.BcryptCost)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("server_error", err.Error()))
		return
	}

	// Also register in the in-memory authenticator for immediate use
	if err := s.authn.AddUser(req.Username, req.Password, req.Roles); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("server_error", err.Error()))
		return
	}

	user, err := s.pg.CreateUser(r.Context(), req.Username, string(hash), req.Roles)
	if err != nil {
		writeJSON(w, http.StatusConflict, errBody("conflict", "username already exists"))
		return
	}

	writeJSON(w, http.StatusCreated, user)
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req loginReq
	if !decodeJSON(w, r, &req) {
		return
	}

	user, err := s.pg.GetUser(r.Context(), req.Username)
	if err != nil || user == nil {
		writeJSON(w, http.StatusUnauthorized, errBody("unauthorized", "invalid credentials"))
		return
	}
	if user.Banned {
		writeJSON(w, http.StatusForbidden, errBody("forbidden", "account banned"))
		return
	}
	if bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)) != nil {
		writeJSON(w, http.StatusUnauthorized, errBody("unauthorized", "invalid credentials"))
		return
	}

	token, err := s.authn.IssueToken(user.Username, "", user.Roles)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("server_error", err.Error()))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"access_token": token,
		"token_type":   "Bearer",
		"expires_in":   int(s.cfg.Auth.JWTDuration.Seconds()),
		"username":     user.Username,
		"roles":        user.Roles,
	})
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	claims := getClaims(r)
	user, _ := s.pg.GetUser(r.Context(), claims.Username)
	writeJSON(w, http.StatusOK, user)
}

func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	claims := getClaims(r)
	token, err := s.authn.IssueToken(claims.Username, claims.ClientID, claims.Roles)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("server_error", err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"access_token": token, "token_type": "Bearer"})
}

// ─── Device Handlers ─────────────────────────────────────────────────────────

type deviceReq struct {
	ClientID    string                 `json:"client_id"`
	Username    string                 `json:"username"`
	Description string                 `json:"description"`
	Metadata    map[string]interface{} `json:"metadata"`
}

func (s *Server) handleDeviceCreate(w http.ResponseWriter, r *http.Request) {
	var req deviceReq
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.ClientID == "" {
		writeJSON(w, http.StatusBadRequest, errBody("validation", "client_id required"))
		return
	}
	d := &storage.Device{
		ClientID:    req.ClientID,
		Username:    req.Username,
		Description: req.Description,
		Metadata:    req.Metadata,
	}
	if err := s.pg.UpsertDevice(r.Context(), d); err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("db_error", err.Error()))
		return
	}
	created, _ := s.pg.GetDevice(r.Context(), req.ClientID)
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) handleDeviceList(w http.ResponseWriter, r *http.Request) {
	limit := queryInt(r, "limit", 50)
	offset := queryInt(r, "offset", 0)
	devices, total, err := s.pg.GetDevices(r.Context(), limit, offset)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("db_error", err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"devices": devices, "total": total, "limit": limit, "offset": offset,
	})
}

func (s *Server) handleDeviceGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	d, err := s.pg.GetDevice(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("db_error", err.Error()))
		return
	}
	if d == nil {
		writeJSON(w, http.StatusNotFound, errBody("not_found", "device not found"))
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) handleDeviceDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	deleted, err := s.pg.DeleteDevice(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("db_error", err.Error()))
		return
	}
	if !deleted {
		writeJSON(w, http.StatusNotFound, errBody("not_found", "device not found"))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── MQTT Control Handlers ────────────────────────────────────────────────────

type publishReq struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
	QoS     byte   `json:"qos"`
	Retain  bool   `json:"retain"`
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	var req publishReq
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Topic == "" || !protocol_ValidTopic(req.Topic) {
		writeJSON(w, http.StatusBadRequest, errBody("validation", "invalid topic"))
		return
	}
	claims := getClaims(r)

	// Inject via Redis so all broker instances receive it
	s.redis.PublishAPI(r.Context(), &storage.APIPublishMsg{
		Topic:     req.Topic,
		Payload:   req.Payload,
		QoS:       req.QoS,
		Retain:    req.Retain,
		Publisher: claims.Username,
	})

	// Async log
	go s.pg.LogMsg(context.Background(), req.Topic,
		[]byte(req.Payload), req.QoS, req.Retain, claims.Username)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true, "topic": req.Topic, "timestamp": time.Now().Unix(),
	})
}

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientID string `json:"client_id"`
		Filter   string `json:"topic_filter"`
		QoS      byte   `json:"qos"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.ClientID == "" || req.Filter == "" {
		writeJSON(w, http.StatusBadRequest, errBody("validation", "client_id and topic_filter required"))
		return
	}
	go s.pg.SaveSub(context.Background(), req.ClientID, req.Filter, req.QoS)
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"success": true, "client_id": req.ClientID, "topic_filter": req.Filter, "qos": req.QoS,
	})
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	limit := queryInt(r, "limit", 50)
	offset := queryInt(r, "offset", 0)

	msgs, total, err := s.pg.GetMsgs(r.Context(), topic, limit, offset)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("db_error", err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"messages": msgs, "total": total,
	})
}

func (s *Server) handleRetained(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	result := map[string]interface{}{}
	s.broker.RouterAllRetained(func(topic string, payload []byte, qos byte) {
		if prefix == "" || strings.HasPrefix(topic, prefix) {
			result[topic] = map[string]interface{}{"qos": qos, "size": len(payload)}
		}
	})
	writeJSON(w, http.StatusOK, map[string]interface{}{"retained": result, "count": len(result)})
}

func (s *Server) handleRetainedDelete(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	s.redis.PublishAPI(r.Context(), &storage.APIPublishMsg{
		Topic: topic, Payload: "", QoS: 0, Retain: true,
	})
	w.WriteHeader(http.StatusNoContent)
}

// ─── Admin Handlers ───────────────────────────────────────────────────────────

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.broker.Stats(r.Context()))
}

func (s *Server) handleSLO(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.Stats(r.Context())
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"targets": map[string]interface{}{
			"messages_per_second":  s.cfg.SLO.TargetMessagesPerSecond,
			"p99_latency_ms":       s.cfg.SLO.TargetP99LatencyMs,
			"connections_cluster":  s.cfg.SLO.TargetConnPerCluster,
			"failover_rto_seconds": s.cfg.SLO.FailoverRTOSeconds,
			"failover_rpo_seconds": s.cfg.SLO.FailoverRPOSeconds,
			"error_budget_percent": s.cfg.SLO.ErrorBudgetPercent,
		},
		"current": stats,
	})
}

func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	s.handleReady(w, r)
}

func (s *Server) handleClients(w http.ResponseWriter, r *http.Request) {
	clients, err := s.redis.GetAllClients(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("redis_error", err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"clients": clients, "count": len(clients), "timestamp": time.Now().Unix(),
	})
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	limit := queryInt(r, "limit", 100)
	topic := r.URL.Query().Get("topic")
	msgs, _, err := s.pg.GetMsgs(r.Context(), topic, limit, 0)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("db_error", err.Error()))
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"logs": msgs, "count": len(msgs)})
}

func (s *Server) handleACLAdd(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ClientGlob  string `json:"client_pattern"`
		TopicFilter string `json:"topic_pattern"`
		Permission  string `json:"permission"` // "subscribe","publish","both","none"
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	permMap := map[string]auth.Perm{
		"subscribe": auth.PermSubscribe,
		"publish":   auth.PermPublish,
		"both":      auth.PermAll,
		"none":      auth.PermNone,
	}
	perm, ok := permMap[req.Permission]
	if !ok {
		writeJSON(w, http.StatusBadRequest, errBody("validation", "invalid permission"))
		return
	}
	s.authn.AddACLRule(&auth.ACLRule{
		ClientGlob:  req.ClientGlob,
		TopicFilter: req.TopicFilter,
		Perm:        perm,
	})
	writeJSON(w, http.StatusCreated, map[string]string{"status": "added"})
}

func (s *Server) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if c, ok := s.broker.GetClient(id); ok {
		c.ForceDisconnect()
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "disconnect_sent", "client_id": id})
}

func (s *Server) handleBan(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	s.authn.SetBanned(req.Username, true)
	s.pg.SetUserBanned(r.Context(), req.Username, true)
	writeJSON(w, http.StatusOK, map[string]string{"status": "banned", "username": req.Username})
}

func (s *Server) handleTimeseries(w http.ResponseWriter, r *http.Request) {
	window := queryInt(r, "window", 60)
	now := time.Now().Unix()
	pipe := s.redis.Client().Pipeline()
	ctx := r.Context()

	cmds := make([]*interface{}, window)
	_ = cmds

	keys := make([]string, window)
	for i := 0; i < window; i++ {
		keys[i] = fmt.Sprintf("luma:rate:%d", now-int64(window)+int64(i))
		pipe.Get(ctx, keys[i])
	}
	results, _ := pipe.Exec(ctx)

	series := make([]map[string]interface{}, window)
	total := int64(0)
	for i, res := range results {
		val := int64(0)
		if res.Err() == nil {
			// Type-assert the result
			fmt.Sscanf(fmt.Sprint(res), "%d", &val)
		}
		total += val
		series[i] = map[string]interface{}{
			"timestamp": now - int64(window) + int64(i),
			"messages":  val,
		}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"window_seconds": window, "series": series, "total": total,
	})
}

// ─── WebSocket Hub ────────────────────────────────────────────────────────────

func (s *Server) streamingWSUpgrader() *websocket.Upgrader {
	return &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return s.originAllowed(r.Header.Get("Origin"))
		},
		ReadBufferSize:  1024,
		WriteBufferSize: 4096,
	}
}

type wsClient struct {
	id   string
	conn *websocket.Conn
	send chan []byte
}

// WSHub manages all active WebSocket connections.
type WSHub struct {
	mu      sync.RWMutex
	clients map[string]*wsClient
	count   atomic.Int64
	logger  *zap.Logger
}

func newWSHub(logger *zap.Logger) *WSHub {
	return &WSHub{
		clients: make(map[string]*wsClient),
		logger:  logger,
	}
}

func (h *WSHub) Count() int64 { return h.count.Load() }

func (h *WSHub) run(ctx context.Context) {
	// Hub just exists as registry; each client manages its own goroutines
	<-ctx.Done()
}

func (h *WSHub) add(c *wsClient) {
	h.mu.Lock()
	h.clients[c.id] = c
	h.mu.Unlock()
	h.count.Add(1)
	metrics.WSConnectionsActive.Inc()
}

func (h *WSHub) remove(id string) {
	h.mu.Lock()
	delete(h.clients, id)
	h.mu.Unlock()
	h.count.Add(-1)
	metrics.WSConnectionsActive.Dec()
}

// Broadcast sends a JSON message to all connected WebSocket clients.
func (h *WSHub) Broadcast(msg interface{}) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	h.mu.RLock()
	clients := make([]*wsClient, 0, len(h.clients))
	for _, c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, c := range clients {
		select {
		case c.send <- data:
		default:
			// Client too slow — drop
		}
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if !s.originAllowed(r.Header.Get("Origin")) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	up := s.streamingWSUpgrader()
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Warn("ws upgrade failed", zap.Error(err))
		return
	}

	c := &wsClient{
		id:   uuid.New().String(),
		conn: conn,
		send: make(chan []byte, 256),
	}
	s.hub.add(c)

	// Welcome
	conn.WriteJSON(map[string]interface{}{
		"event":     "connected",
		"id":        c.id,
		"timestamp": time.Now().Unix(),
		"message":   "Connected to LUMA MQTT stream",
	})

	// Write pump
	go func() {
		defer func() {
			s.hub.remove(c.id)
			conn.Close()
		}()
		pingTicker := time.NewTicker(s.cfg.API.WSPingPeriod)
		defer pingTicker.Stop()

		for {
			select {
			case data := <-c.send:
				conn.SetWriteDeadline(time.Now().Add(s.cfg.API.WSWriteWait))
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					return
				}
			case <-pingTicker.C:
				conn.SetWriteDeadline(time.Now().Add(s.cfg.API.WSWriteWait))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			}
		}
	}()

	// Read pump (handles client commands + keepalive)
	conn.SetReadLimit(4096)
	conn.SetReadDeadline(time.Now().Add(s.cfg.API.WSPongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(s.cfg.API.WSPongWait))
		return nil
	})

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var cmd map[string]interface{}
		if json.Unmarshal(raw, &cmd) != nil {
			continue
		}
		s.handleWSCommand(c, cmd)
	}
	s.hub.remove(c.id)
}

func (s *Server) handleWSCommand(c *wsClient, cmd map[string]interface{}) {
	action, _ := cmd["action"].(string)
	switch action {
	case "publish":
		topic, _ := cmd["topic"].(string)
		payload, _ := cmd["payload"].(string)
		qos := byte(0)
		if q, ok := cmd["qos"].(float64); ok {
			qos = byte(q)
		}
		if topic != "" {
			s.redis.PublishAPI(context.Background(), &storage.APIPublishMsg{
				Topic: topic, Payload: payload, QoS: qos,
			})
		}
		data, _ := json.Marshal(map[string]interface{}{"event": "published", "topic": topic})
		select {
		case c.send <- data:
		default:
		}
	case "ping":
		data, _ := json.Marshal(map[string]interface{}{"event": "pong", "timestamp": time.Now().Unix()})
		select {
		case c.send <- data:
		default:
		}
	}
}

// ingestRedisMessages forwards Redis pub/sub messages to WebSocket clients.
func (s *Server) ingestRedisMessages(ctx context.Context) {
	ch := s.redis.SubscribeWS(ctx)
	for {
		select {
		case msg := <-ch:
			s.hub.Broadcast(map[string]interface{}{
				"event":     "mqtt_message",
				"topic":     msg.Topic,
				"payload":   string(msg.Payload),
				"qos":       msg.QoS,
				"retain":    msg.Retain,
				"timestamp": time.Now().Unix(),
			})
		case <-ctx.Done():
			return
		}
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v interface{}) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("bad_request", "invalid JSON: "+err.Error()))
		return false
	}
	return true
}

func errBody(code, msg string) map[string]string {
	return map[string]string{"error": code, "message": msg}
}

func queryInt(r *http.Request, key string, def int) int {
	if v := r.URL.Query().Get(key); v != "" {
		var n int
		fmt.Sscanf(v, "%d", &n)
		if n > 0 {
			return n
		}
	}
	return def
}

func protocol_ValidTopic(topic string) bool {
	if len(topic) == 0 {
		return false
	}
	for i := 0; i < len(topic); i++ {
		if topic[i] == '+' || topic[i] == '#' || topic[i] == 0 {
			return false
		}
	}
	return true
}

func (s *Server) originAllowed(origin string) bool {
	allowed := s.cfg.API.AllowedOrigins
	if len(allowed) == 0 {
		return false
	}
	if origin == "" {
		return true
	}
	for _, o := range allowed {
		if o == "*" || o == origin {
			return true
		}
	}
	return false
}

func (s *Server) resolveOrigin(origin string) string {
	if origin == "" {
		return "*"
	}
	if s.originAllowed(origin) {
		return origin
	}
	if s.originAllowed("*") {
		return "*"
	}
	return "null"
}
