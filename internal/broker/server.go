package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/luma/broker/internal/auth"
	"github.com/luma/broker/internal/cluster"
	"github.com/luma/broker/internal/metrics"
	"github.com/luma/broker/internal/plugin"
	"github.com/luma/broker/internal/router"
	"github.com/luma/broker/internal/session"
	"github.com/luma/broker/internal/storage"
	"github.com/luma/broker/pkg/config"
	"go.uber.org/zap"
)

// Broker is the central MQTT broker instance.
type Broker struct {
	id     string
	cfg    *config.Config
	logger *zap.Logger

	// Subsystems
	router      *router.Router
	sessions    *session.Store
	authn       *auth.Authenticator
	rateLimiter *auth.RateLimiter
	redis       *storage.Redis
	pg          *storage.Postgres
	partitions  *cluster.Manager
	plugins     *plugin.Manager

	// Active client map: clientID → *Client
	mu      sync.RWMutex
	clients map[string]*Client

	// Connection semaphore
	connSem chan struct{}

	// Metrics
	totalConns atomic.Int64
	startTime  time.Time
	clusterSeq atomic.Uint64

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New constructs a fully initialized Broker.
func New(cfg *config.Config, logger *zap.Logger) (*Broker, error) {
	ctx, cancel := context.WithCancel(context.Background())

	red, err := storage.NewRedis(cfg.Redis, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("redis: %w", err)
	}

	pg, err := storage.NewPostgres(cfg.Postgres, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("postgres: %w", err)
	}

	authn := auth.New(cfg.Auth.JWTSecret, cfg.Auth.JWTIssuer, cfg.Auth.JWTDuration, cfg.Auth.BcryptCost)
	authn.SetRevokedSerials(cfg.TLS.RevokedCerts)
	if err := authn.AddUser("admin", cfg.Auth.DefaultPassword, []string{"admin"}); err != nil {
		logger.Warn("could not add default admin", zap.Error(err))
	}

	brokerID := cfg.Cluster.BrokerID
	if brokerID == "" {
		brokerID = uuid.New().String()
	}

	b := &Broker{
		id:          brokerID,
		cfg:         cfg,
		logger:      logger,
		router:      router.New(logger),
		sessions:    session.NewStore(),
		authn:       authn,
		rateLimiter: auth.NewRateLimiter(cfg.Auth.RateLimit, cfg.Auth.RateBurst),
		redis:       red,
		pg:          pg,
		partitions:  cluster.NewManager(brokerID, cfg.Cluster.PartitionCount),
		plugins:     plugin.NewManager(),
		clients:     make(map[string]*Client),
		connSem:     make(chan struct{}, cfg.Broker.MaxConnections),
		startTime:   time.Now(),
		ctx:         ctx,
		cancel:      cancel,
	}
	return b, nil
}

// Start launches the broker and begins accepting connections.
func (b *Broker) Start() error {
	if err := b.loadRetained(); err != nil {
		b.logger.Warn("retained load failed", zap.Error(err))
	}

	// TCP listener
	tcpLn, err := net.Listen("tcp", b.cfg.Broker.TCPAddr)
	if err != nil {
		return fmt.Errorf("tcp listen %s: %w", b.cfg.Broker.TCPAddr, err)
	}
	b.logger.Info("MQTT TCP listener", zap.String("addr", b.cfg.Broker.TCPAddr))
	b.wg.Add(1)
	go b.acceptLoop(tcpLn)

	// TLS listener (optional)
	if b.cfg.TLS.CertFile != "" && b.cfg.TLS.KeyFile != "" {
		tlsCfg, err := buildTLSConfig(b.cfg.TLS)
		if err != nil {
			b.logger.Warn("TLS disabled", zap.Error(err))
		} else {
			tlsLn, err := tls.Listen("tcp", b.cfg.Broker.TLSAddr, tlsCfg)
			if err != nil {
				b.logger.Warn("TLS listener failed", zap.Error(err))
			} else {
				b.logger.Info("MQTT TLS listener", zap.String("addr", b.cfg.Broker.TLSAddr))
				b.wg.Add(1)
				go b.acceptLoop(tlsLn)
			}
		}
	}

	// MQTT-over-WebSocket listener (optional)
	if b.cfg.Broker.EnableWS {
		b.wg.Add(1)
		go b.wsAcceptLoop()
	}

	// MQTT-over-QUIC listener (pilot placeholder)
	if b.cfg.Broker.EnableQUIC {
		b.wg.Add(1)
		go b.quicLoop()
	}

	// Cluster fan-out
	if b.cfg.Cluster.Enabled {
		b.wg.Add(1)
		go b.partitionSyncLoop()
		b.wg.Add(1)
		go b.clusterLoop()
	}

	// API publish ingestion
	b.wg.Add(1)
	go b.apiPublishLoop()

	// Background tasks
	b.wg.Add(2)
	go b.maintenanceLoop()
	go b.sysLoop()

	// Register this broker in Redis
	go b.redis.SetBrokerMeta(context.Background(), b.id, map[string]interface{}{
		"id":         b.id,
		"tcp_addr":   b.cfg.Broker.TCPAddr,
		"started_at": b.startTime,
	})

	b.logger.Info("LUMA broker started", zap.String("id", b.id))
	return nil
}

// Stop gracefully shuts down the broker with a drain period.
func (b *Broker) Stop() {
	b.logger.Info("shutting down broker...")
	b.cancel()

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		b.logger.Warn("shutdown timeout")
	}

	b.redis.Close()
	b.pg.Close()
	b.logger.Info("broker stopped")
}

// ─── Accept Loop ─────────────────────────────────────────────────────────────

func (b *Broker) acceptLoop(ln net.Listener) {
	defer b.wg.Done()
	defer ln.Close()

	var delay time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-b.ctx.Done():
				return
			default:
			}
			// Temporary error backoff
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if delay == 0 {
					delay = 5 * time.Millisecond
				} else {
					delay *= 2
					if delay > time.Second {
						delay = time.Second
					}
				}
				time.Sleep(delay)
				continue
			}
			b.logger.Error("accept error", zap.Error(err))
			return
		}
		delay = 0

		// Connection capacity check (non-blocking)
		select {
		case b.connSem <- struct{}{}:
		default:
			b.logger.Warn("connection limit reached, rejecting",
				zap.String("addr", conn.RemoteAddr().String()))
			conn.Close()
			metrics.DroppedTotal.WithLabelValues("conn_limit").Inc()
			continue
		}

		b.totalConns.Add(1)
		c := newClient(conn, b)

		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			defer func() { <-b.connSem }()
			c.Run(b.ctx)
		}()
	}
}

// ─── Client Registry ─────────────────────────────────────────────────────────

func (b *Broker) register(c *Client) {
	b.mu.Lock()
	b.clients[c.clientID] = c
	b.mu.Unlock()
}

func (b *Broker) unregister(clientID string) {
	b.mu.Lock()
	delete(b.clients, clientID)
	b.mu.Unlock()
}

func (b *Broker) takeover(clientID string) {
	b.mu.RLock()
	existing, ok := b.clients[clientID]
	b.mu.RUnlock()
	if ok {
		b.logger.Info("client takeover", zap.String("client", clientID))
		existing.sess.WillSet = false
		existing.conn.Close()
	}
}

// GetClient returns the active client connection for a client ID.
func (b *Broker) GetClient(clientID string) (*Client, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	c, ok := b.clients[clientID]
	return c, ok
}

// Publish allows the API to inject a message into the broker.
func (b *Broker) Publish(topic string, payload []byte, qos byte, retain bool) int {
	return b.router.Publish(topic, payload, qos, retain)
}

// ─── Background Loops ────────────────────────────────────────────────────────

func (b *Broker) maintenanceLoop() {
	defer b.wg.Done()
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			b.rateLimiter.Cleanup(5 * time.Minute)
			b.logger.Debug("broker heartbeat",
				zap.Int64("connections", int64(len(b.clients))),
				zap.Int("sessions", b.sessions.Count()),
			)
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *Broker) sysLoop() {
	defer b.wg.Done()
	tick := time.NewTicker(b.cfg.Broker.SysInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			b.publishSys()
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *Broker) publishSys() {
	ctx := context.Background()
	cc, _ := b.redis.ClientCount(ctx)
	rate := b.redis.GetMsgRate(ctx)
	uptime := time.Since(b.startTime).Seconds()

	pairs := [][2]string{
		{"$SYS/broker/clients/connected", fmt.Sprintf("%d", cc)},
		{"$SYS/broker/messages/rate", fmt.Sprintf("%d", rate)},
		{"$SYS/broker/uptime", fmt.Sprintf("%.0f", uptime)},
		{"$SYS/broker/version", "LUMA/2.0.0-go"},
		{"$SYS/broker/sessions", fmt.Sprintf("%d", b.sessions.Count())},
	}
	for _, p := range pairs {
		b.router.Publish(p[0], []byte(p[1]), 0, true)
	}
}

func (b *Broker) partitionSyncLoop() {
	defer b.wg.Done()
	t := time.NewTicker(b.cfg.Cluster.PartitionSyncInterval)
	defer t.Stop()
	for {
		b.reconcilePartitions()
		select {
		case <-t.C:
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *Broker) reconcilePartitions() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ids, err := b.redis.BrokerIDs(ctx)
	if err != nil || len(ids) == 0 {
		for p := 0; p < b.cfg.Cluster.PartitionCount; p++ {
			b.partitions.SetOwner(p, b.id)
		}
		return
	}
	sort.Strings(ids)
	n := len(ids)
	for p := 0; p < b.cfg.Cluster.PartitionCount; p++ {
		b.partitions.SetOwner(p, ids[p%n])
	}
}

func (b *Broker) clusterLoop() {
	defer b.wg.Done()
	ch := b.redis.SubscribeCluster(b.ctx)
	for {
		select {
		case msg := <-ch:
			if msg.SourceBroker != b.id {
				if msg.Ver >= 1 {
					if b.partitions.Owner(msg.Partition) != b.id {
						continue
					}
				}
				b.router.Publish(msg.Topic, msg.Payload, msg.QoS, msg.Retain)
			}
		case <-b.ctx.Done():
			return
		}
	}
}

// apiPublishLoop ingests messages sent via the REST API.
func (b *Broker) apiPublishLoop() {
	defer b.wg.Done()
	ch := b.redis.SubscribeAPIPublish(b.ctx)
	for {
		select {
		case msg := <-ch:
			b.router.Publish(msg.Topic, []byte(msg.Payload), msg.QoS, msg.Retain)
		case <-b.ctx.Done():
			return
		}
	}
}

// ─── Startup Helpers ─────────────────────────────────────────────────────────

func (b *Broker) loadRetained() error {
	records, err := b.redis.GetAllRetained(context.Background())
	if err != nil {
		return err
	}
	for _, rec := range records {
		b.router.SetRetained(&router.Retained{
			Topic:     rec.Topic,
			Payload:   rec.Payload,
			QoS:       rec.QoS,
			Timestamp: rec.Timestamp,
		})
	}
	b.logger.Info("retained messages loaded", zap.Int("count", len(records)))
	return nil
}

// ─── Stats ────────────────────────────────────────────────────────────────────

// Stats returns broker statistics for the API.
func (b *Broker) Stats(ctx context.Context) map[string]interface{} {
	cc, _ := b.redis.ClientCount(ctx)
	rate := b.redis.GetMsgRate(ctx)
	rStats := b.router.Stats()

	b.mu.RLock()
	localClients := len(b.clients)
	b.mu.RUnlock()

	return map[string]interface{}{
		"broker_id":           b.id,
		"uptime_seconds":      time.Since(b.startTime).Seconds(),
		"connected_clients":   cc,
		"local_clients":       localClients,
		"total_connections":   b.totalConns.Load(),
		"messages_per_second": rate,
		"sessions":            b.sessions.Count(),
		"retained_messages":   rStats["retained_messages"],
	}
}

// ─── TLS ─────────────────────────────────────────────────────────────────────

func buildTLSConfig(cfg config.TLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	minVer := uint16(tls.VersionTLS12)
	if cfg.MinTLS == "1.3" {
		minVer = tls.VersionTLS13
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   minVer,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},
		PreferServerCipherSuites: true,
	}
	switch cfg.ClientAuthMode {
	case "request":
		tlsCfg.ClientAuth = tls.RequestClientCert
	case "require":
		tlsCfg.ClientAuth = tls.RequireAnyClientCert
	case "require_and_verify":
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	default:
		tlsCfg.ClientAuth = tls.NoClientCert
	}
	if cfg.ClientCAFile != "" {
		caPEM, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read client ca: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("invalid client ca bundle")
		}
		tlsCfg.ClientCAs = caPool
	}
	return tlsCfg, nil
}
