// Package storage provides Redis and PostgreSQL backends using pgx/v5 (fast,
// no reflection) and go-redis/v9 (async, pipeline support).
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/luma/broker/pkg/config"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ─── Redis ────────────────────────────────────────────────────────────────────

const (
	keyClients   = "luma:clients"
	keyRetained  = "luma:retained"
	keyBrokerMeta= "luma:broker"
	chanBroker   = "luma:cluster:messages"
	chanAPIPublish = "luma:api:publish"
	chanAdminCmd = "luma:admin:cmd"
)

// Redis wraps go-redis with domain-specific helpers.
type Redis struct {
	c      *redis.Client
	logger *zap.Logger
}

// NewRedis creates a validated Redis connection.
func NewRedis(cfg config.RedisConfig, logger *zap.Logger) (*Redis, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdle,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	logger.Info("Redis connected", zap.String("addr", cfg.Addr))
	return &Redis{c: rdb, logger: logger}, nil
}

// ─── Client Registry ─────────────────────────────────────────────────────────

// ClientInfo is stored per connected client in Redis.
type ClientInfo struct {
	ClientID    string    `json:"client_id"`
	Username    string    `json:"username"`
	BrokerID    string    `json:"broker_id"`
	RemoteAddr  string    `json:"remote_addr"`
	ConnectedAt time.Time `json:"connected_at"`
	Protocol    byte      `json:"protocol"`
}

func (r *Redis) SetClient(ctx context.Context, info *ClientInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return r.c.HSet(ctx, keyClients, info.ClientID, data).Err()
}

func (r *Redis) DelClient(ctx context.Context, clientID string) error {
	return r.c.HDel(ctx, keyClients, clientID).Err()
}

func (r *Redis) GetAllClients(ctx context.Context) ([]*ClientInfo, error) {
	raw, err := r.c.HGetAll(ctx, keyClients).Result()
	if err != nil {
		return nil, err
	}
	out := make([]*ClientInfo, 0, len(raw))
	for _, v := range raw {
		var ci ClientInfo
		if json.Unmarshal([]byte(v), &ci) == nil {
			out = append(out, &ci)
		}
	}
	return out, nil
}

func (r *Redis) ClientCount(ctx context.Context) (int64, error) {
	return r.c.HLen(ctx, keyClients).Result()
}

// ─── Retained Messages ───────────────────────────────────────────────────────

type RetainedRecord struct {
	Topic     string    `json:"topic"`
	Payload   []byte    `json:"payload"`
	QoS       byte      `json:"qos"`
	Timestamp time.Time `json:"ts"`
}

func (r *Redis) SetRetained(ctx context.Context, topic string, payload []byte, qos byte) error {
	if len(payload) == 0 {
		return r.c.HDel(ctx, keyRetained, topic).Err()
	}
	rec := RetainedRecord{Topic: topic, Payload: payload, QoS: qos, Timestamp: time.Now()}
	data, _ := json.Marshal(rec)
	return r.c.HSet(ctx, keyRetained, topic, data).Err()
}

func (r *Redis) GetAllRetained(ctx context.Context) ([]*RetainedRecord, error) {
	raw, err := r.c.HGetAll(ctx, keyRetained).Result()
	if err != nil {
		return nil, err
	}
	out := make([]*RetainedRecord, 0, len(raw))
	for _, v := range raw {
		var rec RetainedRecord
		if json.Unmarshal([]byte(v), &rec) == nil {
			out = append(out, &rec)
		}
	}
	return out, nil
}

// ─── Message Rate Counter ─────────────────────────────────────────────────────

// IncrMsgRate increments a per-second message counter with 2s TTL.
// Calling GetMsgRate one second later gives the approximate rate.
func (r *Redis) IncrMsgRate(ctx context.Context) {
	key := fmt.Sprintf("luma:rate:%d", time.Now().Unix())
	pipe := r.c.Pipeline()
	pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 3*time.Second)
	pipe.Exec(ctx) //nolint:errcheck
}

func (r *Redis) GetMsgRate(ctx context.Context) int64 {
	key := fmt.Sprintf("luma:rate:%d", time.Now().Unix()-1)
	n, _ := r.c.Get(ctx, key).Int64()
	return n
}

// ─── Cluster Pub/Sub ─────────────────────────────────────────────────────────

// ClusterMsg is fan-out via Redis when cluster mode is enabled.
type ClusterMsg struct {
	SourceBroker string `json:"src"`
	Partition    int    `json:"partition"`
	Sequence     uint64 `json:"seq"`
	Topic        string `json:"topic"`
	Payload      []byte `json:"payload"`
	QoS          byte   `json:"qos"`
	Retain       bool   `json:"retain"`
	TimestampUnix int64 `json:"ts"`
}

func (r *Redis) PublishCluster(ctx context.Context, msg *ClusterMsg) error {
	data, _ := json.Marshal(msg)
	return r.c.Publish(ctx, chanBroker, data).Err()
}

func (r *Redis) SubscribeCluster(ctx context.Context) <-chan *ClusterMsg {
	sub := r.c.Subscribe(ctx, chanBroker)
	ch  := make(chan *ClusterMsg, 512)
	go func() {
		defer sub.Close()
		for m := range sub.Channel() {
			var cm ClusterMsg
			if json.Unmarshal([]byte(m.Payload), &cm) == nil {
				select {
				case ch <- &cm:
				case <-ctx.Done():
					return
				default:
				}
			}
		}
	}()
	return ch
}

// ─── API Command Channel ─────────────────────────────────────────────────────

// APIPublishMsg is sent by the REST API to inject a message into the broker.
type APIPublishMsg struct {
	Topic     string `json:"topic"`
	Payload   string `json:"payload"`
	QoS       byte   `json:"qos"`
	Retain    bool   `json:"retain"`
	Publisher string `json:"publisher"`
}

func (r *Redis) SubscribeAPIPublish(ctx context.Context) <-chan *APIPublishMsg {
	sub := r.c.Subscribe(ctx, chanAPIPublish)
	ch  := make(chan *APIPublishMsg, 256)
	go func() {
		defer sub.Close()
		for m := range sub.Channel() {
			var msg APIPublishMsg
			if json.Unmarshal([]byte(m.Payload), &msg) == nil {
				select {
				case ch <- &msg:
				case <-ctx.Done():
					return
				default:
				}
			}
		}
	}()
	return ch
}

func (r *Redis) PublishAPI(ctx context.Context, msg *APIPublishMsg) error {
	data, _ := json.Marshal(msg)
	return r.c.Publish(ctx, chanAPIPublish, data).Err()
}

// PublishToWS publishes an MQTT message event to the WebSocket broadcast channel.
func (r *Redis) PublishToWS(ctx context.Context, msg *ClusterMsg) error {
	return r.PublishCluster(ctx, msg)
}

func (r *Redis) SubscribeWS(ctx context.Context) <-chan *ClusterMsg {
	return r.SubscribeCluster(ctx)
}

// SetBrokerMeta stores broker metadata for admin/cluster status.
func (r *Redis) SetBrokerMeta(ctx context.Context, brokerID string, meta map[string]interface{}) error {
	data, _ := json.Marshal(meta)
	return r.c.HSet(ctx, keyBrokerMeta, brokerID, data).Err()
}

// Close shuts down the Redis connection.
func (r *Redis) Close() error { return r.c.Close() }

// Client returns the raw redis client (for pipeline use in API).
func (r *Redis) Client() *redis.Client { return r.c }

// ─── PostgreSQL ───────────────────────────────────────────────────────────────

// Postgres wraps a pgx connection pool with domain operations.
type Postgres struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// NewPostgres creates a pgx pool and runs migrations.
func NewPostgres(cfg config.PostgresConfig, logger *zap.Logger) (*Postgres, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("pgx parse config: %w", err)
	}
	poolCfg.MaxConns          = cfg.MaxConns
	poolCfg.MinConns          = cfg.MinConns
	poolCfg.MaxConnLifetime   = cfg.MaxLifetime
	poolCfg.MaxConnIdleTime   = cfg.MaxIdleTime
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnTimeout

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pgx ping: %w", err)
	}

	pg := &Postgres{pool: pool, logger: logger}
	if err := pg.migrate(ctx); err != nil {
		return nil, fmt.Errorf("migration: %w", err)
	}

	logger.Info("PostgreSQL ready", zap.String("dsn", maskDSN(cfg.DSN)))
	return pg, nil
}

func (p *Postgres) migrate(ctx context.Context) error {
	_, err := p.pool.Exec(ctx, `
		CREATE EXTENSION IF NOT EXISTS "pgcrypto";

		CREATE TABLE IF NOT EXISTS devices (
			id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			client_id   TEXT UNIQUE NOT NULL,
			username    TEXT,
			description TEXT,
			metadata    JSONB    DEFAULT '{}',
			last_seen   TIMESTAMPTZ,
			connected   BOOLEAN  DEFAULT FALSE,
			created_at  TIMESTAMPTZ DEFAULT NOW(),
			updated_at  TIMESTAMPTZ DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_devices_cid ON devices(client_id);

		CREATE TABLE IF NOT EXISTS subscriptions (
			id           BIGSERIAL PRIMARY KEY,
			client_id    TEXT NOT NULL,
			topic_filter TEXT NOT NULL,
			qos          SMALLINT DEFAULT 0,
			created_at   TIMESTAMPTZ DEFAULT NOW(),
			UNIQUE(client_id, topic_filter)
		);
		CREATE INDEX IF NOT EXISTS idx_subs_cid ON subscriptions(client_id);

		CREATE TABLE IF NOT EXISTS users (
			id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			username      TEXT UNIQUE NOT NULL,
			password_hash TEXT NOT NULL,
			roles         TEXT[] DEFAULT '{}',
			banned        BOOLEAN DEFAULT FALSE,
			created_at    TIMESTAMPTZ DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS message_log (
			id           BIGSERIAL PRIMARY KEY,
			topic        TEXT NOT NULL,
			payload      BYTEA,
			qos          SMALLINT,
			retain       BOOLEAN DEFAULT FALSE,
			publisher    TEXT,
			published_at TIMESTAMPTZ DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_msg_topic ON message_log(topic);
		CREATE INDEX IF NOT EXISTS idx_msg_ts    ON message_log(published_at DESC);

		CREATE TABLE IF NOT EXISTS retained_messages (
			topic        TEXT PRIMARY KEY,
			payload      BYTEA NOT NULL,
			qos          SMALLINT DEFAULT 0,
			published_at TIMESTAMPTZ DEFAULT NOW()
		);
	`)
	return err
}

// ─── Devices ──────────────────────────────────────────────────────────────────

type Device struct {
	ID          string
	ClientID    string
	Username    string
	Description string
	Metadata    map[string]interface{}
	LastSeen    *time.Time
	Connected   bool
	CreatedAt   time.Time
}

func (p *Postgres) UpsertDevice(ctx context.Context, d *Device) error {
	meta, _ := json.Marshal(d.Metadata)
	_, err := p.pool.Exec(ctx, `
		INSERT INTO devices(client_id, username, description, metadata)
		VALUES ($1,$2,$3,$4::jsonb)
		ON CONFLICT(client_id) DO UPDATE SET
			username=EXCLUDED.username, description=EXCLUDED.description,
			metadata=EXCLUDED.metadata, updated_at=NOW()
	`, d.ClientID, d.Username, d.Description, meta)
	return err
}

func (p *Postgres) SetDeviceStatus(ctx context.Context, clientID string, connected bool) error {
	_, err := p.pool.Exec(ctx,
		"UPDATE devices SET connected=$2, last_seen=NOW(), updated_at=NOW() WHERE client_id=$1",
		clientID, connected)
	return err
}

func (p *Postgres) GetDevices(ctx context.Context, limit, offset int) ([]*Device, int64, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id,client_id,username,description,metadata,last_seen,connected,created_at
		FROM devices ORDER BY created_at DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var devices []*Device
	for rows.Next() {
		d := &Device{}
		var metaRaw []byte
		if err := rows.Scan(&d.ID, &d.ClientID, &d.Username, &d.Description,
			&metaRaw, &d.LastSeen, &d.Connected, &d.CreatedAt); err != nil {
			return nil, 0, err
		}
		if metaRaw != nil {
			json.Unmarshal(metaRaw, &d.Metadata)
		}
		devices = append(devices, d)
	}

	var total int64
	p.pool.QueryRow(ctx, "SELECT COUNT(*) FROM devices").Scan(&total)
	return devices, total, rows.Err()
}

func (p *Postgres) GetDevice(ctx context.Context, clientID string) (*Device, error) {
	d := &Device{}
	var metaRaw []byte
	err := p.pool.QueryRow(ctx, `
		SELECT id,client_id,username,description,metadata,last_seen,connected,created_at
		FROM devices WHERE client_id=$1`, clientID).
		Scan(&d.ID, &d.ClientID, &d.Username, &d.Description,
			&metaRaw, &d.LastSeen, &d.Connected, &d.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if metaRaw != nil {
		json.Unmarshal(metaRaw, &d.Metadata)
	}
	return d, nil
}

func (p *Postgres) DeleteDevice(ctx context.Context, clientID string) (bool, error) {
	tag, err := p.pool.Exec(ctx, "DELETE FROM devices WHERE client_id=$1", clientID)
	return tag.RowsAffected() > 0, err
}

// ─── Subscriptions ────────────────────────────────────────────────────────────

func (p *Postgres) SaveSub(ctx context.Context, clientID, filter string, qos byte) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO subscriptions(client_id,topic_filter,qos) VALUES($1,$2,$3)
		ON CONFLICT(client_id,topic_filter) DO UPDATE SET qos=EXCLUDED.qos`,
		clientID, filter, qos)
	return err
}

func (p *Postgres) DeleteSub(ctx context.Context, clientID, filter string) error {
	_, err := p.pool.Exec(ctx, "DELETE FROM subscriptions WHERE client_id=$1 AND topic_filter=$2",
		clientID, filter)
	return err
}

type SubRecord struct {
	Filter string
	QoS    byte
}

func (p *Postgres) GetSubs(ctx context.Context, clientID string) ([]SubRecord, error) {
	rows, err := p.pool.Query(ctx, "SELECT topic_filter,qos FROM subscriptions WHERE client_id=$1", clientID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var subs []SubRecord
	for rows.Next() {
		var s SubRecord
		if rows.Scan(&s.Filter, &s.QoS) == nil {
			subs = append(subs, s)
		}
	}
	return subs, rows.Err()
}

// ─── Users ────────────────────────────────────────────────────────────────────

type UserRecord struct {
	ID           string
	Username     string
	PasswordHash string
	Roles        []string
	Banned       bool
	CreatedAt    time.Time
}

func (p *Postgres) CreateUser(ctx context.Context, username, passwordHash string, roles []string) (*UserRecord, error) {
	row := p.pool.QueryRow(ctx, `
		INSERT INTO users(username,password_hash,roles)
		VALUES($1,$2,$3)
		RETURNING id,username,password_hash,roles,banned,created_at`,
		username, passwordHash, roles)

	u := &UserRecord{}
	err := row.Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Roles, &u.Banned, &u.CreatedAt)
	return u, err
}

func (p *Postgres) GetUser(ctx context.Context, username string) (*UserRecord, error) {
	u := &UserRecord{}
	err := p.pool.QueryRow(ctx,
		"SELECT id,username,password_hash,roles,banned,created_at FROM users WHERE username=$1",
		username).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Roles, &u.Banned, &u.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	return u, err
}

func (p *Postgres) SetUserBanned(ctx context.Context, username string, banned bool) error {
	_, err := p.pool.Exec(ctx, "UPDATE users SET banned=$2 WHERE username=$1", username, banned)
	return err
}

// ─── Message Log ──────────────────────────────────────────────────────────────

func (p *Postgres) LogMsg(ctx context.Context, topic string, payload []byte, qos byte, retain bool, publisher string) error {
	_, err := p.pool.Exec(ctx,
		"INSERT INTO message_log(topic,payload,qos,retain,publisher) VALUES($1,$2,$3,$4,$5)",
		topic, payload, qos, retain, publisher)
	return err
}

type MsgRecord struct {
	ID          int64
	Topic       string
	Payload     []byte
	QoS         byte
	Retain      bool
	Publisher   string
	PublishedAt time.Time
}

func (p *Postgres) GetMsgs(ctx context.Context, topic string, limit, offset int) ([]*MsgRecord, int64, error) {
	var rows pgx.Rows
	var err error
	if topic != "" {
		rows, err = p.pool.Query(ctx, `
			SELECT id,topic,payload,qos,retain,COALESCE(publisher,''),published_at
			FROM message_log WHERE topic=$1 ORDER BY published_at DESC LIMIT $2 OFFSET $3`,
			topic, limit, offset)
	} else {
		rows, err = p.pool.Query(ctx, `
			SELECT id,topic,payload,qos,retain,COALESCE(publisher,''),published_at
			FROM message_log ORDER BY published_at DESC LIMIT $1 OFFSET $2`,
			limit, offset)
	}
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var msgs []*MsgRecord
	for rows.Next() {
		m := &MsgRecord{}
		if err := rows.Scan(&m.ID, &m.Topic, &m.Payload, &m.QoS, &m.Retain, &m.Publisher, &m.PublishedAt); err != nil {
			return nil, 0, err
		}
		msgs = append(msgs, m)
	}

	var total int64
	if topic != "" {
		p.pool.QueryRow(ctx, "SELECT COUNT(*) FROM message_log WHERE topic=$1", topic).Scan(&total)
	} else {
		p.pool.QueryRow(ctx, "SELECT COUNT(*) FROM message_log").Scan(&total)
	}
	return msgs, total, rows.Err()
}

// Close closes the pool.
func (p *Postgres) Close() { p.pool.Close() }

// Pool exposes the pool for direct use in the API.
func (p *Postgres) Pool() *pgxpool.Pool { return p.pool }

// ─── Helpers ─────────────────────────────────────────────────────────────────

func maskDSN(dsn string) string {
	if i := indexStr(dsn, "@"); i >= 0 {
		return "postgres://***@" + dsn[i+1:]
	}
	return dsn
}

func indexStr(s, sub string) int {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
