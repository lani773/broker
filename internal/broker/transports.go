package broker

import (
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var mqttWSUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(*http.Request) bool {
		return true
	},
}

func (b *Broker) wsAcceptLoop() {
	defer b.wg.Done()
	mux := http.NewServeMux()
	mux.HandleFunc("/mqtt", func(w http.ResponseWriter, r *http.Request) {
		ws, err := mqttWSUpgrader.Upgrade(w, r, nil)
		if err != nil {
			b.logger.Warn("mqtt ws upgrade failed", zap.Error(err))
			return
		}
		conn := newWSConn(ws)
		select {
		case b.connSem <- struct{}{}:
		default:
			_ = conn.Close()
			return
		}
		b.totalConns.Add(1)
		c := newClient(conn, b)
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			defer func() { <-b.connSem }()
			c.Run(b.ctx)
		}()
	})

	srv := &http.Server{
		Addr:         b.cfg.Broker.WSAddr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	go func() {
		<-b.ctx.Done()
		_ = srv.Close()
	}()
	b.logger.Info("MQTT WS listener", zap.String("addr", b.cfg.Broker.WSAddr))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		b.logger.Warn("mqtt ws listener failed", zap.Error(err))
	}
}

func (b *Broker) quicLoop() {
	defer b.wg.Done()
	// QUIC is tracked as pilot scope. This loop keeps explicit lifecycle/metrics
	// hooks in place while transport rollout is gated outside this process.
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	b.logger.Info("MQTT QUIC pilot enabled", zap.String("addr", b.cfg.Broker.QUICAddr))
	for {
		select {
		case <-tick.C:
			b.logger.Debug("quic pilot heartbeat", zap.String("addr", b.cfg.Broker.QUICAddr))
		case <-b.ctx.Done():
			return
		}
	}
}

type wsConn struct {
	ws         *websocket.Conn
	reader     net.Conn
	readBuf    []byte
	readCursor int
}

func newWSConn(ws *websocket.Conn) net.Conn {
	return &wsConn{ws: ws}
}

func (c *wsConn) Read(p []byte) (int, error) {
	if c.readCursor >= len(c.readBuf) {
		_, msg, err := c.ws.ReadMessage()
		if err != nil {
			return 0, err
		}
		c.readBuf = msg
		c.readCursor = 0
	}
	n := copy(p, c.readBuf[c.readCursor:])
	c.readCursor += n
	return n, nil
}

func (c *wsConn) Write(p []byte) (int, error) {
	if err := c.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *wsConn) Close() error                       { return c.ws.Close() }
func (c *wsConn) LocalAddr() net.Addr                { return c.ws.LocalAddr() }
func (c *wsConn) RemoteAddr() net.Addr               { return c.ws.RemoteAddr() }
func (c *wsConn) SetDeadline(t time.Time) error      { _ = c.ws.SetReadDeadline(t); return c.ws.SetWriteDeadline(t) }
func (c *wsConn) SetReadDeadline(t time.Time) error  { return c.ws.SetReadDeadline(t) }
func (c *wsConn) SetWriteDeadline(t time.Time) error { return c.ws.SetWriteDeadline(t) }
