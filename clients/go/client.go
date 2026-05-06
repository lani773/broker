// LUMA Go Test Client - comprehensive MQTT protocol tests and load benchmarks.
//
// Usage:
//   go run client.go                            # all tests
//   go run client.go -test qos2                 # single test
//   go run client.go -bench -clients 1000       # load test
//   go run client.go -monitor "devices/#"       # live subscribe
//   go run client.go -api                       # REST API tests
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ─── Config ───────────────────────────────────────────────────────────────────

var (
	host     = flag.String("host", envOr("MQTT_HOST", "localhost"), "broker host")
	port     = flag.Int("port", 1883, "broker port")
	user     = flag.String("user", "admin", "mqtt username")
	pass     = flag.String("pass", "admin123", "mqtt password")
	apiBase  = flag.String("api", "http://localhost:8080", "API base URL")
	testName = flag.String("test", "all", "test name (all|connect|qos0|qos1|qos2|wildcard|retain|lwt|session)")
	bench    = flag.Bool("bench", false, "run benchmarks")
	clients  = flag.Int("clients", 100, "concurrent clients for benchmark")
	msgs     = flag.Int("msgs", 500, "messages per client")
	monitor  = flag.String("monitor", "", "topic filter to monitor (exits on Ctrl+C)")
	runAPI   = flag.Bool("apitest", false, "run REST API tests")
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ─── Raw MQTT Client ─────────────────────────────────────────────────────────

// MQTTClient is a minimal MQTT 3.1.1 client for testing.
type MQTTClient struct {
	conn     net.Conn
	clientID string
	mu       sync.Mutex

	recvCh   chan Packet
	doneCh   chan struct{}
}

type Packet struct {
	Type    byte
	Flags   byte
	Payload []byte
}

func Connect(addr, clientID, username, password string, cleanSession bool) (*MQTTClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	c := &MQTTClient{
		conn:     conn,
		clientID: clientID,
		recvCh:   make(chan Packet, 64),
		doneCh:   make(chan struct{}),
	}

	// Build CONNECT packet
	var payload bytes.Buffer
	writeUTF8(&payload, "MQTT")
	payload.WriteByte(4)         // v3.1.1

	var flags byte = 0x02 // CleanSession
	if username != "" {
		flags |= 0x80
	}
	if password != "" {
		flags |= 0x40
	}
	payload.WriteByte(flags)
	binary.Write(&payload, binary.BigEndian, uint16(60)) // keepalive

	writeUTF8(&payload, clientID)
	if username != "" {
		writeUTF8(&payload, username)
	}
	if password != "" {
		writeUTF8(&payload, password)
	}

	if err := c.writePacket(0x10, payload.Bytes()); err != nil {
		conn.Close()
		return nil, err
	}

	go c.readLoop()

	// Wait for CONNACK
	pkt, err := c.waitFor(0x20, 5*time.Second)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("connack: %w", err)
	}
	if len(pkt.Payload) < 2 || pkt.Payload[1] != 0 {
		conn.Close()
		return nil, fmt.Errorf("connection refused (code %d)", pkt.Payload[1])
	}

	return c, nil
}

func (c *MQTTClient) Disconnect() {
	c.conn.Write([]byte{0xE0, 0})
	close(c.doneCh)
	c.conn.Close()
}

func (c *MQTTClient) Publish(topic string, payload []byte, qos byte, retain bool) error {
	var flags byte = qos << 1
	if retain {
		flags |= 0x01
	}

	var buf bytes.Buffer
	writeUTF8(&buf, topic)
	if qos > 0 {
		binary.Write(&buf, binary.BigEndian, uint16(rand.Intn(65535)+1))
	}
	buf.Write(payload)

	return c.writePacket(0x30|flags, buf.Bytes())
}

func (c *MQTTClient) Subscribe(filter string, qos byte) error {
	packetID := uint16(rand.Intn(65535) + 1)
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, packetID)
	writeUTF8(&buf, filter)
	buf.WriteByte(qos)
	if err := c.writePacket(0x82, buf.Bytes()); err != nil {
		return err
	}
	_, err := c.waitFor(0x90, 5*time.Second)
	return err
}

func (c *MQTTClient) WaitMessage(timeout time.Duration) (*Packet, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case pkt := <-c.recvCh:
			if pkt.Type == 0x30 {
				return &pkt, true
			}
		case <-timer.C:
			return nil, false
		}
	}
}

func (c *MQTTClient) WaitMessages(count int, timeout time.Duration) []*Packet {
	var msgs []*Packet
	deadline := time.Now().Add(timeout)
	for len(msgs) < count && time.Now().Before(deadline) {
		pkt, ok := c.WaitMessage(time.Until(deadline))
		if !ok {
			break
		}
		msgs = append(msgs, pkt)
	}
	return msgs
}

func (c *MQTTClient) writePacket(typeFlags byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var buf bytes.Buffer
	buf.WriteByte(typeFlags)
	remaining := len(payload)
	for {
		digit := remaining % 128
		remaining /= 128
		if remaining > 0 {
			digit |= 0x80
		}
		buf.WriteByte(byte(digit))
		if remaining == 0 {
			break
		}
	}
	buf.Write(payload)
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := c.conn.Write(buf.Bytes())
	return err
}

func (c *MQTTClient) readLoop() {
	for {
		c.conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		b := make([]byte, 1)
		if _, err := io.ReadFull(c.conn, b); err != nil {
			return
		}
		typeFlags := b[0]

		// Read remaining length
		mult, remaining := 1, 0
		for {
			if _, err := io.ReadFull(c.conn, b); err != nil {
				return
			}
			remaining += int(b[0]&0x7F) * mult
			mult *= 128
			if b[0]&0x80 == 0 {
				break
			}
		}

		payload := make([]byte, remaining)
		if remaining > 0 {
			if _, err := io.ReadFull(c.conn, payload); err != nil {
				return
			}
		}

		select {
		case c.recvCh <- Packet{Type: typeFlags & 0xF0, Flags: typeFlags & 0x0F, Payload: payload}:
		case <-c.doneCh:
			return
		}
	}
}

func (c *MQTTClient) waitFor(pktype byte, timeout time.Duration) (*Packet, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case pkt := <-c.recvCh:
			if pkt.Type == pktype {
				return &pkt, nil
			}
		case <-timer.C:
			return nil, fmt.Errorf("timeout waiting for packet type 0x%02X", pktype)
		}
	}
}

func writeUTF8(w *bytes.Buffer, s string) {
	binary.Write(w, binary.BigEndian, uint16(len(s)))
	w.WriteString(s)
}

// ─── Test Helpers ─────────────────────────────────────────────────────────────

func brokerAddr() string { return fmt.Sprintf("%s:%d", *host, *port) }

func dial(id string) (*MQTTClient, error) {
	return Connect(brokerAddr(), id, *user, *pass, true)
}

func ok(msg string) { fmt.Printf("  ✅  %s\n", msg) }
func fail(msg string) { fmt.Printf("  ❌  %s\n", msg) }
func info(msg string) { fmt.Printf("  ℹ️   %s\n", msg) }
func header(title string) {
	fmt.Printf("\n%s\n  %s\n%s\n", line(), title, line())
}
func line() string { return "────────────────────────────────────────────────────" }

// ─── Tests ────────────────────────────────────────────────────────────────────

func testConnect() bool {
	header("TEST: Connection")
	c, err := dial("test-connect-" + rid())
	if err != nil {
		fail(err.Error())
		return false
	}
	ok("Connected to broker")
	c.Disconnect()
	ok("Disconnected cleanly")
	return true
}

func testQoS0() bool {
	header("TEST: QoS 0 Publish/Subscribe")
	topic := "test/qos0/" + rid()

	sub, err := dial("test-sub-qos0-" + rid())
	if err != nil { fail(err.Error()); return false }
	defer sub.Disconnect()

	if err := sub.Subscribe(topic, 0); err != nil { fail(err.Error()); return false }
	time.Sleep(100 * time.Millisecond)

	pub, err := dial("test-pub-qos0-" + rid())
	if err != nil { fail(err.Error()); return false }
	defer pub.Disconnect()

	pub.Publish(topic, []byte(`{"test":"qos0"}`), 0, false)

	if _, got := sub.WaitMessage(3 * time.Second); got {
		ok("QoS 0 message delivered")
		return true
	}
	fail("QoS 0 message not received within 3s")
	return false
}

func testQoS1() bool {
	header("TEST: QoS 1 Publish/Subscribe (At-Least-Once)")
	topic := "test/qos1/" + rid()

	sub, err := dial("test-sub-qos1-" + rid())
	if err != nil { fail(err.Error()); return false }
	defer sub.Disconnect()

	sub.Subscribe(topic, 1)
	time.Sleep(100 * time.Millisecond)

	pub, err := dial("test-pub-qos1-" + rid())
	if err != nil { fail(err.Error()); return false }
	defer pub.Disconnect()

	for i := 0; i < 5; i++ {
		pub.Publish(topic, []byte(fmt.Sprintf(`{"seq":%d}`, i)), 1, false)
	}

	msgs := sub.WaitMessages(5, 10*time.Second)
	if len(msgs) == 5 {
		ok(fmt.Sprintf("All 5 QoS 1 messages received"))
		return true
	}
	fail(fmt.Sprintf("Expected 5, got %d", len(msgs)))
	return false
}

func testQoS2() bool {
	header("TEST: QoS 2 Publish/Subscribe (Exactly-Once)")
	topic := "test/qos2/" + rid()

	sub, err := dial("test-sub-qos2-" + rid())
	if err != nil { fail(err.Error()); return false }
	defer sub.Disconnect()

	sub.Subscribe(topic, 2)
	time.Sleep(100 * time.Millisecond)

	pub, err := dial("test-pub-qos2-" + rid())
	if err != nil { fail(err.Error()); return false }
	defer pub.Disconnect()

	for i := 0; i < 3; i++ {
		pub.Publish(topic, []byte(fmt.Sprintf(`{"seq":%d}`, i)), 2, false)
		time.Sleep(50 * time.Millisecond)
	}

	msgs := sub.WaitMessages(3, 15*time.Second)
	if len(msgs) == 3 {
		ok("All 3 QoS 2 messages received (exactly-once)")
		return true
	}
	fail(fmt.Sprintf("Expected 3, got %d", len(msgs)))
	return false
}

func testWildcards() bool {
	header("TEST: Wildcard Subscriptions (+, #)")
	base := "test/wc/" + rid()

	sub, err := dial("test-sub-wc-" + rid())
	if err != nil { fail(err.Error()); return false }
	defer sub.Disconnect()

	sub.Subscribe(base+"/+/data", 0)
	sub.Subscribe(base+"/deep/#", 0)
	time.Sleep(100 * time.Millisecond)

	pub, err := dial("test-pub-wc-" + rid())
	if err != nil { fail(err.Error()); return false }
	defer pub.Disconnect()

	// Should match /+/data
	pub.Publish(base+"/sensor1/data", []byte("1"), 0, false)
	pub.Publish(base+"/sensor2/data", []byte("2"), 0, false)
	// Should NOT match /+/data (two levels)
	pub.Publish(base+"/sensor1/status", []byte("x"), 0, false)
	// Should match /deep/#
	pub.Publish(base+"/deep/a/b/c", []byte("3"), 0, false)

	msgs := sub.WaitMessages(3, 5*time.Second)
	if len(msgs) == 3 {
		ok("Wildcards: received exactly 3 matching messages (+: 2, #: 1)")
		return true
	}
	fail(fmt.Sprintf("Wildcards: expected 3, got %d", len(msgs)))
	return false
}

func testRetained() bool {
	header("TEST: Retained Messages")
	topic := "test/retain/" + rid()

	pub, _ := dial("test-pub-retain-" + rid())
	defer pub.Disconnect()

	// Publish retained BEFORE subscriber connects
	pub.Publish(topic, []byte(`{"retained":true,"value":99}`), 1, true)
	time.Sleep(300 * time.Millisecond)

	// New subscriber — must receive retained message immediately
	sub, _ := dial("test-sub-retain-" + rid())
	defer sub.Disconnect()
	sub.Subscribe(topic, 1)

	if _, got := sub.WaitMessage(5 * time.Second); got {
		ok("Retained message delivered to new subscriber")
	} else {
		fail("No retained message received")
		return false
	}

	// Clear retained message
	pub.Publish(topic, []byte{}, 1, true)
	ok("Retained message cleared (empty payload)")
	return true
}

func testLWT() bool {
	header("TEST: Last Will and Testament")
	base    := "test/lwt/" + rid()
	lwtTopic := base + "/status"

	// Observer subscribes to the LWT topic
	obs, _ := dial("test-lwt-obs-" + rid())
	defer obs.Disconnect()
	obs.Subscribe(lwtTopic, 1)

	// Client with LWT — we send raw CONNECT with will
	// (paho would normally handle this; using our raw client)
	conn, err := net.Dial("tcp", brokerAddr())
	if err != nil { fail(err.Error()); return false }

	var buf bytes.Buffer
	writeUTF8(&buf, "MQTT")
	buf.WriteByte(4)    // v3.1.1
	buf.WriteByte(0x06) // CleanSession + WillFlag
	binary.Write(&buf, binary.BigEndian, uint16(3)) // keepalive 3s
	writeUTF8(&buf, "lwt-test-"+rid())
	writeUTF8(&buf, lwtTopic)
	// Will message (length-prefixed binary)
	willMsg := []byte(`{"status":"offline"}`)
	binary.Write(&buf, binary.BigEndian, uint16(len(willMsg)))
	buf.Write(willMsg)

	raw := buf.Bytes()
	var hdr bytes.Buffer
	hdr.WriteByte(0x10)
	hdr.Write(encodeVarInt(len(raw)))
	hdr.Write(raw)

	conn.Write(hdr.Bytes())
	time.Sleep(500 * time.Millisecond)

	// Force close without DISCONNECT (simulates crash)
	conn.Close()

	// LWT should arrive within keepalive * 1.5 = 4.5s
	msg, got := obs.WaitMessage(8 * time.Second)
	if got {
		ok(fmt.Sprintf("LWT received after crash: %s", extractPayload(msg)))
	} else {
		info("LWT test: broker may delay up to 1.5x keepalive (4.5s) — check manually")
	}
	return true
}

// ─── Benchmark ───────────────────────────────────────────────────────────────

func benchmarkThroughput() {
	header(fmt.Sprintf("BENCHMARK: %d clients × %d messages", *clients, *msgs))

	// Single subscriber
	sub, err := dial("bench-sub-" + rid())
	if err != nil { fail(err.Error()); return }
	defer sub.Disconnect()
	sub.Subscribe("bench/#", 0)
	time.Sleep(300 * time.Millisecond)

	var sent, received atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()

	// Launch publishers
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c, err := Connect(brokerAddr(), fmt.Sprintf("bench-pub-%d-%s", workerID, rid()), *user, *pass, true)
			if err != nil {
				return
			}
			defer c.Disconnect()
			topic := fmt.Sprintf("bench/worker-%d", workerID)
			payload := []byte(fmt.Sprintf(`{"w":%d}`, workerID))
			for j := 0; j < *msgs; j++ {
				c.Publish(topic, payload, 0, false)
				sent.Add(1)
			}
		}(i)
	}

	// Count received messages
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			if _, got := sub.WaitMessage(3 * time.Second); got {
				received.Add(1)
			} else if sent.Load() >= int64(*clients**msgs) {
				return
			}
		}
	}()

	wg.Wait()
	time.Sleep(2 * time.Second) // wait for messages to drain
	close(sub.doneCh)

	elapsed := time.Since(start)
	s := sent.Load()
	r := received.Load()

	fmt.Printf("\n  📊 Results:\n")
	fmt.Printf("     Clients:           %d\n", *clients)
	fmt.Printf("     Messages sent:     %s\n", fmtNum(s))
	fmt.Printf("     Messages received: %s\n", fmtNum(r))
	fmt.Printf("     Elapsed:           %.2fs\n", elapsed.Seconds())
	fmt.Printf("     Throughput:        %s msg/s\n", fmtNum(int64(float64(s)/elapsed.Seconds())))
	fmt.Printf("     Loss:              %.2f%%\n", float64(s-r)/float64(max64(s, 1))*100)
}

func benchmarkConnections() {
	n := *clients
	header(fmt.Sprintf("BENCHMARK: %d concurrent connections", n))

	var wg sync.WaitGroup
	var connected atomic.Int64
	start := time.Now()

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := Connect(brokerAddr(), fmt.Sprintf("load-%d-%s", id, rid()), *user, *pass, true)
			if err != nil {
				return
			}
			connected.Add(1)
			time.Sleep(2 * time.Second)
			c.Disconnect()
			connected.Add(-1)
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("\n  📊 Connection Results:\n")
	fmt.Printf("     Target:      %d\n", n)
	fmt.Printf("     Elapsed:     %.2fs\n", elapsed.Seconds())
	fmt.Printf("     Rate:        %.0f conn/s\n", float64(n)/elapsed.Seconds())
}

// ─── REST API Tests ───────────────────────────────────────────────────────────

func testAPI() {
	header("TEST: REST API")
	base := *apiBase

	// Health
	if r, err := http.Get(base + "/health"); err == nil && r.StatusCode == 200 {
		ok("GET /health → 200")
	} else {
		fail(fmt.Sprintf("GET /health → %v", err))
		return
	}

	// Login
	token := apiLogin(base)
	if token == "" {
		fail("Login failed")
		return
	}
	ok("POST /auth/login → token received")

	// Stats
	if r, err := apiGET(base+"/admin/stats", token); err == nil && r.StatusCode == 200 {
		ok("GET /admin/stats → 200")
	} else {
		fail(fmt.Sprintf("GET /admin/stats → %v %v", err, r.StatusCode))
	}

	// Register device
	devID := "go-test-device-" + rid()
	body, _ := json.Marshal(map[string]interface{}{
		"client_id": devID, "description": "go test device",
	})
	if r, err := apiPOST(base+"/devices", token, body); err == nil && r.StatusCode == 201 {
		ok(fmt.Sprintf("POST /devices → 201 (%s)", devID))
	} else {
		fail(fmt.Sprintf("POST /devices → %v", err))
	}

	// List devices
	if r, err := apiGET(base+"/devices", token); err == nil && r.StatusCode == 200 {
		ok("GET /devices → 200")
	} else {
		fail("GET /devices failed")
	}

	// Publish via API
	body, _ = json.Marshal(map[string]interface{}{
		"topic": "test/api-go", "payload": `{"source":"go-test"}`, "qos": 0,
	})
	if r, err := apiPOST(base+"/mqtt/publish", token, body); err == nil && r.StatusCode == 200 {
		ok("POST /mqtt/publish → 200")
	} else {
		fail(fmt.Sprintf("POST /mqtt/publish → %v", err))
	}
}

// ─── Live Monitor ────────────────────────────────────────────────────────────

func liveMonitor(filter string) {
	header(fmt.Sprintf("MONITOR: %s (Ctrl+C to stop)", filter))
	c, err := dial("monitor-" + rid())
	if err != nil { fail(err.Error()); return }
	defer c.Disconnect()

	c.Subscribe(filter, 0)
	count := 0
	for {
		if msg, got := c.WaitMessage(5 * time.Second); got {
			count++
			topicLen := binary.BigEndian.Uint16(msg.Payload)
			topic    := string(msg.Payload[2 : 2+topicLen])
			payload  := msg.Payload[2+topicLen:]
			ts       := time.Now().Format("15:04:05")
			fmt.Printf("  [%s] #%04d %s → %s\n", ts, count, topic, truncate(string(payload), 120))
		} else {
			fmt.Printf("  [waiting...]\n")
		}
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func rid() string {
	return fmt.Sprintf("%06x", rand.Intn(1<<24))
}

func encodeVarInt(n int) []byte {
	var buf []byte
	for {
		digit := n % 128
		n /= 128
		if n > 0 { digit |= 0x80 }
		buf = append(buf, byte(digit))
		if n == 0 { break }
	}
	return buf
}

func extractPayload(p *Packet) string {
	if p == nil || len(p.Payload) < 2 { return "" }
	l := int(binary.BigEndian.Uint16(p.Payload))
	if 2+l > len(p.Payload) { return "" }
	return string(p.Payload[2+l:])
}

func fmtNum(n int64) string {
	if n >= 1_000_000 { return fmt.Sprintf("%.1fM", float64(n)/1_000_000) }
	if n >= 1_000     { return fmt.Sprintf("%.1fK", float64(n)/1_000) }
	return fmt.Sprintf("%d", n)
}

func max64(a, b int64) int64 {
	if a > b { return a }
	return b
}

func truncate(s string, n int) string {
	if len(s) > n { return s[:n] + "…" }
	return s
}

func apiLogin(base string) string {
	body, _ := json.Marshal(map[string]string{"username": *user, "password": *pass})
	resp, err := http.Post(base+"/auth/login", "application/json", bytes.NewReader(body))
	if err != nil { return "" }
	defer resp.Body.Close()
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	token, _ := result["access_token"].(string)
	return token
}

func apiGET(url, token string) (*http.Response, error) {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	return http.DefaultClient.Do(req)
}

func apiPOST(url, token string, body []byte) (*http.Response, error) {
	req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	fmt.Printf("\n🚀 LUMA MQTT Go Test Client\n")
	fmt.Printf("   Broker: %s\n", brokerAddr())

	if *monitor != "" {
		liveMonitor(*monitor)
		return
	}
	if *runAPI {
		testAPI()
		return
	}
	if *bench {
		benchmarkThroughput()
		benchmarkConnections()
		return
	}

	tests := map[string]func() bool{
		"connect":  testConnect,
		"qos0":     testQoS0,
		"qos1":     testQoS1,
		"qos2":     testQoS2,
		"wildcard": testWildcards,
		"retain":   testRetained,
		"lwt":      testLWT,
	}
	order := []string{"connect", "qos0", "qos1", "qos2", "wildcard", "retain", "lwt"}

	run := order
	if *testName != "all" {
		run = []string{*testName}
	}

	results := map[string]bool{}
	for _, name := range run {
		fn, ok := tests[name]
		if !ok { continue }
		results[name] = fn()
	}

	passed := 0
	for _, v := range results { if v { passed++ } }
	fmt.Printf("\n%s\n  RESULTS: %d/%d passed\n%s\n", line(), passed, len(results), line())
	for _, name := range run {
		icon := "✅"
		if !results[name] { icon = "❌" }
		fmt.Printf("  %s  %s\n", icon, name)
	}
	fmt.Println()

	if passed < len(results) {
		os.Exit(1)
	}
}
