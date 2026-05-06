package broker

import "github.com/luma/broker/internal/router"

// ForceDisconnect closes this client's connection from the API.
func (c *Client) ForceDisconnect() {
	c.sess.WillSet = false
	c.conn.Close()
}

// RouterAllRetained exposes the router's retained messages to the API layer.
func (b *Broker) RouterAllRetained(fn func(topic string, payload []byte, qos byte)) {
	b.router.AllRetained(func(r *router.Retained) {
		fn(r.Topic, r.Payload, r.QoS)
	})
}
