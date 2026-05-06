// Package router implements a sharded topic trie for high-concurrency MQTT routing.
//
// Performance design:
//   - 256 shards keyed by the first byte of the root segment's hash.
//     Most workloads distribute across shards; a single hot topic namespace
//     still lands in one shard but avoids contention with all others.
//   - Per-node RWMutex: reads (match) take shared lock, writes (sub/unsub) exclusive.
//   - copy-on-write subscriber maps: the match path copies nothing; the write
//     path swaps the map pointer atomically after building the new version.
//   - Retained messages stored in a separate sync.Map (topic → *Retained).
package router

import (
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luma/broker/internal/metrics"
	"github.com/luma/broker/internal/protocol"
	"go.uber.org/zap"
)

const numShards = 256

// ─── Subscriber ───────────────────────────────────────────────────────────────

// DeliverFn is called to deliver a message to a subscribed client.
// It must be non-blocking; slow clients buffer in their write queue.
type DeliverFn func(topic string, payload []byte, qos byte, retain bool, packetID uint16)

// Subscriber holds per-subscription state stored in the trie.
type Subscriber struct {
	ClientID string
	QoS      byte
	Deliver  DeliverFn
}

// ─── Retained Message ────────────────────────────────────────────────────────

// Retained is a stored retained message.
type Retained struct {
	Topic     string
	Payload   []byte
	QoS       byte
	Timestamp time.Time
}

// ─── Trie Node ────────────────────────────────────────────────────────────────

type node struct {
	mu       sync.RWMutex
	children map[string]*node
	// subscribers is a copy-on-write map: clientID → *Subscriber
	// Protected by mu; replaced atomically.
	subscribers map[string]*Subscriber
}

func newNode() *node {
	return &node{
		children:    make(map[string]*node, 4),
		subscribers: make(map[string]*Subscriber),
	}
}

// ─── Shard ───────────────────────────────────────────────────────────────────

type shard struct {
	root *node
}

func newShard() *shard {
	return &shard{root: newNode()}
}

func (s *shard) subscribe(segments []string, sub *Subscriber) {
	n := s.root
	for _, seg := range segments {
		n.mu.Lock()
		child, ok := n.children[seg]
		if !ok {
			child = newNode()
			n.children[seg] = child
		}
		n.mu.Unlock()
		n = child
	}
	// Copy-on-write: build new map, swap under lock
	n.mu.Lock()
	newMap := make(map[string]*Subscriber, len(n.subscribers)+1)
	for k, v := range n.subscribers {
		newMap[k] = v
	}
	newMap[sub.ClientID] = sub
	n.subscribers = newMap
	n.mu.Unlock()
}

func (s *shard) unsubscribe(segments []string, clientID string) {
	s.unsubRecursive(s.root, segments, 0, clientID)
}

func (s *shard) unsubRecursive(n *node, segs []string, depth int, clientID string) bool {
	if depth == len(segs) {
		n.mu.Lock()
		if _, exists := n.subscribers[clientID]; exists {
			newMap := make(map[string]*Subscriber, len(n.subscribers)-1)
			for k, v := range n.subscribers {
				if k != clientID {
					newMap[k] = v
				}
			}
			n.subscribers = newMap
		}
		empty := len(n.children) == 0 && len(n.subscribers) == 0
		n.mu.Unlock()
		return empty
	}

	seg := segs[depth]
	n.mu.Lock()
	child, ok := n.children[seg]
	n.mu.Unlock()
	if !ok {
		return false
	}

	if s.unsubRecursive(child, segs, depth+1, clientID) {
		n.mu.Lock()
		delete(n.children, seg)
		empty := len(n.children) == 0 && len(n.subscribers) == 0
		n.mu.Unlock()
		return empty
	}
	return false
}

// match collects all subscribers whose filter matches topic segments.
// Results are merged into the provided map (highest QoS wins per client).
func (s *shard) match(topicSegs []string, out map[string]*Subscriber) {
	matchNode(s.root, topicSegs, 0, out)
}

func matchNode(n *node, segs []string, depth int, out map[string]*Subscriber) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// '#' at current level matches everything remaining
	if hChild, ok := n.children["#"]; ok {
		hChild.mu.RLock()
		mergeSubscribers(hChild.subscribers, out)
		hChild.mu.RUnlock()
	}

	if depth == len(segs) {
		mergeSubscribers(n.subscribers, out)
		return
	}

	seg := segs[depth]

	// Exact segment match
	if child, ok := n.children[seg]; ok {
		matchNode(child, segs, depth+1, out)
	}
	// '+' single-level wildcard
	if pChild, ok := n.children["+"]; ok {
		matchNode(pChild, segs, depth+1, out)
	}
}

func mergeSubscribers(src, dst map[string]*Subscriber) {
	for id, sub := range src {
		if existing, ok := dst[id]; !ok || sub.QoS > existing.QoS {
			dst[id] = sub
		}
	}
}

// removeClient removes a client from every node in the subtree.
func removeClientFromNode(n *node, clientID string) {
	n.mu.Lock()
	if _, ok := n.subscribers[clientID]; ok {
		newMap := make(map[string]*Subscriber, len(n.subscribers)-1)
		for k, v := range n.subscribers {
			if k != clientID {
				newMap[k] = v
			}
		}
		n.subscribers = newMap
	}
	// Snapshot children to avoid holding lock during recursion
	children := make([]*node, 0, len(n.children))
	for _, c := range n.children {
		children = append(children, c)
	}
	n.mu.Unlock()

	for _, c := range children {
		removeClientFromNode(c, clientID)
	}
}

// getFilters collects all topic filters a client is subscribed to.
func collectFilters(n *node, prefix string, clientID string, out *[]string) {
	n.mu.RLock()
	if _, ok := n.subscribers[clientID]; ok {
		*out = append(*out, prefix)
	}
	children := make(map[string]*node, len(n.children))
	for k, v := range n.children {
		children[k] = v
	}
	n.mu.RUnlock()

	for seg, child := range children {
		var next string
		if prefix == "" {
			next = seg
		} else {
			next = prefix + "/" + seg
		}
		collectFilters(child, next, clientID, out)
	}
}

// ─── Router ───────────────────────────────────────────────────────────────────

// sharedGroup is one MQTT v5 shared subscription group ($share/name/filter).
type sharedGroup struct {
	shareName   string
	topicFilter string
	mu          sync.Mutex
	subs        map[string]*Subscriber
	rr          atomic.Uint64
}

// Router is the central publish-subscribe engine.
// It manages topic subscriptions, retained messages, and message delivery.
type Router struct {
	shards   [numShards]*shard
	retained sync.Map // topic(string) → *Retained

	sharedMu sync.RWMutex
	shared   map[string]*sharedGroup // key: shareName + "\x00" + topicFilter

	retainedCount atomic.Int64
	logger        *zap.Logger
}

// New creates a Router with all shards initialized.
func New(logger *zap.Logger) *Router {
	r := &Router{
		logger: logger,
		shared: make(map[string]*sharedGroup),
	}
	for i := range r.shards {
		r.shards[i] = newShard()
	}
	return r
}

func sharedStoreKey(shareName, topicFilter string) string {
	return shareName + "\x00" + topicFilter
}

func sharedKeyToFilter(key string) string {
	i := strings.IndexByte(key, 0)
	if i < 0 {
		return ""
	}
	return "$share/" + key[:i] + "/" + key[i+1:]
}

// shardFor picks a shard based on the first segment of the filter/topic.
func (r *Router) shardFor(filter string) *shard {
	h := fnv.New32a()
	slash := strings.IndexByte(filter, '/')
	if slash == -1 {
		h.Write([]byte(filter))
	} else {
		h.Write([]byte(filter[:slash]))
	}
	return r.shards[h.Sum32()&(numShards-1)]
}

// Subscribe registers a subscription in the trie.
func (r *Router) Subscribe(filter string, sub *Subscriber) {
	if shareName, topicFilter, ok := protocol.ParseSharedTopicFilter(filter); ok {
		r.subscribeShared(shareName, topicFilter, sub)
		metrics.ActiveSubscriptions.Inc()
		metrics.SubscribeTotal.Inc()
		return
	}
	segs := splitFilter(filter)
	r.shardFor(filter).subscribe(segs, sub)
	metrics.ActiveSubscriptions.Inc()
	metrics.SubscribeTotal.Inc()
}

func (r *Router) subscribeShared(shareName, topicFilter string, sub *Subscriber) {
	key := sharedStoreKey(shareName, topicFilter)
	r.sharedMu.Lock()
	g := r.shared[key]
	if g == nil {
		g = &sharedGroup{
			shareName:   shareName,
			topicFilter: topicFilter,
			subs:        make(map[string]*Subscriber),
		}
		r.shared[key] = g
	}
	r.sharedMu.Unlock()

	g.mu.Lock()
	g.subs[sub.ClientID] = sub
	g.mu.Unlock()
}

// Unsubscribe removes a single client subscription.
func (r *Router) Unsubscribe(filter, clientID string) {
	if shareName, topicFilter, ok := protocol.ParseSharedTopicFilter(filter); ok {
		r.unsubscribeShared(shareName, topicFilter, clientID)
		return
	}
	segs := splitFilter(filter)
	r.shardFor(filter).unsubscribe(segs, clientID)
	metrics.ActiveSubscriptions.Dec()
}

func (r *Router) unsubscribeShared(shareName, topicFilter, clientID string) {
	key := sharedStoreKey(shareName, topicFilter)
	r.sharedMu.Lock()
	g := r.shared[key]
	if g == nil {
		r.sharedMu.Unlock()
		return
	}
	g.mu.Lock()
	delete(g.subs, clientID)
	empty := len(g.subs) == 0
	g.mu.Unlock()
	if empty {
		delete(r.shared, key)
	}
	r.sharedMu.Unlock()
	metrics.ActiveSubscriptions.Dec()
}

// UnsubscribeAll removes all subscriptions for a client across every shard.
func (r *Router) UnsubscribeAll(clientID string) {
	for _, sh := range r.shards {
		removeClientFromNode(sh.root, clientID)
	}
	n := r.removeClientFromAllShared(clientID)
	for i := 0; i < n; i++ {
		metrics.ActiveSubscriptions.Dec()
	}
}

func (r *Router) removeClientFromAllShared(clientID string) int {
	removed := 0
	r.sharedMu.Lock()
	for key, g := range r.shared {
		g.mu.Lock()
		if _, ok := g.subs[clientID]; ok {
			delete(g.subs, clientID)
			removed++
		}
		empty := len(g.subs) == 0
		g.mu.Unlock()
		if empty {
			delete(r.shared, key)
		}
	}
	r.sharedMu.Unlock()
	return removed
}

// GetFilters returns all topic filters a client is subscribed to.
func (r *Router) GetFilters(clientID string) []string {
	var filters []string
	for _, sh := range r.shards {
		collectFilters(sh.root, "", clientID, &filters)
	}
	r.sharedMu.RLock()
	for key, g := range r.shared {
		g.mu.Lock()
		if _, ok := g.subs[clientID]; ok {
			filters = append(filters, sharedKeyToFilter(key))
		}
		g.mu.Unlock()
	}
	r.sharedMu.RUnlock()
	return filters
}

// Publish routes a message to all matching subscribers and handles retention.
// Returns the number of subscribers delivered to.
func (r *Router) Publish(topic string, payload []byte, qos byte, retain bool) int {
	// Handle retained message
	if retain {
		if len(payload) == 0 {
			r.retained.Delete(topic)
			r.retainedCount.Add(-1)
			metrics.RetainedMessages.Dec()
		} else {
			_, loaded := r.retained.Swap(topic, &Retained{
				Topic:     topic,
				Payload:   append([]byte(nil), payload...), // copy for safety
				QoS:       qos,
				Timestamp: time.Now(),
			})
			if !loaded {
				r.retainedCount.Add(1)
				metrics.RetainedMessages.Inc()
			}
		}
	}

	// Match subscribers (trie + shared subscriptions are handled separately).
	matched := r.matchSubscribers(topic)

	// Deliver to each subscriber
	delivered := 0
	for _, sub := range matched {
		effQoS := qos
		if sub.QoS < effQoS {
			effQoS = sub.QoS
		}
		if sub.Deliver != nil {
			sub.Deliver(topic, payload, effQoS, false, 0)
			delivered++
		}
	}

	delivered += r.deliverShared(topic, payload, qos)

	metrics.DeliveredTotal.Add(float64(delivered))
	return delivered
}

func (r *Router) deliverShared(topic string, payload []byte, qos byte) int {
	n := 0
	r.sharedMu.RLock()
	keys := make([]string, 0, len(r.shared))
	for k := range r.shared {
		keys = append(keys, k)
	}
	r.sharedMu.RUnlock()

	for _, k := range keys {
		r.sharedMu.RLock()
		g := r.shared[k]
		r.sharedMu.RUnlock()
		if g == nil {
			continue
		}
		if !TopicMatchesFilter(topic, g.topicFilter) {
			continue
		}
		g.mu.Lock()
		if len(g.subs) == 0 {
			g.mu.Unlock()
			continue
		}
		ids := make([]string, 0, len(g.subs))
		for id := range g.subs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		next := g.rr.Add(1)
		idx := int((next - 1) % uint64(len(ids)))
		sub := g.subs[ids[idx]]
		g.mu.Unlock()

		effQoS := qos
		if sub.QoS < effQoS {
			effQoS = sub.QoS
		}
		if sub.Deliver != nil {
			sub.Deliver(topic, payload, effQoS, false, 0)
			n++
		}
	}
	return n
}

// matchSubscribers returns matched subscribers by splitting across all relevant shards.
func (r *Router) matchSubscribers(topic string) map[string]*Subscriber {
	segs := splitFilter(topic)
	result := make(map[string]*Subscriber, 8)

	// A topic can match subscriptions across all shards because different
	// prefixes map to different shards. We must check all shards.
	for _, sh := range r.shards {
		sh.match(segs, result)
	}
	return result
}

// SendRetained delivers retained messages matching a filter to a new subscriber.
func (r *Router) SendRetained(filter string, sub *Subscriber) {
	matchFilter := filter
	if _, inner, ok := protocol.ParseSharedTopicFilter(filter); ok {
		matchFilter = inner
	}
	r.retained.Range(func(key, value any) bool {
		topic := key.(string)
		msg := value.(*Retained)
		if TopicMatchesFilter(topic, matchFilter) {
			effQoS := msg.QoS
			if sub.QoS < effQoS {
				effQoS = sub.QoS
			}
			if sub.Deliver != nil {
				sub.Deliver(topic, msg.Payload, effQoS, true, 0)
			}
		}
		return true
	})
}

// SetRetained stores a retained message (e.g., loaded from persistent store on startup).
func (r *Router) SetRetained(msg *Retained) {
	_, loaded := r.retained.Swap(msg.Topic, msg)
	if !loaded {
		r.retainedCount.Add(1)
		metrics.RetainedMessages.Inc()
	}
}

// GetRetained returns the retained message for an exact topic, if any.
func (r *Router) GetRetained(topic string) *Retained {
	v, ok := r.retained.Load(topic)
	if !ok {
		return nil
	}
	return v.(*Retained)
}

// AllRetained iterates all retained messages.
func (r *Router) AllRetained(fn func(*Retained)) {
	r.retained.Range(func(_, v any) bool {
		fn(v.(*Retained))
		return true
	})
}

// Stats returns router statistics.
func (r *Router) Stats() map[string]int64 {
	return map[string]int64{
		"retained_messages": r.retainedCount.Load(),
	}
}

// ─── Topic Filter Matching ────────────────────────────────────────────────────

// TopicMatchesFilter checks if a concrete topic matches a filter (for retained delivery).
func TopicMatchesFilter(topic, filter string) bool {
	tSegs := splitFilter(topic)
	fSegs := splitFilter(filter)
	return matchSegs(tSegs, fSegs, 0, 0)
}

func matchSegs(t, f []string, ti, fi int) bool {
	for fi < len(f) {
		seg := f[fi]
		if seg == "#" {
			return true
		}
		if ti >= len(t) {
			return false
		}
		if seg != "+" && seg != t[ti] {
			return false
		}
		ti++
		fi++
	}
	return ti == len(t)
}

func splitFilter(f string) []string {
	if f == "" {
		return []string{""}
	}
	return strings.Split(f, "/")
}
