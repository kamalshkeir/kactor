package kactor

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/ksmux/ws"
	"github.com/kamalshkeir/ksmux/wspool"
)

// NewPubSub creates a new PubSub
func NewPubSub() *PubSub {
	ps := &PubSub{
		subscribers: kmap.New[string, *psSubList](32),
		patterns:    kmap.New[string, *psSubList](32),
		pendingMsgs: kmap.New[string, *PublishOptions](32),
	}

	ps.msgPool = sync.Pool{
		New: func() any {
			return &psMessage{
				payload: make(map[string]any, 8),
				idBuf:   make([]byte, 0, 64),
			}
		},
	}

	ps.payloadPool = sync.Pool{
		New: func() any {
			return make(map[string]any, 8)
		},
	}

	ps.idPool = sync.Pool{
		New: func() any {
			return make([]byte, 0, 64)
		},
	}

	ps.subsPool = sync.Pool{
		New: func() any {
			return make([]string, 0, 32)
		},
	}

	ps.bufPool = sync.Pool{
		New: func() any {
			return make([]byte, 0, 64)
		},
	}

	return ps
}

// SetDebug enables or disables debug logging
func (ps *PubSub) WithDebug(enabled bool) {
	ps.debug = enabled
}

// debugLog prints debug messages if debug mode is enabled
func (ps *PubSub) debugLog(format string, args ...interface{}) {
	fmt.Printf("[PubSub] "+format+"\n", args...)
}

// GetTopic implements the Subscription interface
func (s *psSubscription) GetTopic() string {
	return s.topic
}

// GetSubID implements the Subscription interface
func (s *psSubscription) GetSubID() string {
	return s.subID
}

// matchTopic is an optimized pattern matching implementation
func (ps *PubSub) matchTopic(pattern, topic string) bool {
	// Fast path: exact match
	if pattern == topic {
		return true
	}

	// Fast path: no wildcards
	hasWildcard := false
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '*' || pattern[i] == '+' {
			hasWildcard = true
			break
		}
	}
	if !hasWildcard {
		return false
	}

	// Full pattern matching with minimal branching
	var i, j int
	for i < len(pattern) && j < len(topic) {
		switch pattern[i] {
		case '*':
			return true
		case '+':
			// Skip to next segment
			for i < len(pattern) && pattern[i] != '/' {
				i++
			}
			for j < len(topic) && topic[j] != '/' {
				j++
			}
			i++
			j++
		default:
			if pattern[i] != topic[j] {
				return false
			}
			i++
			j++
		}
	}
	return i == len(pattern) && j == len(topic)
}

// getPayload gets a payload map from the pool
func (ps *PubSub) getPayload() map[string]any {
	m := ps.payloadPool.Get().(map[string]any)
	for k := range m {
		delete(m, k)
	}
	return m
}

// getMessage gets a message from the pool
func (ps *PubSub) getMessage() *psMessage {
	msg := ps.msgPool.Get().(*psMessage)
	if msg.payload == nil {
		msg.payload = ps.getPayload()
	} else {
		// Clear existing payload
		for k := range msg.payload {
			delete(msg.payload, k)
		}
	}
	if msg.idBuf == nil {
		msg.idBuf = ps.idPool.Get().([]byte)
	}
	msg.idBuf = msg.idBuf[:0] // Reset but keep capacity
	msg.pending = 0
	msg.options = nil
	msg.topic = ""
	msg.targetID = ""
	return msg
}

// putMessage returns a message to the pool
func (ps *PubSub) putMessage(msg *psMessage) {
	ps.msgPool.Put(msg)
}

// Subscribe adds a subscriber
func (ps *PubSub) Subscribe(topic, subID string, handler func(map[string]any, Subscription)) Subscription {
	atomic.AddInt32(&ps.userCount, 1)
	if ps.debug {
		ps.debugLog("Subscribing to topic %s with subID %s", topic, subID)
	}

	// Check if it's a pattern subscription
	if strings.ContainsAny(topic, "+*") {
		list := ps.getOrCreateList(ps.patterns, topic)
		list.mu.Lock()
		defer list.mu.Unlock()

		// Check if we already have this subscription
		for i := int32(0); i < list.count; i++ {
			if node := list.subs[i]; node != nil && node.subID == subID {
				return node.sub
			}
		}

		// Find empty slot
		for i := int32(0); i < 32; i++ {
			if list.subs[i] == nil {
				sub := &psSubscription{topic: topic, subID: subID, pubsub: ps}
				list.subs[i] = &psSubNode{handler: handler, subID: subID, sub: sub}
				if i >= list.count {
					atomic.StoreInt32(&list.count, i+1)
				}
				return sub
			}
		}
		return nil
	}

	// Direct subscription
	list := ps.getOrCreateList(ps.subscribers, topic)
	list.mu.Lock()
	defer list.mu.Unlock()

	// Check if we already have this subscription
	for i := int32(0); i < list.count; i++ {
		if node := list.subs[i]; node != nil && node.subID == subID {
			if ps.debug {
				ps.debugLog("ALready subscribed to topic %s with subID %s", topic, subID)
			}
			return node.sub
		}
	}

	// Find empty slot
	for i := int32(0); i < 32; i++ {
		if list.subs[i] == nil {
			sub := &psSubscription{topic: topic, subID: subID, pubsub: ps}
			list.subs[i] = &psSubNode{handler: handler, subID: subID, sub: sub}
			if i >= list.count {
				atomic.StoreInt32(&list.count, i+1)
			}
			if ps.debug {
				ps.debugLog("Subscribed with success to topic %s with subID %s", topic, subID)
			}
			return sub
		}
	}
	return nil
}

func (ps *PubSub) getOrCreateList(m *kmap.SafeMap[string, *psSubList], topic string) *psSubList {
	list, exists := m.Get(topic)
	if !exists {
		list = &psSubList{}
		m.Set(topic, list)
	}
	return list
}

// Publish publishes a message
func (ps *PubSub) Publish(topic string, payload map[string]any, opts *PublishOptions) bool {
	if ps.debug {
		ps.debugLog("Publishing message to topic %s", topic)
	}

	success := false
	// Fast path: check if we have any subscribers without locking
	var exactList *psSubList
	var exactExists bool
	if exactList, exactExists = ps.subscribers.Get(topic); exactExists {
		count := atomic.LoadInt32(&exactList.count)
		if count > 0 {
			if ps.debug {
				ps.debugLog("Found %d subscribers for topic %s", count, topic)
			}
			// Use stack allocation for local copy
			var localSubs [32]*psSubNode
			exactList.mu.RLock()
			copy(localSubs[:], exactList.subs[:count])
			exactList.mu.RUnlock()

			// Call handlers outside the lock
			for i := int32(0); i < count; i++ {
				if node := localSubs[i]; node != nil {
					if ps.debug {
						ps.debugLog("Delivering message to subscriber %d for topic %s", i, topic)
					}
					node.handler(payload, node.sub)
				}
			}
			success = true
		}
	}

	// Only check patterns if no exact match found
	if !success && ps.patterns.Len() > 0 {
		// Fast path: check for global wildcard pattern
		if list, exists := ps.patterns.Get("*"); exists {
			count := atomic.LoadInt32(&list.count)
			if count > 0 {
				// Stack allocate subscriber list for zero heap allocation
				var localSubs [32]*psSubNode
				list.mu.RLock()
				copy(localSubs[:], list.subs[:count])
				list.mu.RUnlock()

				// Process subscribers outside lock
				for j := int32(0); j < count; j++ {
					if node := localSubs[j]; node != nil {
						node.handler(payload, node.sub)
					}
				}
				success = true
			}
		}

		// Only check other patterns if still no success
		if !success {
			// Stack allocate pattern matches for zero heap allocation
			var matches struct {
				lists  [4]*psSubList
				counts [4]int32
				size   int
			}

			// Check other patterns
			ps.patterns.Range(func(pattern string, list *psSubList) bool {
				count := atomic.LoadInt32(&list.count)
				if count == 0 {
					return true
				}

				// Fast path: check pattern length first
				plen := len(pattern)
				if plen == 0 {
					return true
				}

				// Fast path: check first and last characters
				if pattern[0] == '*' || (plen > 1 && pattern[plen-1] == '*' &&
					len(topic) >= plen-1 && topic[:plen-1] == pattern[:plen-1]) {
					matches.lists[matches.size] = list
					matches.counts[matches.size] = count
					matches.size++
					if matches.size >= 4 {
						return false
					}
				} else if pattern == topic {
					matches.lists[matches.size] = list
					matches.counts[matches.size] = count
					matches.size++
					return false
				}
				return matches.size < 4
			})

			// Process matches if any
			if matches.size > 0 {
				// Stack allocate subscriber list for zero heap allocation
				var localSubs [32]*psSubNode

				// Process all matches with minimal locking
				for i := 0; i < matches.size; i++ {
					list := matches.lists[i]
					count := matches.counts[i]

					// Fast copy subscribers under lock
					list.mu.RLock()
					copy(localSubs[:], list.subs[:count])
					list.mu.RUnlock()

					// Process subscribers outside lock
					for j := int32(0); j < count; j++ {
						if node := localSubs[j]; node != nil {
							node.handler(payload, node.sub)
						}
					}
				}
				success = true
			}
		}
	}

	// Handle callbacks without allocations
	if opts != nil {
		if success {
			if opts.OnSuccess != nil {
				opts.OnSuccess()
			}
		} else {
			if opts.OnFailure != nil {
				opts.OnFailure(fmt.Errorf("no subscribers found for topic: %s", topic))
			}
		}
	}

	return success
}

// PublishTo publishes a direct message
func (ps *PubSub) PublishTo(topic string, targetID string, payload map[string]any, opts *PublishOptions) bool {
	if ps.debug {
		ps.debugLog("Publishing direct message to topic %s, target %s", topic, targetID)
	}

	found := false
	// Look for subscribers in the specified topic
	if list, exists := ps.subscribers.Get(topic); exists {
		list.mu.RLock()
		count := atomic.LoadInt32(&list.count)
		var localSubs [32]*psSubNode
		copy(localSubs[:], list.subs[:count])
		list.mu.RUnlock()

		// Find subscriber with matching subID
		for i := int32(0); i < count; i++ {
			if node := localSubs[i]; node != nil && node.subID == targetID {
				if ps.debug {
					ps.debugLog("Found target subscriber %s in topic %s", targetID, topic)
				}
				node.handler(payload, node.sub)
				found = true
				break
			}
		}
	}

	if found {
		if opts != nil && opts.OnSuccess != nil {
			opts.OnSuccess()
		}
		return true
	}

	if opts != nil && opts.OnFailure != nil {
		opts.OnFailure(fmt.Errorf("target subscriber not found: %s", targetID))
	}
	return false
}

// PublishWithRetry implements retry logic
func (ps *PubSub) PublishWithRetry(topic string, payload map[string]any, cfg *RetryConfig, opts *PublishOptions) bool {
	if cfg == nil {
		cfg = &DefaultRetryConfig
	}

	// Get message from pool
	msg := ps.getMessage()
	msg.topic = topic
	msg.targetID = ""
	msg.options = opts
	msg.pending = 0

	// Generate message ID directly in buffer without allocations
	msg.idBuf = msg.idBuf[:0] // Reset length but keep capacity
	msg.idBuf = append(msg.idBuf, topic...)
	msg.idBuf = append(msg.idBuf, '-')
	msg.idBuf = strconv.AppendInt(msg.idBuf, time.Now().UnixNano(), 10)
	msg.msgID = msg.idBuf

	// Initialize payload map if needed
	if msg.payload == nil {
		msg.payload = ps.getPayload()
	}

	// Copy payload
	for k, v := range payload {
		msg.payload[k] = v
	}

	success := false
	for i := 0; i < cfg.MaxAttempts; i++ {
		if ps.Publish(topic, msg.payload, opts) {
			success = true
			break
		}
		if i < cfg.MaxAttempts-1 {
			runtime.Gosched()
			// Exponential backoff up to MaxBackoff yields
			backoff := 1 << uint(i)
			if backoff > cfg.MaxBackoff {
				backoff = cfg.MaxBackoff
			}
			for j := 0; j < backoff; j++ {
				runtime.Gosched()
			}
		}
	}

	// Clean up message
	ps.putMessage(msg)
	return success
}

// PublishToWithRetry implements retry logic for direct messages
func (ps *PubSub) PublishToWithRetry(topic string, targetID string, payload map[string]any, cfg *RetryConfig, opts *PublishOptions) bool {
	if cfg == nil {
		cfg = &DefaultRetryConfig
	}

	// Get message from pool
	msg := ps.getMessage()
	msg.topic = topic
	msg.targetID = targetID
	msg.options = opts
	msg.pending = 0

	// Generate message ID directly in buffer without allocations
	msg.idBuf = msg.idBuf[:0] // Reset length but keep capacity
	msg.idBuf = append(msg.idBuf, topic...)
	msg.idBuf = append(msg.idBuf, '-')
	msg.idBuf = append(msg.idBuf, targetID...)
	msg.idBuf = append(msg.idBuf, '-')
	msg.idBuf = strconv.AppendInt(msg.idBuf, time.Now().UnixNano(), 10)
	msg.msgID = msg.idBuf

	// Initialize payload map if needed
	if msg.payload == nil {
		msg.payload = ps.getPayload()
	}

	// Copy payload
	for k, v := range payload {
		msg.payload[k] = v
	}

	success := false
	for i := 0; i < cfg.MaxAttempts; i++ {
		if ps.PublishTo(topic, targetID, msg.payload, opts) {
			success = true
			break
		}
		if i < cfg.MaxAttempts-1 {
			runtime.Gosched()
			// Exponential backoff up to MaxBackoff yields
			backoff := 1 << uint(i)
			if backoff > cfg.MaxBackoff {
				backoff = cfg.MaxBackoff
			}
			for j := 0; j < backoff; j++ {
				runtime.Gosched()
			}
		}
	}

	// Clean up message
	ps.putMessage(msg)
	return success
}

// Close stops all message processing and cleans up resources
func (ps *PubSub) Close() {
	if !atomic.CompareAndSwapInt32(&ps.closed, 0, 1) {
		return
	}

	if ps.debug {
		ps.debugLog("Closing LowAllocPubSub")
	}

	// Clear all subscribers
	ps.subscribers.Flush()
	ps.patterns.Flush()
	ps.pendingMsgs.Flush()

	if ps.debug {
		ps.debugLog("LowAllocPubSub closed")
	}
}

// CleanupConnection cleans up resources associated with a WebSocket connection
func (ps *PubSub) CleanupConnection(conn *wspool.Conn) {
	if ps.debug {
		ps.debugLog("Starting cleanup for connection %p", conn)
	}

	// Clean up direct subscriptions
	ps.subscribers.Range(func(topic string, list *psSubList) bool {
		list.mu.Lock()
		// Find and remove all subscriptions for this connection
		for i := int32(0); i < list.count; i++ {
			if node := list.subs[i]; node != nil && node.conn == conn.WSConn() {
				if ps.debug {
					ps.debugLog("Removing subscription for connection %p from topic %s", conn, topic)
				}
				// Move last element to this position
				lastIdx := atomic.LoadInt32(&list.count) - 1
				list.subs[i] = list.subs[lastIdx]
				list.subs[lastIdx] = nil
				atomic.AddInt32(&list.count, -1)
				atomic.AddInt32(&ps.userCount, -1)
				i-- // Recheck this position since we moved an element here
			}
		}
		list.mu.Unlock()

		// If no more subscriptions for this topic, remove the topic
		if atomic.LoadInt32(&list.count) == 0 {
			if ps.debug {
				ps.debugLog("Removing empty topic %s", topic)
			}
			ps.subscribers.Delete(topic)
		}
		return true
	})

	// Clean up pattern subscriptions
	ps.patterns.Range(func(pattern string, list *psSubList) bool {
		list.mu.Lock()
		// Find and remove all subscriptions for this connection
		for i := int32(0); i < list.count; i++ {
			if node := list.subs[i]; node != nil && node.conn == conn.WSConn() {
				if ps.debug {
					ps.debugLog("Removing pattern subscription for connection %p from pattern %s", conn, pattern)
				}
				// Move last element to this position
				lastIdx := atomic.LoadInt32(&list.count) - 1
				list.subs[i] = list.subs[lastIdx]
				list.subs[lastIdx] = nil
				atomic.AddInt32(&list.count, -1)
				atomic.AddInt32(&ps.userCount, -1)
				i-- // Recheck this position since we moved an element here
			}
		}
		list.mu.Unlock()

		// If no more subscriptions for this pattern, remove the pattern
		if atomic.LoadInt32(&list.count) == 0 {
			if ps.debug {
				ps.debugLog("Removing empty pattern %s", pattern)
			}
			ps.patterns.Delete(pattern)
		}
		return true
	})

	if ps.debug {
		ps.debugLog("Finished cleanup for connection %p", conn)
	}
}

// CleanupClient removes all subscriptions for a client
func (ps *PubSub) CleanupClient(clientID string) {
	if ps.debug {
		ps.debugLog("Starting cleanup for client %s", clientID)
	}
	// Clean up direct subscriptions
	ps.subscribers.Range(func(topic string, list *psSubList) bool {
		list.mu.Lock()
		// Find and remove all subscriptions for this client
		for i := int32(0); i < list.count; i++ {
			if node := list.subs[i]; node != nil && node.subID == clientID {
				if ps.debug {
					ps.debugLog("Removing subscription for client %s from topic %s", clientID, topic)
				}
				// Move last element to this position
				lastIdx := atomic.LoadInt32(&list.count) - 1
				list.subs[i] = list.subs[lastIdx]
				list.subs[lastIdx] = nil
				atomic.AddInt32(&list.count, -1)
				i-- // Recheck this position since we moved an element here
			}
		}
		list.mu.Unlock()

		// If no more subscriptions for this topic, remove the topic
		if atomic.LoadInt32(&list.count) == 0 {
			if ps.debug {
				ps.debugLog("Removing empty topic %s", topic)
			}
			ps.subscribers.Delete(topic)
		}
		return true
	})

	// Clean up pattern subscriptions
	ps.patterns.Range(func(pattern string, list *psSubList) bool {
		list.mu.Lock()
		// Find and remove all subscriptions for this client
		for i := int32(0); i < list.count; i++ {
			if node := list.subs[i]; node != nil && node.subID == clientID {
				// Move last element to this position
				lastIdx := atomic.LoadInt32(&list.count) - 1
				list.subs[i] = list.subs[lastIdx]
				list.subs[lastIdx] = nil
				atomic.AddInt32(&list.count, -1)
				i-- // Recheck this position since we moved an element here
			}
		}
		list.mu.Unlock()

		// If no more subscriptions for this pattern, remove the pattern
		if atomic.LoadInt32(&list.count) == 0 {
			ps.patterns.Delete(pattern)
		}
		return true
	})

	// Decrement user count
	atomic.AddInt32(&ps.userCount, -1)
}

// HasSubscribers checks if there are any subscribers for a given topic
func (ps *PubSub) HasSubscribers(topic string) bool {
	if ps.debug {
		ps.debugLog("=== CHECKING SUBSCRIBERS ===")
		ps.debugLog("Checking subscribers for topic: %s", topic)
	}

	// Fast path: check direct subscribers first with read lock
	if list, exists := ps.subscribers.Get(topic); exists {
		count := atomic.LoadInt32(&list.count)
		if ps.debug {
			ps.debugLog("Found direct subscriber list for topic %s with count %d", topic, count)
		}
		if count > 0 {
			if ps.debug {
				ps.debugLog("Has direct subscribers")
				ps.debugLog("=== CHECKING SUBSCRIBERS END ===")
			}
			return true
		}
	} else if ps.debug {
		ps.debugLog("No direct subscribers found for topic %s", topic)
	}

	// Only check patterns if we have any pattern subscribers
	patternCount := ps.patterns.Len()
	if ps.debug {
		ps.debugLog("Found %d pattern subscribers", patternCount)
	}

	if patternCount > 0 {
		// Fast path: check global wildcard first
		if list, exists := ps.patterns.Get("*"); exists {
			count := atomic.LoadInt32(&list.count)
			if ps.debug {
				ps.debugLog("Found global wildcard (*) subscriber list with count %d", count)
			}
			if count > 0 {
				if ps.debug {
					ps.debugLog("Has global wildcard subscribers")
					ps.debugLog("=== CHECKING SUBSCRIBERS END ===")
				}
				return true
			}
		}

		// Check other patterns only if necessary
		var hasMatch bool
		ps.patterns.Range(func(pattern string, list *psSubList) bool {
			if ps.debug {
				ps.debugLog("Checking pattern: %s", pattern)
			}
			if pattern != "*" && // Skip global wildcard as we already checked it
				atomic.LoadInt32(&list.count) > 0 {
				if ps.debug {
					ps.debugLog("Pattern %s has %d subscribers", pattern, list.count)
				}
				if ps.matchTopic(pattern, topic) {
					if ps.debug {
						ps.debugLog("Pattern %s matches topic %s", pattern, topic)
					}
					hasMatch = true
					return false // Stop iteration early
				}
			}
			return true
		})

		if ps.debug {
			if hasMatch {
				ps.debugLog("Found matching pattern subscriber")
			} else {
				ps.debugLog("No matching pattern subscribers found")
			}
			ps.debugLog("=== CHECKING SUBSCRIBERS END ===")
		}
		return hasMatch
	}

	if ps.debug {
		ps.debugLog("No subscribers found")
		ps.debugLog("=== CHECKING SUBSCRIBERS END ===")
	}
	return false
}

// SubscribeWS adds a subscriber with a WebSocket connection reference
func (ps *PubSub) SubscribeWS(topic, subID string, handler func(map[string]any, Subscription), conn *ws.Conn) Subscription {
	atomic.AddInt32(&ps.userCount, 1)
	if ps.debug {
		ps.debugLog("Subscribing to topic %s with subID %s", topic, subID)
	}

	// Check if it's a pattern subscription
	if strings.ContainsAny(topic, "+*") {
		list := ps.getOrCreateList(ps.patterns, topic)
		list.mu.Lock()
		defer list.mu.Unlock()

		// Check if we already have this subscription
		for i := int32(0); i < list.count; i++ {
			if node := list.subs[i]; node != nil && node.subID == subID {
				// Update the connection reference if it's a WebSocket subscription
				if node.conn != nil {
					node.conn = conn
				}
				return node.sub
			}
		}

		// Find empty slot
		for i := int32(0); i < 32; i++ {
			if list.subs[i] == nil {
				sub := &psSubscription{topic: topic, subID: subID, pubsub: ps}
				list.subs[i] = &psSubNode{handler: handler, subID: subID, sub: sub, conn: conn}
				if i >= list.count {
					atomic.StoreInt32(&list.count, i+1)
				}
				return sub
			}
		}
		return nil
	}

	// Direct subscription
	list := ps.getOrCreateList(ps.subscribers, topic)
	list.mu.Lock()
	defer list.mu.Unlock()

	// Check if we already have this subscription
	for i := int32(0); i < list.count; i++ {
		if node := list.subs[i]; node != nil && node.subID == subID {
			// Update the connection reference if it's a WebSocket subscription
			if node.conn != nil {
				node.conn = conn
			}
			return node.sub
		}
	}

	// Find empty slot
	for i := int32(0); i < 32; i++ {
		if list.subs[i] == nil {
			sub := &psSubscription{topic: topic, subID: subID, pubsub: ps}
			list.subs[i] = &psSubNode{handler: handler, subID: subID, sub: sub, conn: conn}
			if i >= list.count {
				atomic.StoreInt32(&list.count, i+1)
			}
			return sub
		}
	}
	return nil
}

// Unsubscribe implements the Subscription interface
func (s *psSubscription) Unsubscribe() {
	s.pubsub.Unsubscribe(s.topic, s.subID)
}

// Unsubscribe removes a subscription for a given topic and subID
func (ps *PubSub) Unsubscribe(topic, subID string) bool {

	if atomic.LoadInt32(&ps.closed) == 1 {
		return false
	}
	if ps.debug {
		ps.debugLog("Unsubscribing from topic %s with subID %s", topic, subID)
	}

	// Check patterns first for better performance in most cases
	if list, exists := ps.patterns.Get(topic); exists {
		list.mu.Lock()
		for i := int32(0); i < list.count; i++ {
			if node := list.subs[i]; node != nil && node.subID == subID {
				// Move last element to this position for O(1) removal
				lastIdx := atomic.LoadInt32(&list.count) - 1
				list.subs[i] = list.subs[lastIdx]
				list.subs[lastIdx] = nil
				atomic.AddInt32(&list.count, -1)
				atomic.AddInt32(&ps.userCount, -1)
				list.mu.Unlock()

				// Remove empty list
				if atomic.LoadInt32(&list.count) == 0 {
					ps.patterns.Delete(topic)
				}
				return true
			}
		}
		list.mu.Unlock()
	}

	// Check direct subscribers
	if list, exists := ps.subscribers.Get(topic); exists {
		list.mu.Lock()
		for i := int32(0); i < list.count; i++ {
			if node := list.subs[i]; node != nil && node.subID == subID {
				// Move last element to this position for O(1) removal
				lastIdx := atomic.LoadInt32(&list.count) - 1
				list.subs[i] = list.subs[lastIdx]
				list.subs[lastIdx] = nil
				atomic.AddInt32(&list.count, -1)
				atomic.AddInt32(&ps.userCount, -1)
				list.mu.Unlock()

				// Remove empty list
				if atomic.LoadInt32(&list.count) == 0 {
					ps.subscribers.Delete(topic)
				}
				return true
			}
		}
		list.mu.Unlock()
	}

	return false
}

// PS C:\Users\kamal\Desktop\kactor> go test -bench=^. -benchmem
// goos: windows
// goarch: amd64
// pkg: github.com/kamalshkeir/kactor
// cpu: Intel(R) Core(TM) i5-7300HQ CPU @ 2.50GHz
// BenchmarkLowAllocPubSub/Single_Publisher_Single_Subscriber-4            21832273                52.05 ns/op       21832273 msgs_received               0 B/op          0 allocs/op
// BenchmarkLowAllocPubSub/Single_Publisher_Multiple_Subscribers-4          4073089               298.5 ns/op       130338848 msgs_received               0 B/op          0 allocs/op
// BenchmarkLowAllocPubSub/Multiple_Publishers_Single_Subscriber-4         12793176                94.90 ns/op       12793176 msgs_received               0 B/op          0 allocs/op
// BenchmarkLowAllocPubSub/Multiple_Publishers_Multiple_Subscribers-4       1980909               678.0 ns/op        63389088 msgs_received               0 B/op          0 allocs/op
// BenchmarkLowAllocPubSub/Direct_Message_Performance-4                    26440161                42.75 ns/op       26440161 msgs_received               0 B/op          0 allocs/op
// BenchmarkLowAllocPubSubRetry/Default_Retry_Config-4                      8233828               157.8 ns/op         8233828 msgs_received               0 B/op          0 allocs/op
// BenchmarkLowAllocPubSubRetry/Aggressive_Retry-4                          8105806               147.8 ns/op         8105806 msgs_received               0 B/op          0 allocs/op
// BenchmarkLowAllocPubSubRetry/Light_Retry-4                               8186169               155.7 ns/op         8186169 msgs_received               0 B/op          0 allocs/op
// PASS
// ok      github.com/kamalshkeir/kactor   11.693s
