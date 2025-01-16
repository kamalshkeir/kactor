package kactor

// Example usage:
/*
func main() {
	// 1. Create and configure the client
	client, err := NewClient(ClientConfig{
								Address:       "localhost:8080",
								Path:         "/ws",
								Secure:       false,
								ClientID:     "test-client",
								AutoReconnect: true,
								MaxRetries:    5,
								BackoffMin:    100 * time.Millisecond,
								BackoffMax:    5 * time.Second,
							})
							if err != nil {
								log.Fatal("Failed to create client:", err)
							}
							defer client.Close()

							// 2. Subscribe to a topic
							subscription := client.Subscribe("chat", "sub1", func(message map[string]any, sub Subscription) {
								log.Printf("Received message: %v", message)
								// message example: { "data": "Hello, World!" }
							})
							if subscription == nil {
								log.Fatal("Failed to subscribe")
							}
							defer subscription.Unsubscribe()

							// 3. Publish a message to all subscribers of a topic
							client.Publish("chat", map[string]any{
								"data": "Hello, everyone!",
							}, &PublishOptions{
								OnSuccess: func() {
									log.Println("Message received by subscribers")
								},
								OnFailure: func(err error) {
									log.Printf("Failed to publish: %v", err)
								},
							})

							// 4. Send a direct message to a specific client
							client.PublishTo("chat", "other-client-id", map[string]any{
								"data": "Hello, specific client!",
							}, &PublishOptions{
								OnSuccess: func() {
									log.Println("Direct message received by target")
								},
								OnFailure: func(err error) {
									log.Printf("Failed to send direct message: %v", err)
								},
							})

							// 5. Publish with retry (automatically retries on failure)
							client.PublishWithRetry("chat", map[string]any{
								"data": "Important message with retry!",
							}, &RetryConfig{
								MaxAttempts: 3,
								MaxBackoff:  4,
							}, &PublishOptions{
								OnSuccess: func() {
									log.Println("Message with retry received by subscribers")
								},
								OnFailure: func(err error) {
									log.Printf("Failed to publish with retry: %v", err)
								},
							})

							6. PublishToWithRetry
							client.PublishToWithRetry("chat", "browser1", map[string]any{
								"data": "Important message with retry!",
							}, &RetryConfig{
								MaxAttempts: 3,
								MaxBackoff:  4,
							}, &PublishOptions{
								OnSuccess: func() {
									log.Println("Message with retry received by subscribers")
								},
								OnFailure: func(err error) {
									log.Printf("Failed to publish with retry: %v", err)
								},
							})

							client.Close()
*/

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/ksmux/ws"
)

// Client represents a WebSocket client
type Client struct {
	config      ClientConfig
	conn        *ws.Conn
	mu          sync.RWMutex
	isConnected bool
	retryCount  int
	handlers    *kmap.SafeMap[string, func(map[string]any, Subscription)]
	pending     *kmap.SafeMap[string, chan *WSMessage]
}

// ClientConfig holds the configuration for the WebSocket client
type ClientConfig struct {
	Address       string
	Path          string
	Secure        bool
	ClientID      string
	AutoReconnect bool
	MaxRetries    int
	BackoffMin    time.Duration
	BackoffMax    time.Duration
}

// NewClient creates a new WebSocket client
func NewClient(config ClientConfig) (*Client, error) {
	if config.ClientID == "" {
		config.ClientID = fmt.Sprintf("client-%d", time.Now().UnixNano())
	}
	if config.Address == "" {
		config.Address = "localhost:9313"
	}
	if config.Path == "" {
		config.Path = "/ws/kactor"
	}

	client := &Client{
		config:   config,
		handlers: kmap.New[string, func(map[string]any, Subscription)](50),
		pending:  kmap.New[string, chan *WSMessage](10),
	}

	err := client.connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	return client, nil
}

func (c *Client) connect() error {
	protocol := "ws"
	if c.config.Secure {
		protocol = "wss"
	}
	url := fmt.Sprintf("%s://%s%s", protocol, c.config.Address, c.config.Path)

	conn, _, err := ws.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.isConnected = true
	c.retryCount = 0
	c.mu.Unlock()

	// Start message handler
	go c.handleMessages()

	return nil
}

func (c *Client) handleMessages() {
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if ws.IsCloseError(err, ws.CloseNormalClosure, ws.CloseGoingAway) {
				break
			}
			if c.config.AutoReconnect && c.retryCount < c.config.MaxRetries {
				c.reconnect()
			}
			break
		}

		var msg WSMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			continue
		}

		switch msg.Type {
		case "message":
			if handler, ok := c.handlers.GetAny(msg.Topic+"-"+msg.Target, msg.Topic); ok {
				subID := msg.ID
				if sid, ok := msg.Payload["subID"].(string); ok && sid != "" {
					subID = sid
				}
				handler(msg.Payload, &clientSubscription{
					topic:  msg.Topic,
					subID:  subID,
					client: c,
				})
			}
		case "state_pool_message":
			if poolName, ok := msg.Payload["pool"].(string); ok {
				handlerID := fmt.Sprintf("state_pool_%s", poolName)
				if handler, ok := c.handlers.Get(handlerID); ok {
					handler(msg.Payload, &clientSubscription{
						topic:  handlerID,
						subID:  msg.ID,
						client: c,
					})
				}
			}
		case "error", "published", "subscribed", "unsubscribed", "subscribers_status",
			"state_pool_created", "state_pool_message_sent", "state_pool_updated",
			"state_pool_keys_deleted", "state_pool_cleared", "state_pool_saved",
			"state_pool_loaded", "state_pool_stopped", "state_pool_removed",
			"state_pool_state", "actor_pool_created", "actor_pool_message_sent",
			"actor_pool_stopped", "actor_pool_removed":
			if ch, ok := c.pending.Get(msg.ID); ok {
				select {
				case ch <- &msg:
					// Message sent successfully
				default:
					// Channel is closed or full, clean up
					c.pending.Delete(msg.ID)
				}
			}
		case "actor_pool_message":
			if poolName, ok := msg.Payload["pool"].(string); ok {
				handlerID := fmt.Sprintf("actor_pool_%s", poolName)
				if handler, ok := c.handlers.Get(handlerID); ok {
					handler(msg.Payload, &clientSubscription{
						topic:  handlerID,
						subID:  msg.ID,
						client: c,
					})
				}
			}
		}
	}
}

func (c *Client) reconnect() {
	backoff := time.Duration(c.config.BackoffMin.Nanoseconds() * (1 << uint(c.retryCount)))
	if backoff > c.config.BackoffMax {
		backoff = c.config.BackoffMax
	}
	time.Sleep(backoff)
	c.retryCount++
	c.connect()
}

func (c *Client) generateMessageID(topic, subID string) string {
	s := c.config.ClientID
	if topic != "" {
		s += "-" + topic
	}
	if subID != "" {
		s += "-" + topic
	}
	return s
}

func (c *Client) waitForResponse(messageID string, timeout time.Duration) (*WSMessage, error) {
	// Get the response channel
	respChan, ok := c.pending.Get(messageID)
	if !ok {
		return nil, fmt.Errorf("no response channel for message %s", messageID)
	}

	// Wait for response with timeout
	select {
	case resp := <-respChan:
		if resp == nil {
			return nil, fmt.Errorf("response channel closed without response")
		}
		return resp, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("response timeout after %v", timeout)
	}
}

// Subscribe subscribes to a topic
func (c *Client) Subscribe(topic string, subID string, handler func(map[string]any, Subscription)) Subscription {
	if subID == "" {
		subID = c.config.ClientID
	}
	messageID := ksmux.GenerateID()
	message := WSMessage{
		Type:   "subscribe",
		Topic:  topic,
		ID:     messageID,
		Target: subID,
	}

	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)

	err := c.conn.WriteJSON(message)
	if err != nil {
		c.pending.Delete(messageID)
		return nil
	}

	// Wait for response
	resp, err := c.waitForResponse(messageID, 5*time.Second)
	if err != nil || resp.Type != "subscribed" {
		c.pending.Delete(messageID)
		return nil
	}

	c.handlers.Set(topic+"-"+subID, handler)
	return &clientSubscription{
		topic:  topic,
		subID:  subID,
		client: c,
	}
}

// Publish publishes a message to a topic
func (c *Client) Publish(topic string, payload map[string]any, opts *PublishOptions) bool {
	messageID := "pub-" + ksmux.GenerateID()
	message := WSMessage{
		Type:    "publish",
		Topic:   topic,
		ID:      messageID,
		Payload: payload,
	}

	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)
	defer func() {
		// Clean up the channel
		c.pending.Delete(messageID)
		close(respChan)
	}()

	data, _ := json.Marshal(message)
	err := c.conn.WriteMessage(ws.TextMessage, data)
	if err != nil {
		if opts != nil && opts.OnFailure != nil {
			opts.OnFailure(err)
		}
		return false
	}

	// Wait for response
	var success bool
	select {
	case resp := <-respChan:
		success = resp != nil && resp.Type == "published"
		if success {
			if opts != nil && opts.OnSuccess != nil {
				opts.OnSuccess()
			}
		} else if opts != nil && opts.OnFailure != nil {
			if resp != nil && resp.Type == "error" {
				if errMsg, ok := resp.Payload["error"].(string); ok {
					opts.OnFailure(fmt.Errorf("%s", errMsg))
				} else {
					opts.OnFailure(fmt.Errorf("publish failed"))
				}
			} else {
				opts.OnFailure(fmt.Errorf("publish failed"))
			}
		}
	case <-time.After(5 * time.Second):
		if opts != nil && opts.OnFailure != nil {
			opts.OnFailure(fmt.Errorf("publish timeout"))
		}
		success = false
	}

	return success
}

// PublishWithRetry publishes a message with retry
func (c *Client) PublishWithRetry(topic string, payload map[string]any, cfg *RetryConfig, opts *PublishOptions) bool {
	if cfg == nil {
		cfg = &DefaultRetryConfig
	}

	messageID := "pubretry-" + ksmux.GenerateID()
	message := WSMessage{
		Type:  "publishWithRetry",
		Topic: topic,
		ID:    messageID,
		Payload: map[string]any{
			"data": payload,
			"retry_config": map[string]any{
				"max_attempts": cfg.MaxAttempts,
				"max_backoff":  cfg.MaxBackoff,
			},
		},
	}

	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)
	defer func() {
		// Clean up the channel
		c.pending.Delete(messageID)
		close(respChan)
	}()

	data, _ := json.Marshal(message)
	err := c.conn.WriteMessage(ws.TextMessage, data)
	if err != nil {
		if opts != nil && opts.OnFailure != nil {
			opts.OnFailure(err)
		}
		return false
	}

	// Wait for response
	var success bool
	select {
	case resp := <-respChan:
		success = resp != nil && resp.Type == "published"
		if success {
			if opts != nil && opts.OnSuccess != nil {
				opts.OnSuccess()
			}
		} else if opts != nil && opts.OnFailure != nil {
			if resp != nil && resp.Type == "error" {
				if errMsg, ok := resp.Payload["error"].(string); ok {
					opts.OnFailure(fmt.Errorf("%s", errMsg))
				} else {
					opts.OnFailure(fmt.Errorf("publish failed"))
				}
			} else {
				opts.OnFailure(fmt.Errorf("publish failed"))
			}
		}
	case <-time.After(5 * time.Second):
		if opts != nil && opts.OnFailure != nil {
			opts.OnFailure(fmt.Errorf("publish timeout"))
		}
		success = false
	}

	return success
}

// PublishTo publishes a message to a specific client
func (c *Client) PublishTo(topic, targetID string, payload map[string]any, opts *PublishOptions) bool {
	messageID := "pubto-" + ksmux.GenerateID()
	message := WSMessage{
		Type:    "publishTo",
		Topic:   topic,
		Target:  targetID,
		ID:      messageID,
		Payload: payload,
	}

	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)
	defer func() {
		// Clean up the channel
		c.pending.Delete(messageID)
		close(respChan)
	}()

	data, _ := json.Marshal(message)
	err := c.conn.WriteMessage(ws.TextMessage, data)
	if err != nil {
		if opts != nil && opts.OnFailure != nil {
			opts.OnFailure(err)
		}
		return false
	}

	// Wait for response
	var success bool
	select {
	case resp := <-respChan:
		success = resp != nil && resp.Type == "published"
		if success {
			if opts != nil && opts.OnSuccess != nil {
				opts.OnSuccess()
			}
		} else if opts != nil && opts.OnFailure != nil {
			if resp != nil && resp.Type == "error" {
				if errMsg, ok := resp.Payload["error"].(string); ok {
					opts.OnFailure(fmt.Errorf("%s", errMsg))
				} else {
					opts.OnFailure(fmt.Errorf("publish failed"))
				}
			} else {
				opts.OnFailure(fmt.Errorf("publish failed"))
			}
		}
	case <-time.After(5 * time.Second):
		if opts != nil && opts.OnFailure != nil {
			opts.OnFailure(fmt.Errorf("publish timeout"))
		}
		success = false
	}

	return success
}

// PublishToWithRetry publishes a message to a specific client with retry
func (c *Client) PublishToWithRetry(topic string, targetID string, payload map[string]any, cfg *RetryConfig, opts *PublishOptions) bool {
	if cfg == nil {
		cfg = &DefaultRetryConfig
	}

	messageID := "pubtoretry-" + ksmux.GenerateID()
	message := WSMessage{
		Type:   "publishToWithRetry",
		Topic:  topic,
		ID:     messageID,
		Target: targetID,
		Payload: map[string]any{
			"data": payload,
			"retry_config": map[string]any{
				"max_attempts": cfg.MaxAttempts,
				"max_backoff":  cfg.MaxBackoff,
			},
		},
	}

	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)
	defer func() {
		// Clean up the channel
		c.pending.Delete(messageID)
		close(respChan)
	}()

	data, _ := json.Marshal(message)
	err := c.conn.WriteMessage(ws.TextMessage, data)
	if err != nil {
		if opts != nil && opts.OnFailure != nil {
			opts.OnFailure(err)
		}
		return false
	}

	// Wait for response
	var success bool
	select {
	case resp := <-respChan:
		success = resp != nil && resp.Type == "published"
		if success {
			if opts != nil && opts.OnSuccess != nil {
				opts.OnSuccess()
			}
		} else if opts != nil && opts.OnFailure != nil {
			if resp != nil && resp.Type == "error" {
				if errMsg, ok := resp.Payload["error"].(string); ok {
					opts.OnFailure(fmt.Errorf("%s", errMsg))
				} else {
					opts.OnFailure(fmt.Errorf("publish failed"))
				}
			} else {
				opts.OnFailure(fmt.Errorf("publish failed"))
			}
		}
	case <-time.After(5 * time.Second):
		if opts != nil && opts.OnFailure != nil {
			opts.OnFailure(fmt.Errorf("publish timeout"))
		}
		success = false
	}

	return success
}

// HasSubscribers checks if a topic has subscribers
func (c *Client) HasSubscribers(topic string) bool {
	messageID := c.generateMessageID(topic, "hasSubs")
	message := WSMessage{
		Type:  "has_subscribers",
		Topic: topic,
		ID:    messageID,
	}

	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)
	defer c.pending.Delete(messageID)

	data, _ := json.Marshal(message)
	err := c.conn.WriteMessage(ws.TextMessage, data)
	if err != nil {
		return false
	}

	resp, err := c.waitForResponse(messageID, 5*time.Second)
	if err != nil {
		return false
	}

	if hasSubscribers, ok := resp.Payload["has_subscribers"].(bool); ok {
		return hasSubscribers
	}
	return false
}

// PublishToServer sends a message to another server
func (c *Client) PublishToServer(serverAddr string, msg map[string]any, opts *PublishOptions, path ...string) bool {
	payl := map[string]any{
		"server_addr": serverAddr,
		"data":        msg,
	}
	if len(path) > 0 {
		payl["path"] = path[0]
	}
	messageID := c.generateMessageID(serverAddr, "pubToServer")
	message := WSMessage{
		Type:    "send_to_server",
		ID:      messageID,
		Payload: payl,
	}

	err := c.conn.WriteJSON(message)
	if err != nil {
		if opts != nil && opts.OnFailure != nil {
			opts.OnFailure(err)
		}
		return false
	}

	resp, err := c.waitForResponse(messageID, 5*time.Second)
	if err != nil {
		if opts != nil && opts.OnFailure != nil {
			opts.OnFailure(err)
		}
		return false
	}

	if opts != nil && opts.OnSuccess != nil {
		opts.OnSuccess()
	}
	return resp.Type == "published"
}

// Close closes the WebSocket connection
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) unsubscribe(topic, subID string) {
	messageID := c.generateMessageID(topic, "unsub-"+subID)
	message := WSMessage{
		Type:  "unsubscribe",
		Topic: topic,
		ID:    messageID,
		Payload: map[string]any{
			"subID": subID,
		},
	}

	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)

	data, _ := json.Marshal(message)
	err := c.conn.WriteMessage(ws.TextMessage, data)
	if err != nil {
		c.pending.Delete(messageID)
		return
	}

	// Wait for response
	select {
	case resp := <-respChan:
		if resp != nil && resp.Type == "unsubscribed" {
			c.handlers.Delete(topic)
		}
	case <-time.After(5 * time.Second):
		// Timeout, but still delete the handler
		c.handlers.Delete(topic)
	}

	c.pending.Delete(messageID)
}

type clientSubscription struct {
	topic  string
	subID  string
	client *Client
}

func (s *clientSubscription) GetTopic() string {
	return s.topic
}

func (s *clientSubscription) Unsubscribe() {
	if s.client != nil {
		s.client.unsubscribe(s.topic, s.subID)
	}
}

// CreateStatePool creates a new stateful actor pool on the server
func (c *Client) CreateStatePool(config StatefulPoolConfig) error {
	// Create a serializable version of the config
	serializableConfig := map[string]any{
		"name":          config.Name,
		"size":          config.Size,
		"initial":       config.Initial,
		"state_size_mb": config.StateSizeMB,
	}

	msg := WSMessage{
		Type: "create_state_pool",
		ID:   c.generateMessageID("create-state-pool-client-go", config.Name),
		Payload: map[string]any{
			"config": serializableConfig,
		},
	}
	return c.sendStatePoolMessage(msg)
}

// RemovePoolHandler removes the message handler for a specific pool
func (c *Client) RemovePoolHandler(poolName string) {
	handlerID := fmt.Sprintf("pool_%s", poolName)
	c.handlers.Delete(handlerID)
}

// SavePoolState persists a pool's state to disk
func (c *Client) SavePoolState(poolName string, directory string) error {
	msg := WSMessage{
		Type: "save_pool_state",
		ID:   c.generateMessageID("save-state-pool", poolName+"-"+directory),
		Payload: map[string]any{
			"pool":      poolName,
			"directory": directory,
		},
	}
	return c.sendPoolMessage(msg)
}

// LoadPoolState loads a pool's state from disk
func (c *Client) LoadPoolState(poolName string, directory string) error {
	msg := WSMessage{
		Type: "load_pool_state",
		ID:   c.generateMessageID("load-pool-state", poolName+"-"+directory),
		Payload: map[string]any{
			"pool":      poolName,
			"directory": directory,
		},
	}
	return c.sendPoolMessage(msg)
}

// Helper function to send pool messages and handle responses
func (c *Client) sendPoolMessage(msg WSMessage) error {
	c.mu.Lock()
	if c.conn == nil || !c.isConnected {
		c.mu.Unlock()
		return fmt.Errorf("client not connected")
	}

	// Create response channel
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(msg.ID, respChan)
	defer c.pending.Delete(msg.ID)

	// Send request
	if err := c.conn.WriteJSON(msg); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("failed to send pool message: %w", err)
	}
	c.mu.Unlock()

	// Wait for response with timeout
	resp, err := c.waitForResponse(msg.ID, 10*time.Second)
	if err != nil {
		return err
	}

	if resp.Type == "error" {
		if errMsg, ok := resp.Payload["error"].(string); ok {
			return fmt.Errorf("%s", errMsg)
		}
		return fmt.Errorf("unknown error in response")
	}

	return nil
}

// SendToStatePool sends a message to a specific state pool
func (c *Client) SendToStatePool(poolName string, data any) error {
	msg := WSMessage{
		Type: "state_pool_message",
		ID:   c.generateMessageID("send-to-state-pool", poolName),
		Payload: map[string]any{
			"pool": poolName,
			"data": data,
		},
	}
	return c.sendStatePoolMessage(msg)
}

// GetState retrieves the current state of a state pool
func (c *Client) GetState(poolName string) (map[string]any, error) {
	msg := WSMessage{
		Type: "state_pool_state",
		ID:   c.generateMessageID("getstate", poolName),
		Payload: map[string]any{
			"pool": poolName,
		},
	}

	c.mu.Lock()
	if c.conn == nil || !c.isConnected {
		c.mu.Unlock()
		return nil, fmt.Errorf("client not connected")
	}

	// Create response channel
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(msg.ID, respChan)
	defer c.pending.Delete(msg.ID)

	// Send request
	if err := c.conn.WriteJSON(msg); err != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("failed to send state request: %w", err)
	}
	c.mu.Unlock()

	// Wait for response with timeout
	resp, err := c.waitForResponse(msg.ID, 10*time.Second)
	if err != nil {
		return nil, err
	}

	if resp.Type == "error" {
		if errMsg, ok := resp.Payload["error"].(string); ok {
			return nil, fmt.Errorf("%s", errMsg)
		}
		return nil, fmt.Errorf("unknown error in response")
	}

	// Extract state from response
	if state, ok := resp.Payload["state"].(map[string]any); ok {
		return state, nil
	}

	return nil, fmt.Errorf("invalid state format in response")
}

// OnStatePoolMessage registers a handler for messages from a specific state pool
func (c *Client) OnStatePoolMessage(poolName string, handler func(data any)) {
	handlerID := fmt.Sprintf("state_pool_%s", poolName)
	c.handlers.Set(handlerID, func(msg map[string]any, _ Subscription) {
		if data, exists := msg["data"]; exists {
			handler(data)
		}
	})
}

// UpdateStatePool updates specific state values in a state pool
func (c *Client) UpdateStatePool(poolName string, updates map[string]any) error {
	msg := WSMessage{
		Type: "update_state_pool",
		ID:   c.generateMessageID("update-state", poolName),
		Payload: map[string]any{
			"pool":    poolName,
			"updates": updates,
		},
	}
	return c.sendStatePoolMessage(msg)
}

// DeleteStatePoolKeys deletes specific keys from a state pool's state
func (c *Client) DeleteStatePoolKeys(poolName string, keys []string) error {
	msg := WSMessage{
		Type: "delete_state_pool_keys",
		ID:   c.generateMessageID("delete-state", poolName),
		Payload: map[string]any{
			"pool": poolName,
			"keys": keys,
		},
	}
	return c.sendStatePoolMessage(msg)
}

// ClearStatePool removes all values from a state pool's state
func (c *Client) ClearStatePool(poolName string) error {
	msg := WSMessage{
		Type: "clear_state_pool",
		ID:   c.generateMessageID("clear-state", poolName),
		Payload: map[string]any{
			"pool": poolName,
		},
	}
	return c.sendStatePoolMessage(msg)
}

// SaveStatePool persists a state pool's state to disk
func (c *Client) SaveStatePool(poolName string, directory string) error {
	msg := WSMessage{
		Type: "save_state_pool",
		ID:   c.generateMessageID("save-state", poolName+"-"+directory),
		Payload: map[string]any{
			"pool":      poolName,
			"directory": directory,
		},
	}
	return c.sendStatePoolMessage(msg)
}

// LoadStatePool loads a state pool's state from disk
func (c *Client) LoadStatePool(poolName string, directory string) error {
	msg := WSMessage{
		Type: "load_state_pool",
		ID:   c.generateMessageID("load-state", poolName+"-"+directory),
		Payload: map[string]any{
			"pool":      poolName,
			"directory": directory,
		},
	}
	return c.sendStatePoolMessage(msg)
}

// StopStatePool stops a state pool's processing
func (c *Client) StopStatePool(poolName string) error {
	msg := WSMessage{
		Type: "stop_state_pool",
		ID:   c.generateMessageID("stop-state", poolName),
		Payload: map[string]any{
			"pool": poolName,
		},
	}
	return c.sendStatePoolMessage(msg)
}

// RemoveStatePool stops and removes a state pool
func (c *Client) RemoveStatePool(poolName string) error {
	msg := WSMessage{
		Type: "remove_state_pool",
		ID:   c.generateMessageID("remove-state", poolName),
		Payload: map[string]any{
			"pool": poolName,
		},
	}
	return c.sendStatePoolMessage(msg)
}

// RemoveStatePoolHandler removes the message handler for a specific state pool
func (c *Client) RemoveStatePoolHandler(poolName string) {
	handlerID := fmt.Sprintf("state_pool_%s", poolName)
	c.handlers.Delete(handlerID)
}

// Helper function to send state pool messages and handle responses
func (c *Client) sendStatePoolMessage(msg WSMessage) error {
	c.mu.Lock()
	if c.conn == nil || !c.isConnected {
		c.mu.Unlock()
		return fmt.Errorf("client not connected")
	}

	// Create response channel
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(msg.ID, respChan)
	defer c.pending.Delete(msg.ID)

	// Send request
	if err := c.conn.WriteJSON(msg); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("failed to send state pool message: %w", err)
	}
	c.mu.Unlock()

	// Wait for response with timeout
	resp, err := c.waitForResponse(msg.ID, 10*time.Second)
	if err != nil {
		return err
	}

	if resp.Type == "error" {
		if errMsg, ok := resp.Payload["error"].(string); ok {
			return fmt.Errorf("%s", errMsg)
		}
		return fmt.Errorf("unknown error in response")
	}

	return nil
}

// CreateActorPool creates a new actor pool on the server
func (c *Client) CreateActorPool(name string, size int) error {
	msg := WSMessage{
		Type: "create_actor_pool",
		ID:   c.generateMessageID("create-actor", name),
		Payload: map[string]any{
			"config": map[string]any{
				"name": name,
				"size": size,
			},
		},
	}
	return c.sendActorPoolMessage(msg)
}

// SendToActorPool sends a message to a specific actor pool
func (c *Client) SendToActorPool(poolName string, data any) error {
	msg := WSMessage{
		Type: "actor_pool_message",
		ID:   c.generateMessageID("send-actor", poolName),
		Payload: map[string]any{
			"pool": poolName,
			"data": data,
		},
	}
	return c.sendActorPoolMessage(msg)
}

// OnActorPoolMessage registers a handler for messages from a specific actor pool
func (c *Client) OnActorPoolMessage(poolName string, handler func(data any)) {
	handlerID := fmt.Sprintf("actor_pool_%s", poolName)
	c.handlers.Set(handlerID, func(msg map[string]any, _ Subscription) {
		if data, exists := msg["data"]; exists {
			handler(data)
		}
	})
}

// RemoveActorPoolHandler removes the message handler for a specific actor pool
func (c *Client) RemoveActorPoolHandler(poolName string) {
	handlerID := fmt.Sprintf("actor_pool_%s", poolName)
	c.handlers.Delete(handlerID)
}

// StopActorPool stops an actor pool's processing
func (c *Client) StopActorPool(poolName string) error {
	msg := WSMessage{
		Type: "stop_actor_pool",
		ID:   c.generateMessageID("stop-actor", poolName),
		Payload: map[string]any{
			"pool": poolName,
		},
	}
	return c.sendActorPoolMessage(msg)
}

// RemoveActorPool stops and removes an actor pool
func (c *Client) RemoveActorPool(poolName string) error {
	msg := WSMessage{
		Type: "remove_actor_pool",
		ID:   c.generateMessageID("remove-actor", poolName),
		Payload: map[string]any{
			"pool": poolName,
		},
	}
	return c.sendActorPoolMessage(msg)
}

// Helper function to send actor pool messages and handle responses
func (c *Client) sendActorPoolMessage(msg WSMessage) error {
	c.mu.Lock()
	if c.conn == nil || !c.isConnected {
		c.mu.Unlock()
		return fmt.Errorf("client not connected")
	}

	// Create response channel
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(msg.ID, respChan)
	defer c.pending.Delete(msg.ID)

	// Send request
	if err := c.conn.WriteJSON(msg); err != nil {
		c.mu.Unlock()
		return fmt.Errorf("failed to send actor pool message: %w", err)
	}
	c.mu.Unlock()

	// Wait for response with timeout
	resp, err := c.waitForResponse(msg.ID, 10*time.Second)
	if err != nil {
		return err
	}

	if resp.Type == "error" {
		if errMsg, ok := resp.Payload["error"].(string); ok {
			return fmt.Errorf("%s", errMsg)
		}
		return fmt.Errorf("unknown error in response")
	}

	return nil
}
