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
	"fmt"
	"sync"
	"time"

	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/ksmux/ws"
	"github.com/kamalshkeir/ksmux/wspool"
)

// Client represents a WebSocket client
type Client struct {
	config      ClientConfig
	conn        *wspool.Conn
	isConnected bool
	retryCount  int
	handlers    *kmap.SafeMap[string, func(map[string]any, Subscription)]
	pending     *kmap.SafeMap[string, chan *WSMessage]
	// Message pool for better memory reuse
	msgPool sync.Pool
	debug   bool
}

// ClientConfig holds the configuration for the WebSocket client
type ClientConfig struct {
	Address       string
	Path          string
	Secure        bool
	ID            string
	AutoReconnect bool
	MaxRetries    int
	BackoffMin    time.Duration
	BackoffMax    time.Duration
}

func (c *Client) ID() string {
	return c.config.ID
}
func (c *Client) Conn() *wspool.Conn {
	return c.conn
}
func (c *Client) IsConnected() bool {
	return c.isConnected
}
func (c *Client) Config() *ClientConfig {
	return &c.config
}

func (c *Client) Handlers() *kmap.SafeMap[string, func(map[string]any, Subscription)] {
	return c.handlers
}

// NewClient creates a new WebSocket client
func NewClient(config ClientConfig) (*Client, error) {
	if config.ID == "" {
		config.ID = fmt.Sprintf("client-%d", time.Now().UnixNano())
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

	// Initialize message pool
	client.msgPool.New = func() interface{} {
		return &WSMessage{
			Payload: make(map[string]any),
		}
	}

	if client.debug {
		client.debugTitle("Connecting...")
	}
	err := client.connect()
	if err != nil {
		if client.debug {
			client.debugLog("failed to connect %w", err)
		}
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	if client.debug {
		client.debugLog("Connected as %s to %s%s", client.config.ID, config.Address, config.Path)
		client.debugLines()
	}
	return client, nil
}

func (c *Client) WithDebug(debug bool) {
	c.debug = debug
}

func (c *Client) connect() error {
	protocol := "ws"
	if c.config.Secure {
		protocol = "wss"
	}
	url := fmt.Sprintf("%s://%s%s", protocol, c.config.Address, c.config.Path)
	if c.debug {
		c.debugLog("connecting to %s", url)
	}
	conn, _, err := ws.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	cn := wspool.DefaultPool().AddClient(conn, c.config.Address)

	c.conn = cn
	c.isConnected = true
	c.retryCount = 0

	// Start message handler
	go c.handleMessages()

	return nil
}

func (c *Client) debugLog(format string, args ...interface{}) {
	if c.debug {
		fmt.Printf("[CLIENT DEBUG] "+format+"\n", args...)
	}
}

func (c *Client) debugTitle(format string, args ...interface{}) {
	if c.debug {
		fmt.Printf("---------- "+format+" ----------\n", args...)
	}
}

func (c *Client) debugLines() {
	if c.debug {
		fmt.Println("-------------------------------")
	}
}

func (c *Client) handleMessages() {
	for {
		msg := c.getWSMessage()
		defer c.putWSMessage(msg)
		err := c.conn.ReadJSON(&msg)
		if err != nil {
			if c.debug {
				c.debugLog("[handleMessages] closing the connection")
			}
			if ws.IsCloseError(err, ws.CloseNormalClosure, ws.CloseGoingAway) {
				break
			}
			if c.config.AutoReconnect && c.retryCount < c.config.MaxRetries {
				c.reconnect()
			}
			break
		}
		if c.debug {
			c.debugLog("[handleMessages] got msg type='%s', topic='%s', target='%s' id='%s'", msg.Type, msg.Topic, msg.Target, msg.ID)
		}
		switch msg.Type {
		case "message":
			if c.debug {
				c.debugTitle("'%s'", msg.Type)
			}
			handlerKey := msg.Topic + "-" + msg.Target
			if handler, ok := c.handlers.GetAny(handlerKey, msg.Topic); ok {
				if c.debug {
					c.debugLog("[handleMessages](%s) handler found for key='%s', topic='%s'", msg.Type, handlerKey, msg.Topic)
				}
				handler(msg.Payload, &clientSubscription{
					topic:  msg.Topic,
					subID:  msg.Target,
					client: c,
				})
			} else if c.debug {
				c.debugLog("[handleMessages](%s) handler not found for key='%s', topic='%s'", msg.Type, handlerKey, msg.Topic)
			}
		case "state_pool_message":
			if c.debug {
				c.debugTitle("'%s'", msg.Type)
			}
			if poolName, ok := msg.Payload["pool"].(string); ok {
				handlerKey := fmt.Sprintf("state_pool_%s", poolName)
				if handler, ok := c.handlers.Get(handlerKey); ok {
					if c.debug {
						c.debugLog("[handleMessages](%s) handler found for key='%s', topic='%s'", msg.Type, handlerKey, msg.Topic)
					}
					handler(msg.Payload, &clientSubscription{
						topic:  handlerKey,
						subID:  msg.ID,
						client: c,
					})
				} else {
					if c.debug {
						c.debugLog("[handleMessages](%s) handler not found for key='%s', topic='%s'", msg.Type, handlerKey, msg.Topic)
					}
				}
			} else {
				if c.debug {
					c.debugLog("[handleMessages](%s) missing 'pool' in payload %+v", msg.Type, msg.Payload)
				}
			}
		case "error", "published", "subscribed", "unsubscribed", "subscribers_status",
			"state_pool_created", "state_pool_message_sent", "state_pool_updated",
			"state_pool_keys_deleted", "state_pool_cleared", "state_pool_saved",
			"state_pool_loaded", "state_pool_stopped", "state_pool_removed",
			"state_pool_state", "actor_pool_created", "actor_pool_message_sent",
			"actor_pool_stopped", "actor_pool_removed":
			if c.debug {
				c.debugTitle("'%s'", msg.Type)
				c.debugLog("[handleMessages](%s) GOT msg_id='%s', topic='%s', target='%s'", msg.Type, msg.ID, msg.Topic, msg.Target)
			}
			if ch, ok := c.pending.Get(msg.ID); ok {
				if c.debug {
					c.debugLog("channel found")
				}
				select {
				case ch <- msg:
					if c.debug {
						c.debugLog("channel %s sent", msg.ID)
					}
					// Message will be put back by the receiver
				default:
					// Channel is closed or full, clean up
					if c.debug {
						c.debugLog("channel is closed or full, deleting pending %s", msg.ID)
					}
				}
			} else {
				if c.debug {
					c.debugLog("channel not found")
				}
			}
		case "actor_pool_message":
			if c.debug {
				c.debugTitle("'%s'", msg.Type)
			}
			if poolName, ok := msg.Payload["pool"].(string); ok {
				handlerKey := fmt.Sprintf("actor_pool_%s", poolName)
				if handler, ok := c.handlers.Get(handlerKey); ok {
					handler(msg.Payload, &clientSubscription{
						topic:  handlerKey,
						subID:  msg.ID,
						client: c,
					})
				}
			}
		default:
			if c.debug {
				c.debugLog("hit default case for type='%s', topic='%s', target='%s' id='%s'", msg.Type, msg.Topic, msg.Target, msg.ID)
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
	if c.debug {
		c.debugLog("Reconnecting")
	}
	c.connect()
	if c.debug {
		c.debugLog("Connected")
	}
}

func (c *Client) generateMessageID(topic, subID string) string {
	s := c.config.ID
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
	if c.debug {
		c.debugTitle("'waitForResponse'")
	}
	respChan, ok := c.pending.Get(messageID)
	if !ok && c.debug {
		c.debugLog("no response channel for message %s", messageID)
		return nil, fmt.Errorf("no response channel for message %s", messageID)
	}
	// Wait for response with timeout
	if c.debug {
		c.debugLog("waiting for response for %s", messageID)
	}
	select {
	case resp := <-respChan:
		if c.debug {
			c.debugLog("got response, deleting pending, and closing channel")
		}
		if resp == nil {
			c.pending.Delete(messageID)
			if respChan != nil {
				close(respChan)
			}
			return nil, fmt.Errorf("response channel closed without response")
		}
		// Only clean up after we know we have a valid response
		c.pending.Delete(messageID)
		if respChan != nil {
			close(respChan)
		}
		if c.debug {
			c.debugLog("done waiting")
		}
		return resp, nil
	case <-time.After(timeout):
		// Clean up on timeout
		c.pending.Delete(messageID)
		if respChan != nil {
			close(respChan)
		}
		if c.debug {
			c.debugLog("done waiting without response after %v", timeout)
		}
		return nil, fmt.Errorf("response timeout after %v", timeout)
	}
}

// Subscribe subscribes to a topic
func (c *Client) Subscribe(topic string, subID string, handler func(map[string]any, Subscription)) Subscription {
	if subID == "" {
		subID = c.config.ID
	}
	messageID := "sub-" + ksmux.GenerateID()
	msg := c.getWSMessage()
	defer c.putWSMessage(msg)
	msg.Type = "subscribe"
	msg.Topic = topic
	msg.Target = subID
	msg.ID = messageID

	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)

	if c.debug {
		c.debugTitle("Subscribing...")
		c.debugLog("subscribing to topic=%s, subID=%s, msgID=%s", msg.Topic, msg.Target, msg.ID)
	}
	err := c.conn.WriteJSON(msg)
	if err != nil {
		if c.debug {
			c.debugLog("failed to send subscription %+v %w", msg, err)
		}
		c.pending.Delete(messageID)
		close(respChan)
		return nil
	}

	// Wait for response
	if c.debug {
		c.debugLog("start waiting for sub response")
	}
	resp, err := c.waitForResponse(messageID, 5*time.Second)
	if err != nil || resp.Type != "subscribed" {
		if resp != nil {
			c.debugLog("no response ^^")
		}
		return nil
	}
	if c.debug {
		c.debugLog("finish waiting response")
	}
	defer c.putWSMessage(resp)

	// Store handler with consistent key format
	handlerKey := topic + "-" + subID
	if c.debug {
		c.debugLog("storing the handler for topic='%s' subID='%s' with key='%s'", msg.Topic, msg.Target, handlerKey)
	}
	c.handlers.Set(handlerKey, handler)

	return &clientSubscription{
		topic:  topic,
		subID:  subID,
		client: c,
	}
}

// Publish publishes a message to a topic
func (c *Client) Publish(topic string, payload map[string]any, opts *PublishOptions) bool {
	messageID := "pub-" + ksmux.GenerateID()
	msg := c.getWSMessage()
	defer c.putWSMessage(msg)
	msg.Type = "publish"
	msg.Topic = topic
	msg.ID = messageID
	msg.Payload = payload
	if c.debug {
		c.debugTitle("Publishing...")
		c.debugLog("publishing to topic='%s' payload='%+v' opts='%+v'", topic, payload, opts)
	}

	if opts == nil {
		if c.debug {
			c.debugLog("no ack")
		}
		msg.Payload["no_ack"] = true
		err := c.conn.WriteJSON(msg)
		return err == nil
	}

	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)

	err := c.conn.WriteJSON(msg)
	if err != nil {
		c.pending.Delete(messageID)
		close(respChan)
		if opts.OnFailure != nil {
			opts.OnFailure(err)
		}
		return false
	}

	// Wait for response
	if c.debug {
		c.debugLog("start wait for %s", messageID)
	}
	resp, err := c.waitForResponse(messageID, 5*time.Second)
	if err != nil {
		if opts.OnFailure != nil {
			opts.OnFailure(err)
		}
		return false
	}
	if c.debug {
		c.debugLog("finish wait for %s", messageID)
	}
	success := resp != nil && resp.Type == "published"
	if success {
		if opts.OnSuccess != nil {
			if c.debug {
				c.debugLog("OnSuccess Publish case triggering fn")
			}
			opts.OnSuccess()
		} else if c.debug {
			c.debugLog("no handler OnSuccess")
		}
	} else if opts.OnFailure != nil {
		if resp != nil && resp.Type == "error" {
			if c.debug {
				c.debugLog("OnFail Publish case triggering fn")
			}
			if errMsg, ok := resp.Payload["error"].(string); ok {
				if c.debug {
					c.debugLog("triggering OnFailure with err %s", errMsg)
				}
				opts.OnFailure(fmt.Errorf("%s", errMsg))
			} else {
				if c.debug {
					c.debugLog("triggering OnFailure with default err pub fail")
				}
				opts.OnFailure(fmt.Errorf("publish failed"))
			}
		} else {
			if c.debug {
				c.debugLog("response is nil or resp.Type not error")
			}
			opts.OnFailure(fmt.Errorf("publish failed"))
		}
	}

	c.putWSMessage(resp)
	return success
}

// Publish publishes a message to a topic
func (c *Client) sendToServer(address string, payload map[string]any, opts *PublishOptions) bool {
	messageID := "sendToServer-" + ksmux.GenerateID()
	msg := c.getWSMessage()
	defer c.putWSMessage(msg)
	msg.Type = "server_message"
	msg.ID = messageID
	msg.Payload = payload
	if c.debug {
		c.debugTitle("Publishing...")
		c.debugLog("publishing to address='%s' payload='%+v'", address, payload)
	}

	if opts == nil {
		if c.debug {
			c.debugLog("no ack")
		}
		msg.Payload["no_ack"] = true
		err := c.conn.WriteJSON(msg)
		return err == nil
	}

	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)

	err := c.conn.WriteJSON(msg)
	if err != nil {
		c.pending.Delete(messageID)
		close(respChan)
		if opts.OnFailure != nil {
			opts.OnFailure(err)
		}
		return false
	}

	// Wait for response
	if c.debug {
		c.debugLog("start wait for %s", messageID)
	}
	resp, err := c.waitForResponse(messageID, 5*time.Second)
	if err != nil {
		if opts.OnFailure != nil {
			opts.OnFailure(err)
		}
		return false
	}
	if c.debug {
		c.debugLog("finish wait for %s", messageID)
	}
	success := resp != nil && resp.Type == "published"
	if success {
		if opts.OnSuccess != nil {
			if c.debug {
				c.debugLog("OnSuccess Publish case triggering fn")
			}
			opts.OnSuccess()
		} else if c.debug {
			c.debugLog("no handler OnSuccess")
		}
	} else if opts.OnFailure != nil {
		if resp != nil && resp.Type == "error" {
			if c.debug {
				c.debugLog("OnFail Publish case triggering fn")
			}
			if errMsg, ok := resp.Payload["error"].(string); ok {
				if c.debug {
					c.debugLog("triggering OnFailure with err %s", errMsg)
				}
				opts.OnFailure(fmt.Errorf("%s", errMsg))
			} else {
				if c.debug {
					c.debugLog("triggering OnFailure with default err pub fail")
				}
				opts.OnFailure(fmt.Errorf("publish failed"))
			}
		} else {
			if c.debug {
				c.debugLog("response is nil or resp.Type not error")
			}
			opts.OnFailure(fmt.Errorf("publish failed"))
		}
	}

	c.putWSMessage(resp)
	return success
}

// PublishWithRetry publishes a message with retry
func (c *Client) PublishWithRetry(topic string, payload map[string]any, cfg *RetryConfig, opts *PublishOptions) bool {
	if cfg == nil {
		cfg = &DefaultRetryConfig
	}

	messageID := "pubretry-" + ksmux.GenerateID()
	msg := c.getWSMessage()
	defer c.putWSMessage(msg)
	msg.Type = "publishWithRetry"
	msg.Topic = topic
	msg.ID = messageID
	msg.Payload["data"] = payload
	msg.Payload["retry_config"] = map[string]any{
		"max_attempts": cfg.MaxAttempts,
		"max_backoff":  cfg.MaxBackoff,
	}

	if opts == nil {
		if c.debug {
			c.debugLog("PublishWithRetry: no ack %s", topic)
		}
		msg.Payload["no_ack"] = true
		err := c.conn.WriteJSON(msg)
		return err == nil
	}
	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)
	defer func() {
		// Clean up the channel
		c.pending.Delete(messageID)
		close(respChan)
	}()

	err := c.conn.WriteJSON(msg)
	if err != nil {
		if opts.OnFailure != nil {
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
			if opts.OnSuccess != nil {
				opts.OnSuccess()
			}
		} else if opts.OnFailure != nil {
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
		if opts.OnFailure != nil {
			opts.OnFailure(fmt.Errorf("publish timeout"))
		}
		success = false
	}

	return success
}

// PublishTo publishes a message to a specific client
func (c *Client) PublishTo(topic, targetID string, payload map[string]any, opts *PublishOptions) bool {
	messageID := "pubto-" + ksmux.GenerateID()
	msg := c.getWSMessage()
	defer c.putWSMessage(msg)
	msg.Type = "publishTo"
	msg.Topic = topic
	msg.Target = targetID
	msg.ID = messageID
	for k, v := range payload {
		msg.Payload[k] = v
	}

	if opts == nil {
		if c.debug {
			c.debugLog("PublishTo: no ack %s %s", topic, targetID)
		}
		msg.Payload["no_ack"] = true
		err := c.conn.WriteJSON(msg)
		return err == nil
	}

	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)
	defer func() {
		// Clean up the channel
		c.pending.Delete(messageID)
		close(respChan)
	}()

	err := c.conn.WriteJSON(msg)
	if err != nil {
		if opts.OnFailure != nil {
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
			if opts.OnSuccess != nil {
				opts.OnSuccess()
			}
		} else if opts.OnFailure != nil {
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
		if opts.OnFailure != nil {
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
	msg := c.getWSMessage()
	defer c.putWSMessage(msg)
	msg.Type = "publishToWithRetry"
	msg.Topic = topic
	msg.ID = messageID
	msg.Target = targetID
	msg.Payload["data"] = payload
	msg.Payload["retry_config"] = map[string]any{
		"max_attempts": cfg.MaxAttempts,
		"max_backoff":  cfg.MaxBackoff,
	}
	if c.debug {
		c.debugTitle("PublishToWithRetry...")
		c.debugLog("pub with retry topic='%s', target='%s', msgID='%s', payload='%+v'", msg.Topic, msg.Payload, cfg, msg.ID)
	}
	if opts == nil {
		if c.debug {
			c.debugLog("PublishToWithRetry: no ack %s %s", topic, targetID)
		}
		msg.Payload["no_ack"] = true
		err := c.conn.WriteJSON(msg)
		return err == nil
	}

	// Check for no_ack in payload
	if na, ok := payload["no_ack"].(bool); ok && na {
		err := c.conn.WriteJSON(msg)
		return err == nil
	}
	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)
	defer func() {
		// Clean up the channel
		c.pending.Delete(messageID)
		close(respChan)
	}()

	err := c.conn.WriteJSON(msg)
	if err != nil {
		if opts.OnFailure != nil {
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
			if opts.OnSuccess != nil {
				opts.OnSuccess()
			}
		} else if opts.OnFailure != nil {
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
		if opts.OnFailure != nil {
			opts.OnFailure(fmt.Errorf("publish timeout"))
		}
		success = false
	}

	return success
}

// HasSubscribers checks if a topic has subscribers
func (c *Client) HasSubscribers(topic string) bool {
	messageID := c.generateMessageID(topic, "hasSubs")
	msg := c.getWSMessage()
	defer c.putWSMessage(msg)
	msg.Type = "has_subscribers"
	msg.Topic = topic
	msg.ID = messageID

	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)
	defer c.pending.Delete(messageID)

	err := c.conn.WriteJSON(msg)
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
	messageID := c.generateMessageID(serverAddr, "pubToServer")
	message := c.getWSMessage()
	defer c.putWSMessage(message)
	message.Type = "publishToServer"
	message.ID = messageID
	message.Payload["server_addr"] = serverAddr
	message.Payload["data"] = msg
	if len(path) > 0 {
		message.Payload["path"] = path[0]
	}

	if opts == nil {
		if c.debug {
			c.debugLog("PublishToServer: no ack %s", serverAddr)
		}
		message.Payload["no_ack"] = true
		err := c.conn.WriteJSON(msg)
		return err == nil
	}

	err := c.conn.WriteJSON(message)
	if err != nil {
		if opts.OnFailure != nil {
			opts.OnFailure(err)
		}
		return false
	}

	resp, err := c.waitForResponse(messageID, 5*time.Second)
	if err != nil {
		if opts.OnFailure != nil {
			opts.OnFailure(err)
		}
		return false
	}

	if opts.OnSuccess != nil {
		opts.OnSuccess()
	}
	return resp.Type == "published"
}

func (c *Client) unsubscribe(topic, subID string) {
	messageID := c.generateMessageID(topic, "unsub-"+subID)
	msg := c.getWSMessage()
	defer c.putWSMessage(msg)
	msg.Type = "unsubscribe"
	msg.Topic = topic
	msg.Target = subID
	msg.ID = messageID
	msg.Payload["subID"] = subID
	if c.debug {
		c.debugTitle("unsub")
		c.debugLog("unsub from topic='%s', subID='%s'", topic, subID)
	}
	// Create response channel before sending
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(messageID, respChan)
	if c.debug {
		c.debugLog("sending unsub message %+v", msg)
	}
	err := c.conn.WriteJSON(msg)
	if err != nil {
		c.pending.Delete(messageID)
		return
	}

	// Wait for response
	select {
	case resp := <-respChan:
		if resp != nil && resp.Type == "unsubscribed" {
			// Clean up both specific and topic-level handlers
			handlerKey := topic + "-" + subID
			if c.debug {
				c.debugLog("unsub confirmed, deleting handler %s", handlerKey)
			}

			c.handlers.Delete(handlerKey)
		}
	case <-time.After(5 * time.Second):
		// Timeout, but still delete both handlers
		handlerKey := topic + "-" + subID
		c.handlers.Delete(handlerKey)
	}

	c.pending.Delete(messageID)
}

// Close closes the WebSocket connection
func (c *Client) Close() {
	if c.debug {
		c.debugLog("closing client")
	}
	if c.conn != nil {
		c.conn.Close()
	}
	if c.debug {
		c.debugLog("closed")
	}
	return
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
	if s.client.debug {
		s.client.debugTitle("clientSubscription unsub from topic='%s' subID='%s'", s.topic, s.subID)
	}
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

	msg := c.getWSMessage()
	msg.Type = "create_state_pool"
	msg.ID = c.generateMessageID("create-state-pool-client-go", config.Name)
	msg.Payload["config"] = serializableConfig
	err := c.sendStatePoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// RemovePoolHandler removes the message handler for a specific pool
func (c *Client) RemovePoolHandler(poolName string) {
	handlerID := fmt.Sprintf("pool_%s", poolName)
	c.handlers.Delete(handlerID)
}

// SavePoolState persists a pool's state to disk
func (c *Client) SavePoolState(poolName string, directory string) error {
	msg := c.getWSMessage()
	msg.Type = "save_pool_state"
	msg.ID = c.generateMessageID("save-state-pool", poolName+"-"+directory)
	msg.Payload["pool"] = poolName
	msg.Payload["directory"] = directory
	err := c.sendPoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// LoadPoolState loads a pool's state from disk
func (c *Client) LoadPoolState(poolName string, directory string) error {
	msg := c.getWSMessage()
	msg.Type = "load_pool_state"
	msg.ID = c.generateMessageID("load-pool-state", poolName+"-"+directory)
	msg.Payload["pool"] = poolName
	msg.Payload["directory"] = directory
	err := c.sendPoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// sendPoolMessage sends a message to a pool and handles the response
func (c *Client) sendPoolMessage(msg *WSMessage) error {
	if c.conn == nil || !c.isConnected {
		return fmt.Errorf("client not connected")
	}

	// Create response channel
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(msg.ID, respChan)
	defer c.pending.Delete(msg.ID)

	// Send request
	if err := c.conn.WriteJSON(msg); err != nil {
		return fmt.Errorf("failed to send pool message: %w", err)
	}

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
	msg := c.getWSMessage()
	msg.Type = "state_pool_message"
	msg.ID = c.generateMessageID("send-to-state-pool", poolName)
	msg.Payload["pool"] = poolName
	msg.Payload["data"] = data
	err := c.sendStatePoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// GetState retrieves the current state of a state pool
func (c *Client) GetState(poolName string) (map[string]any, error) {
	msg := c.getWSMessage()
	msg.Type = "state_pool_state"
	msg.ID = c.generateMessageID("getstate", poolName)
	msg.Payload["pool"] = poolName

	if c.conn == nil || !c.isConnected {
		c.putWSMessage(msg)
		return nil, fmt.Errorf("client not connected")
	}

	// Create response channel
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(msg.ID, respChan)
	defer c.pending.Delete(msg.ID)

	// Send request
	if err := c.conn.WriteJSON(msg); err != nil {
		c.putWSMessage(msg)
		return nil, fmt.Errorf("failed to send state request: %w", err)
	}

	// Wait for response with timeout
	resp, err := c.waitForResponse(msg.ID, 10*time.Second)
	// Now we can safely put the original message back
	c.putWSMessage(msg)

	if err != nil {
		return nil, err
	}
	defer c.putWSMessage(resp)

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
	msg := c.getWSMessage()
	msg.Type = "update_state_pool"
	msg.ID = c.generateMessageID("update-state", poolName)
	msg.Payload["pool"] = poolName
	msg.Payload["updates"] = updates
	err := c.sendStatePoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// DeleteStatePoolKeys deletes specific keys from a state pool's state
func (c *Client) DeleteStatePoolKeys(poolName string, keys []string) error {
	msg := c.getWSMessage()
	msg.Type = "delete_state_pool_keys"
	msg.ID = c.generateMessageID("delete-state", poolName)
	msg.Payload["pool"] = poolName
	msg.Payload["keys"] = keys
	err := c.sendStatePoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// ClearStatePool removes all values from a state pool's state
func (c *Client) ClearStatePool(poolName string) error {
	msg := c.getWSMessage()
	msg.Type = "clear_state_pool"
	msg.ID = c.generateMessageID("clear-state", poolName)
	msg.Payload["pool"] = poolName
	err := c.sendStatePoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// SaveStatePool persists a state pool's state to disk
func (c *Client) SaveStatePool(poolName string, directory string) error {
	msg := c.getWSMessage()
	msg.Type = "save_state_pool"
	msg.ID = c.generateMessageID("save-state", poolName+"-"+directory)
	msg.Payload["pool"] = poolName
	msg.Payload["directory"] = directory
	err := c.sendStatePoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// LoadStatePool loads a state pool's state from disk
func (c *Client) LoadStatePool(poolName string, directory string) error {
	msg := c.getWSMessage()
	msg.Type = "load_state_pool"
	msg.ID = c.generateMessageID("load-state", poolName+"-"+directory)
	msg.Payload["pool"] = poolName
	msg.Payload["directory"] = directory
	err := c.sendStatePoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// StopStatePool stops a state pool's processing
func (c *Client) StopStatePool(poolName string) error {
	msg := c.getWSMessage()
	msg.Type = "stop_state_pool"
	msg.ID = c.generateMessageID("stop-state", poolName)
	msg.Payload["pool"] = poolName
	err := c.sendStatePoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// RemoveStatePool stops and removes a state pool
func (c *Client) RemoveStatePool(poolName string) error {
	msg := c.getWSMessage()
	msg.Type = "remove_state_pool"
	msg.ID = c.generateMessageID("remove-state", poolName)
	msg.Payload["pool"] = poolName
	err := c.sendStatePoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// RemoveStatePoolHandler removes the message handler for a specific state pool
func (c *Client) RemoveStatePoolHandler(poolName string) {
	handlerID := fmt.Sprintf("state_pool_%s", poolName)
	c.handlers.Delete(handlerID)
}

// sendStatePoolMessage sends a message to a state pool and handles the response
func (c *Client) sendStatePoolMessage(msg *WSMessage) error {
	if c.conn == nil || !c.isConnected {
		return fmt.Errorf("client not connected")
	}

	// Create response channel
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(msg.ID, respChan)
	defer c.pending.Delete(msg.ID)

	// Send request
	if err := c.conn.WriteJSON(msg); err != nil {
		return fmt.Errorf("failed to send state pool message: %w", err)
	}

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
	msg := c.getWSMessage()
	msg.Type = "create_actor_pool"
	msg.ID = c.generateMessageID("create-actor", name)
	msg.Payload["config"] = map[string]any{
		"name": name,
		"size": size,
	}
	err := c.sendActorPoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// SendToActorPool sends a message to a specific actor pool
func (c *Client) SendToActorPool(poolName string, data any) error {
	msg := c.getWSMessage()
	msg.Type = "actor_pool_message"
	msg.ID = c.generateMessageID("send-actor", poolName)
	msg.Payload["pool"] = poolName
	msg.Payload["data"] = data
	err := c.sendActorPoolMessage(msg)
	c.putWSMessage(msg)
	return err
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
	msg := c.getWSMessage()
	msg.Type = "stop_actor_pool"
	msg.ID = c.generateMessageID("stop-actor", poolName)
	msg.Payload["pool"] = poolName
	err := c.sendActorPoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// RemoveActorPool stops and removes an actor pool
func (c *Client) RemoveActorPool(poolName string) error {
	msg := c.getWSMessage()
	msg.Type = "remove_actor_pool"
	msg.ID = c.generateMessageID("remove-actor", poolName)
	msg.Payload["pool"] = poolName
	err := c.sendActorPoolMessage(msg)
	c.putWSMessage(msg)
	return err
}

// sendActorPoolMessage sends a message to an actor pool and handles the response
func (c *Client) sendActorPoolMessage(msg *WSMessage) error {
	if c.conn == nil || !c.isConnected {
		return fmt.Errorf("client not connected")
	}

	// Create response channel
	respChan := make(chan *WSMessage, 1)
	c.pending.Set(msg.ID, respChan)
	defer c.pending.Delete(msg.ID)

	// Send request
	if err := c.conn.WriteJSON(msg); err != nil {
		return fmt.Errorf("failed to send actor pool message: %w", err)
	}

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

// getWSMessage gets a message from the pool and returns it
func (c *Client) getWSMessage() *WSMessage {
	return c.msgPool.Get().(*WSMessage)
}

// putWSMessage resets and returns a message to the pool
func (c *Client) putWSMessage(msg *WSMessage) {
	msg.Type = ""
	msg.Topic = ""
	msg.ID = ""
	msg.Target = ""
	msg.MsgID = ""
	clear(msg.Payload)
	c.msgPool.Put(msg)
}
