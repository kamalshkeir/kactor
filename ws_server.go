package kactor

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"encoding/json"

	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/ksmux/ws"
	"github.com/kamalshkeir/ksmux/wspool"
)

var ErrUpgradeWs = errors.New("error init websocket connection")
var ErrActorPoolAlreadyExists = errors.New("actor pool already exists")
var ErrActorPoolNotFound = errors.New("actor pool not found")

// PoolMessage represents a message sent to an actor pool
type PoolMessage struct {
	ClientID string
	Data     any
}

// Implement Message interface for PoolMessage
func (m PoolMessage) GetData() any {
	return m.Data
}

// MessageContext holds the message and its associated connection
type MessageContext struct {
	msg  *WSMessage
	conn *wspool.Conn
}

// BusServer handles WebSocket connections and message routing
type BusServer struct {
	id           string
	app          *ksmux.Router
	bus          *PubSub
	path         string
	debug        bool
	pools        *kmap.SafeMap[string, *StatefulActorPool]
	actorPools   *kmap.SafeMap[string, *ActorPool]
	serversConns *kmap.SafeMap[string, *Client]
	onServerData []func(map[string]any)
	// Message pool for better memory reuse
	messagePool *MessagePool
	// Connection tracking
	connections *kmap.SafeMap[string, *wspool.Conn]
	// Batch processing channels
	batchSize int
	batchChan chan *MessageContext
	// Add write buffer pool
	writeBufferPool sync.Pool
	// Add JSON encoder pool
	encoderPool sync.Pool
}

// debugLog prints debug messages if debug mode is enabled
func (b *BusServer) debugLog(format string, args ...interface{}) {
	if b.debug {
		fmt.Printf("[SERVER DEBUG] "+format+"\n", args...)
	}
}

func NewBusServer(config ...ksmux.Config) *BusServer {
	var conf *ksmux.Config
	if len(config) > 0 {
		conf = &config[0]
	}
	if conf == nil {
		conf = &ksmux.Config{
			Address: "localhost:9313",
		}
	}
	ps := NewPubSub()
	app := ksmux.New(*conf)

	b := &BusServer{
		id:           fmt.Sprintf("server-%d", time.Now().UnixNano()),
		app:          app,
		bus:          ps,
		path:         "/ws/kactor",
		debug:        false,
		pools:        kmap.New[string, *StatefulActorPool](32),
		actorPools:   kmap.New[string, *ActorPool](32),
		serversConns: kmap.New[string, *Client](32),
		messagePool:  NewMessagePool(),
		connections:  kmap.New[string, *wspool.Conn](32),
		batchSize:    100,
		batchChan:    make(chan *MessageContext, 1000),
		// Initialize buffer pool
		writeBufferPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, 1024))
			},
		},
		// Initialize encoder pool
		encoderPool: sync.Pool{
			New: func() interface{} {
				return json.NewEncoder(bytes.NewBuffer(nil))
			},
		},
	}

	// Add shutdown handlers
	app.OnShutdown(func() error {
		ps.Close()
		b.CloseServerConnections()
		wspool.CleanUpActors()
		// Stop all actor pools
		b.pools.Range(func(_ string, pool *StatefulActorPool) bool {
			pool.acpool.Stop()
			return true
		})
		b.actorPools.Range(func(_ string, pool *ActorPool) bool {
			pool.Stop()
			return true
		})
		return nil
	})

	return b
}

func (b *BusServer) ID() string {
	return b.id
}
func (b *BusServer) SetID(serverID string) {
	b.id = serverID
}

func (b *BusServer) sendErrorWS(err string, req *WSMessage, conn *wspool.Conn) {
	response := b.messagePool.Get()
	clear(response.Payload)
	response.Type = "error"
	response.ID = req.ID
	response.Topic = req.Topic
	response.Target = req.Target
	response.Payload["error"] = err
	_ = conn.WriteJSON(response)
	b.messagePool.Put(response)
}
func (b *BusServer) sendAckWS(acktype string, req *WSMessage, conn *wspool.Conn, payload ...map[string]any) {
	response := b.messagePool.Get()
	if len(payload) > 0 {
		response.Payload = payload[0]
	} else {
		clear(response.Payload)
	}
	response.Type = acktype
	response.ID = req.ID
	response.Topic = req.Topic
	response.Target = req.Target
	_ = conn.WriteJSON(response)
	b.messagePool.Put(response)
}

func (b *BusServer) HandleWS(c *ksmux.Context) {
	rawConn, err := wspool.UpgradeConnection(c.ResponseWriter, c.Request, nil)
	if err != nil {
		if b.debug {
			b.debugLog("[HandleWS] WebSocket upgrade failed: %v", err)
		}
		c.Error(ErrUpgradeWs)
		return
	}
	conn := wspool.DefaultPool().Add(rawConn)
	if conn == nil {
		if b.debug {
			b.debugLog("[HandleWS] ERROR: couldn't add to pool")
		}
		c.Error("could not add to pool")
		return
	}

	var clientID string
	// Ensure cleanup happens when the connection is closed
	defer func() {
		if b.debug {
			b.debugLog("[HandleWS] Cleaning connection client %s", clientID)
		}
		b.connections.Delete(clientID)
		conn.Close()
	}()

	// Handle incoming messages
	for {
		var msg *WSMessage
		err := conn.ReadJSON(&msg)
		if err != nil {
			if ws.IsCloseError(err, ws.CloseNormalClosure, ws.CloseGoingAway) {
				if b.debug {
					b.debugLog("[HandleWS] Client closed connection normally")
				}
			} else if !ws.IsUnexpectedCloseError(err, ws.CloseNormalClosure, ws.CloseGoingAway) {
				if b.debug {
					b.debugLog("[HandleWS] WebSocket error %v", err)
				}
			}
			return
		}

		// Store client ID for cleanup and connection tracking
		if msg.ID != "" {
			if conn.GetClientID() != msg.ID {
				conn.SetClientID(msg.ID)
			}
			clientID = msg.ID
			b.connections.Set(clientID, conn)
		}

		if b.debug {
			b.debugLog("[HandleWS] Received message: type=%s, topic=%s, id=%s", msg.Type, msg.Topic, msg.ID)
		}

		switch msg.Type {
		case "server_message":
			if sm, ok := msg.Payload["server_addr"].(string); ok {
				if b.onServerData != nil && sm == b.app.Address() {
					for _, fn := range b.onServerData {
						fn(msg.Payload)
					}
				}
				continue
			}
		case "publish":
			if b.debug {
				b.debugLog("[handlerPublish] got msg %+v", msg)
			}
			noAck := false
			if na, ok := msg.Payload["no_ack"].(bool); ok {
				noAck = na
				delete(msg.Payload, "no_ack")
			}
			if !noAck {
				po := &PublishOptions{
					OnSuccess: func() {
						b.sendAckWS("published", msg, conn)
					},
					OnFailure: func(err error) {
						b.sendErrorWS(err.Error(), msg, conn)
					},
				}
				success := b.bus.Publish(msg.Topic, msg.Payload, po)
				if !success {
					b.sendErrorWS("no subscribers found", msg, conn)
				}
			} else {
				success := b.bus.Publish(msg.Topic, msg.Payload, nil)
				if !success {
					b.sendErrorWS("no subscribers found", msg, conn)
				}
			}
		case "publishTo":
			if b.debug {
				b.debugLog("[handlerPublishTo] got msg %+v", msg)
			}
			noAck := false
			if na, ok := msg.Payload["no_ack"].(bool); ok {
				noAck = na
				delete(msg.Payload, "no_ack")
			}
			if !noAck {
				success := b.bus.PublishTo(msg.Topic, msg.Target, msg.Payload, &PublishOptions{
					OnSuccess: func() {
						b.sendAckWS("published", msg, conn)
					},
					OnFailure: func(err error) {
						b.sendErrorWS(err.Error(), msg, conn)
					},
				})
				if !success {
					if b.debug {
						b.debugLog("Failed to publish direct message to topic %s, target %s", msg.Topic, msg.Target)
					}
					b.sendErrorWS("Failed to publish direct message", msg, conn)
				}
			} else {
				success := b.bus.PublishTo(msg.Topic, msg.Target, msg.Payload, nil)
				if !success {
					b.sendErrorWS("no subs found", msg, conn)
				}
			}
		case "publishWithRetry":
			if b.debug {
				b.debugLog("[handlerPublishWithRetry] got msg %+v", msg)
			}
			noAck := false
			if na, ok := msg.Payload["no_ack"].(bool); ok {
				noAck = na
				delete(msg.Payload, "no_ack")
			}
			var cfg RetryConfig
			if cfgData, ok := msg.Payload["retry_config"].(map[string]any); ok {
				if maxAttempts, ok := cfgData["max_attempts"].(float64); ok {
					cfg.MaxAttempts = int(maxAttempts)
				}
				if maxBackoff, ok := cfgData["max_backoff"].(float64); ok {
					cfg.MaxBackoff = int(maxBackoff)
				}
			}
			if cfg.MaxAttempts == 0 {
				cfg = DefaultRetryConfig
			}

			dmap, ok := msg.Payload["data"].(map[string]any)
			if !ok {
				dmap = map[string]any{
					"data": msg.Payload["data"],
				}
			}

			if !noAck {
				success := b.bus.PublishWithRetry(msg.Topic, dmap, &cfg, &PublishOptions{
					OnSuccess: func() {
						b.sendAckWS("published", msg, conn)
					},
					OnFailure: func(err error) {
						b.sendErrorWS(err.Error(), msg, conn)
					},
				})
				if !success {
					if b.debug {
						b.debugLog("Failed to publish with retry to topic %s", msg.Topic)
					}
					b.sendErrorWS("Failed to publish with retry", msg, conn)
				}
			} else {
				success := b.bus.PublishWithRetry(msg.Topic, dmap, &cfg, nil)
				if !success {
					b.sendErrorWS("no subs", msg, conn)
				}
			}
		case "publishToWithRetry":
			if b.debug {
				b.debugLog("[handlerPublishToWithRetry] got msg %+v", msg)
			}
			noAck := false
			if na, ok := msg.Payload["no_ack"].(bool); ok {
				noAck = na
				delete(msg.Payload, "no_ack")
			}
			var cfg RetryConfig
			if cfgData, ok := msg.Payload["retry_config"].(map[string]any); ok {
				if maxAttempts, ok := cfgData["max_attempts"].(float64); ok {
					cfg.MaxAttempts = int(maxAttempts)
				}
				if maxBackoff, ok := cfgData["max_backoff"].(float64); ok {
					cfg.MaxBackoff = int(maxBackoff)
				}
			}
			if cfg.MaxAttempts == 0 {
				cfg = DefaultRetryConfig
			}

			if !noAck && msg.Target == "" {
				response := b.messagePool.Get()
				response.Type = "error"
				response.ID = msg.ID
				response.Topic = msg.Topic
				response.Target = msg.Target
				clear(response.Payload)
				response.Payload["error"] = "target field is required for publishTo"
				conn.WriteJSON(response)
				// Return the original message to the pool
				b.messagePool.Put(response)
				continue
			}

			dmap, ok := msg.Payload["data"].(map[string]any)
			if !ok {
				dmap = map[string]any{
					"data": msg.Payload["data"],
				}
			}

			if !noAck {
				success := b.bus.PublishToWithRetry(msg.Topic, msg.Target, dmap, &cfg, &PublishOptions{
					OnSuccess: func() {
						b.sendAckWS("published", msg, conn)
					},
					OnFailure: func(err error) {
						b.sendErrorWS(err.Error(), msg, conn)
					},
				})
				if !success {
					if b.debug {
						b.debugLog("Failed to publish direct message with retry to topic %s, target %s", msg.Topic, msg.Target)
					}
					b.sendErrorWS("Failed to publish direct message with retry", msg, conn)
				}
			} else {
				success := b.bus.PublishToWithRetry(msg.Topic, msg.Target, dmap, &cfg, nil)
				if !success {
					b.sendErrorWS("no subs pubToWithRetry", msg, conn)
				}
			}
		case "subscribe":
			if b.debug {
				b.debugLog("[handlerSubscribe] got msg %+v", msg)
			}
			sub := b.bus.Subscribe(msg.Topic, msg.Target, func(payload map[string]any, sub Subscription) {
				b.sendAckWS("message", msg, conn, payload)
			})

			if sub == nil {
				b.sendErrorWS("failed to subscribe", msg, conn)
				continue
			}
			b.sendAckWS("subscribed", msg, conn, map[string]any{
				"subID": msg.Target,
			})
		case "unsubscribe":
			if b.debug {
				b.debugLog("[handlerUnsubscribe] got msg %+v", msg)
			}
			// Extract subscription ID from payload or use message ID
			subID := msg.ID
			if sid, ok := msg.Payload["subID"].(string); ok && sid != "" {
				subID = sid
			}

			if b.bus.Unsubscribe(msg.Topic, subID) {
				b.sendAckWS("unsubscribed", msg, conn, map[string]any{
					"subID": subID,
				})
			} else {
				b.sendErrorWS("unsubscribe failed: subscription not found", msg, conn)
			}
		case "publishToServer":
			noAck := false
			if na, ok := msg.Payload["no_ack"].(bool); ok {
				noAck = na
				delete(msg.Payload, "no_ack")
			}
			// Handle sending message to another server
			serverAddr, ok := msg.Payload["server_addr"].(string)
			if !ok {
				b.sendErrorWS("missing or invalid server_addr", msg, conn)
				continue
			}

			data, ok := msg.Payload["data"].(map[string]any)
			if !ok {
				if b.debug {
					b.debugLog("[HandleWS] Invalid data in publishToServer message")
				}
				b.sendErrorWS("missing or invalid data", msg, conn)
				continue
			}

			// Get custom path if provided
			var path []string
			if customPath, ok := msg.Payload["path"].(string); ok {
				path = []string{customPath}
			}

			secure := false
			if sec, ok := msg.Payload["secure"].(bool); ok {
				secure = sec
			}

			// Send message to other server
			var success bool
			if !noAck {
				po := &PublishOptions{
					OnSuccess: func() {
						b.sendAckWS("published", msg, conn, map[string]any{
							"server_addr": serverAddr,
						})
					},
					OnFailure: func(err error) {
						b.sendAckWS("error", msg, conn, map[string]any{
							"error":       err.Error(),
							"server_addr": serverAddr,
						})
					},
				}
				success = b.PublishToServer(secure, serverAddr, data, po, path...)
				if !success {
					b.sendErrorWS("missing or invalid server_addr", msg, conn)
				}
			} else {
				success = b.PublishToServer(secure, serverAddr, data, nil, path...)
				if !success {
					b.sendErrorWS("couldn't send data to server", msg, conn)
				} else {
					b.sendAckWS("published", msg, conn, map[string]any{
						"server_addr": serverAddr,
					})
				}
			}
		case "has_subscribers":
			// Check if topic has subscribers
			b.sendAckWS("subscribers_status", msg, conn, map[string]any{
				"has_subscribers": b.bus.HasSubscribers(msg.Topic),
			})
		case "create_state_pool":
			if err := b.handleStatePoolCreation(msg, conn); err != nil {
				if b.debug {
					b.debugLog("[HandleWS] Failed to create pool: %v", err)
				}
				b.sendErrorWS(err.Error(), msg, conn)
			} else {
				if b.debug {
					b.debugLog("[HandleWS] Successfully created pool")
				}
			}
		case "create_actor_pool":
			if err := b.handleActorPoolCreation(msg, conn); err != nil {
				b.sendErrorWS(err.Error(), msg, conn)
			} else {
				if b.debug {
					b.debugLog("[HandleWS] Successfully created actor pool")
				}
			}
		case "actor_pool_message", "stop_actor_pool", "remove_actor_pool":
			if err := b.handleActorPoolMessage(msg, conn); err != nil {
				b.sendErrorWS(err.Error(), msg, conn)
			} else {
				if b.debug {
					b.debugLog("[HandleWS] got event %s", msg.Type)
				}
			}
		case "state_pool_message", "state_pool_state", "update_state_pool",
			"delete_state_pool_keys", "clear_state_pool", "save_state_pool",
			"load_state_pool", "stop_state_pool", "remove_state_pool":
			if err := b.handleStatePoolMessage(msg, conn); err != nil {
				b.sendErrorWS(err.Error(), msg, conn)
			}
		default:
			if b.debug {
				b.debugLog("[HandleWS] Unknown message type from client %p: %s", conn, msg.Type)
			}
			b.sendErrorWS("type not handled", msg, conn)
		}
	}
}

// publishMessage represents a message to be published
type publishMessage struct {
	msg  *WSMessage
	conn *wspool.Conn
}

// Implement Message interface for publishMessage
func (m *publishMessage) GetData() any {
	return m.msg
}

// MessagePool manages a pool of WSMessage objects
type MessagePool struct {
	pool sync.Pool
}

func NewMessagePool() *MessagePool {
	return &MessagePool{
		pool: sync.Pool{
			New: func() interface{} {
				return &WSMessage{
					Payload: make(map[string]any),
				}
			},
		},
	}
}

// Get gets a message from the pool
func (p *MessagePool) Get() *WSMessage {
	return p.pool.Get().(*WSMessage)
}

// Put returns a message to the pool
func (p *MessagePool) Put(msg *WSMessage) {
	msg.Type = ""
	msg.Topic = ""
	msg.ID = ""
	msg.Target = ""
	msg.MsgID = ""
	for k := range msg.Payload {
		delete(msg.Payload, k)
	}
	p.pool.Put(msg)
}

// WSMessage implements Message interface
func (m *WSMessage) GetData() any {
	return m
}

// RemovePool removes and stops a stateful actor pool
func (b *BusServer) RemovePool(poolName string) {
	if pool, exists := b.pools.Get(poolName); exists {
		pool.acpool.Stop()
		b.pools.Delete(poolName)
	}
}

// addPool adds a new stateful actor pool
func (b *BusServer) addPool(poolName string, pool *StatefulActorPool) error {
	if _, exists := b.pools.Get(poolName); exists {
		return ErrActorPoolAlreadyExists
	}
	b.pools.Set(poolName, pool)
	return nil
}

// Handle pool-related WebSocket messages
func (b *BusServer) handleStatePoolMessage(msg *WSMessage, conn *wspool.Conn) error {
	switch msg.Type {
	case "state_pool_message":
		// Extract pool name from payload
		poolName, ok := msg.Payload["pool"].(string)
		if !ok {
			return errors.New("pool name required")
		}

		// Get the pool
		pool, exists := b.pools.Get(poolName)
		if !exists {
			return ErrActorPoolNotFound
		}

		// Send message to pool
		success := pool.acpool.Send(PoolMessage{
			ClientID: msg.ID,
			Data:     msg.Payload["data"],
		})

		if success {
			// Send success response
			response := b.messagePool.Get()
			response.Type = "state_pool_message_sent"
			response.ID = msg.ID
			clear(response.Payload)
			response.Payload["pool"] = poolName
			err := conn.WriteJSON(response)
			b.messagePool.Put(response)
			if err != nil {
				return err
			}

			// Send message to client handler
			message := b.messagePool.Get()
			message.Type = "state_pool_message"
			message.ID = msg.ID
			// Clear and reuse payload map
			for k := range message.Payload {
				delete(message.Payload, k)
			}
			message.Payload["pool"] = poolName
			message.Payload["data"] = msg.Payload["data"]
			err = conn.WriteJSON(message)
			b.messagePool.Put(message)
			return err
		}
		return errors.New("failed to send message to state pool")

	case "state_pool_state":
		// Extract pool name from payload
		poolName, ok := msg.Payload["pool"].(string)
		if !ok {
			return errors.New("pool name required")
		}

		// Get the pool
		pool, exists := b.pools.Get(poolName)
		if !exists {
			return ErrActorPoolNotFound
		}

		// Get current state
		state := make(map[string]any)
		pool.State().Range(func(key string, value any) bool {
			state[key] = value
			return true
		})

		b.sendAckWS("state_pool_state", msg, conn, map[string]any{
			"pool":  poolName,
			"state": state,
		})
		return nil

	case "update_state_pool":
		// Extract pool name and updates
		poolName, ok := msg.Payload["pool"].(string)
		if !ok {
			return errors.New("pool name required")
		}

		updates, ok := msg.Payload["updates"].(map[string]any)
		if !ok {
			return errors.New("updates required")
		}

		// Get the pool
		pool, exists := b.pools.Get(poolName)
		if !exists {
			return ErrActorPoolNotFound
		}

		// Apply updates
		for key, value := range updates {
			pool.SetStateValue(key, value)
		}

		b.sendAckWS("state_pool_updated", msg, conn, map[string]any{
			"pool": poolName,
		})
		return nil

	case "delete_state_pool_keys":
		// Extract pool name and keys
		poolName, ok := msg.Payload["pool"].(string)
		if !ok {
			return errors.New("pool name required")
		}

		keys, ok := msg.Payload["keys"].([]any)
		if !ok {
			return errors.New("keys required")
		}

		// Get the pool
		pool, exists := b.pools.Get(poolName)
		if !exists {
			return ErrActorPoolNotFound
		}

		// Delete keys
		for _, k := range keys {
			if key, ok := k.(string); ok {
				pool.DeleteStateValue(key)
			}
		}

		b.sendAckWS("state_pool_keys_deleted", msg, conn, map[string]any{
			"pool": poolName,
		})
		return nil

	case "clear_state_pool":
		// Extract pool name
		poolName, ok := msg.Payload["pool"].(string)
		if !ok {
			return errors.New("pool name required")
		}

		// Get the pool
		pool, exists := b.pools.Get(poolName)
		if !exists {
			return ErrActorPoolNotFound
		}

		// Clear state
		pool.ClearState()

		b.sendAckWS("state_pool_cleared", msg, conn, map[string]any{
			"pool": poolName,
		})
		return nil

	case "save_state_pool":
		// Extract pool name and directory
		poolName, ok := msg.Payload["pool"].(string)
		if !ok {
			return errors.New("pool name required")
		}

		directory, ok := msg.Payload["directory"].(string)
		if !ok {
			return errors.New("directory required")
		}

		// Get the pool
		pool, exists := b.pools.Get(poolName)
		if !exists {
			return ErrActorPoolNotFound
		}

		// Save state
		if err := pool.SaveState(directory); err != nil {
			return fmt.Errorf("failed to save state: %w", err)
		}

		b.sendAckWS("state_pool_saved", msg, conn, map[string]any{
			"pool": poolName,
		})
		return nil

	case "load_state_pool":
		// Extract pool name and directory
		poolName, ok := msg.Payload["pool"].(string)
		if !ok {
			return errors.New("pool name required")
		}

		directory, ok := msg.Payload["directory"].(string)
		if !ok {
			return errors.New("directory required")
		}

		// Get the pool
		pool, exists := b.pools.Get(poolName)
		if !exists {
			return ErrActorPoolNotFound
		}

		// Load state
		if err := pool.LoadState(directory); err != nil {
			return fmt.Errorf("failed to load state: %w", err)
		}

		b.sendAckWS("state_pool_loaded", msg, conn, map[string]any{
			"pool": poolName,
		})
		return nil

	case "stop_state_pool":
		// Extract pool name
		poolName, ok := msg.Payload["pool"].(string)
		if !ok {
			return errors.New("pool name required")
		}

		// Get the pool
		pool, exists := b.pools.Get(poolName)
		if !exists {
			return ErrActorPoolNotFound
		}

		// Stop the pool
		pool.acpool.Stop()

		b.sendAckWS("state_pool_stopped", msg, conn, map[string]any{
			"pool": poolName,
		})
		return nil

	case "remove_state_pool":
		// Extract pool name
		poolName, ok := msg.Payload["pool"].(string)
		if !ok {
			return errors.New("pool name required")
		}

		// Check if pool exists
		if _, exists := b.pools.Get(poolName); !exists {
			return ErrActorPoolNotFound
		}

		// Remove the pool
		b.RemovePool(poolName)

		b.sendAckWS("state_pool_removed", msg, conn, map[string]any{
			"pool": poolName,
		})
		return nil

	default:
		return nil // Not a pool message, continue normal processing
	}
}

// addActorPool adds a new actor pool
func (b *BusServer) addActorPool(poolName string, pool *ActorPool) error {
	if _, exists := b.actorPools.Get(poolName); exists {
		return ErrActorPoolAlreadyExists
	}
	b.actorPools.Set(poolName, pool)
	return nil
}

// RemoveActorPool removes and stops an actor pool
func (b *BusServer) RemoveActorPool(poolName string) {
	if pool, exists := b.actorPools.Get(poolName); exists {
		pool.Stop()
		b.actorPools.Delete(poolName)
	}
}

// GetActorPool returns an actor pool by name
func (b *BusServer) GetActorPool(poolName string) (*ActorPool, error) {
	pool, exists := b.actorPools.Get(poolName)
	if !exists {
		return nil, ErrActorPoolNotFound
	}
	return pool, nil
}

// ActorPools returns the map of actor pools
func (b *BusServer) ActorPools() *kmap.SafeMap[string, *ActorPool] {
	return b.actorPools
}

// CreateActorPool creates a new actor pool with the given configuration
func (b *BusServer) CreateActorPool(name string, size int, handler func([]Message)) (*ActorPool, error) {
	if name == "" {
		return nil, fmt.Errorf("CreateActorPool has been given empty name")
	}
	if size < 1 {
		size = 1
	}
	pool, err := NewActorPool(name, size, handler)
	if err != nil {
		return nil, err
	}
	if err := b.addActorPool(name, pool); err != nil {
		return nil, fmt.Errorf("failed to add pool: %w", err)
	}
	return pool, nil
}

func (b *BusServer) handleActorPoolCreation(msg *WSMessage, conn *wspool.Conn) error {
	if b.debug {
		b.debugLog("[handleActorPoolCreation] Handling actor pool creation request")
	}
	config, ok := msg.Payload["config"].(map[string]any)
	if !ok {
		if b.debug {
			b.debugLog("[handleActorPoolCreation] Invalid pool configuration: %v", msg.Payload)
		}
		return fmt.Errorf("invalid pool configuration")
	}

	// Extract pool configuration
	name, ok := config["name"].(string)
	if !ok {
		if b.debug {
			b.debugLog("[handleActorPoolCreation] Pool name is required but not found in config: %v", config)
		}
		return fmt.Errorf("pool name is required")
	}
	if b.debug {
		b.debugLog("[handleActorPoolCreation] Creating actor pool with name: %s", name)
	}

	size, ok := config["size"].(float64)
	if !ok {
		if b.debug {
			b.debugLog("[handleActorPoolCreation] Size not found in config, defaulting to 1")
		}
		size = 1
	}

	// Validate size
	if size <= 0 {
		if b.debug {
			b.debugLog("[handleActorPoolCreation] Invalid pool size: %v", size)
		}
		return fmt.Errorf("pool size must be greater than 0")
	}

	// Create handler function
	handler := func(msgs []Message) {
		for _, msg := range msgs {
			if poolMsg, ok := msg.(PoolMessage); ok {
				response := b.messagePool.Get()
				response.Type = "actor_pool_message"
				response.ID = poolMsg.ClientID
				clear(response.Payload)
				response.Payload["pool"] = name
				response.Payload["data"] = poolMsg.Data
				if b.debug {
					b.debugLog("[handleActorPoolCreation][handlerfunc] Sending actor pool message response")
				}
				conn.WriteJSON(response)
				b.messagePool.Put(response)
			}
		}
	}

	// Create the pool
	if b.debug {
		b.debugLog("[handleActorPoolCreation] Creating actor pool with size: %d", int(size))
	}
	pool, err := b.CreateActorPool(name, int(size), handler)
	if err != nil {
		if b.debug {
			b.debugLog("[handleActorPoolCreation] Failed to create actor pool: %v", err)
		}
		return fmt.Errorf("failed to create pool: %w", err)
	}

	// Send success response
	if b.debug {
		b.debugLog("[handleActorPoolCreation]  Sending success response for actor pool creation")
	}
	b.sendAckWS("actor_pool_created", msg, conn, map[string]any{
		"pool": name,
		"size": pool.Size(),
	})
	return err
}

func (b *BusServer) handleActorPoolMessage(msg *WSMessage, conn *wspool.Conn) error {
	switch msg.Type {
	case "actor_pool_message":
		// Extract pool name from payload
		poolName, ok := msg.Payload["pool"].(string)
		if !ok {
			return errors.New("pool name required")
		}

		// Get the pool
		pool, exists := b.actorPools.Get(poolName)
		if !exists {
			return ErrActorPoolNotFound
		}

		// Send message to pool
		success := pool.Send(PoolMessage{
			ClientID: msg.ID,
			Data:     msg.Payload["data"],
		})

		if success {
			// Send success response
			b.sendAckWS("actor_pool_message_sent", msg, conn, map[string]any{
				"pool": poolName,
			})
			return nil
		}
		return errors.New("failed to send message to actor pool")

	case "stop_actor_pool":
		// Extract pool name
		poolName, ok := msg.Payload["pool"].(string)
		if !ok {
			return errors.New("pool name required")
		}

		// Get the pool
		pool, exists := b.actorPools.Get(poolName)
		if !exists {
			return ErrActorPoolNotFound
		}

		// Stop the pool
		pool.Stop()

		b.sendAckWS("actor_pool_stopped", msg, conn, map[string]any{
			"pool": poolName,
		})
		return nil

	case "remove_actor_pool":
		// Extract pool name
		poolName, ok := msg.Payload["pool"].(string)
		if !ok {
			return errors.New("pool name required")
		}

		// Check if pool exists
		if _, exists := b.actorPools.Get(poolName); !exists {
			return ErrActorPoolNotFound
		}

		// Remove the pool
		b.RemoveActorPool(poolName)

		b.sendAckWS("actor_pool_removed", msg, conn, map[string]any{
			"pool": poolName,
		})
		return nil

	default:
		return nil // Not an actor pool message, continue normal processing
	}
}

func (b *BusServer) handleStatePoolCreation(msg *WSMessage, conn *wspool.Conn) error {
	if b.debug {
		b.debugLog("[handleStatePoolCreation] Handling state pool creation request")
	}
	config, ok := msg.Payload["config"].(map[string]any)
	if !ok {
		return fmt.Errorf("invalid pool configuration")
	}

	// Extract pool configuration
	name, ok := config["name"].(string)
	if !ok {
		return fmt.Errorf("pool name is required")
	}
	if b.debug {
		b.debugLog("[handleStatePoolCreation] Creating state pool with name: %s", name)
	}

	size, ok := config["size"].(float64)
	if !ok {
		size = 1
	}

	initial, okin := config["initial"].(map[string]any)
	if !okin {
		initial = map[string]any{}
	}
	stateSizeMB, okSize := config["state_size_mb"].(float64)
	if !okSize {
		stateSizeMB = 32
	}
	// Create pool configuration
	poolConfig := StatefulPoolConfig{
		Size:        int(size),
		Name:        name,
		Initial:     initial,
		StateSizeMB: int(stateSizeMB),
		Handler: func(msg Message, state *kmap.SafeMap[string, any]) error {
			// Default handler that can be updated later
			return nil
		},
	}

	// Create the pool
	if b.debug {
		b.debugLog("[handleStatePoolCreation] Creating pool with config: %+v", poolConfig)
	}
	pool, err := NewStatefulPool(name, poolConfig)
	if err != nil {
		return fmt.Errorf("failed to create pool: %w", err)
	}

	// Add pool to server
	if err := b.addPool(name, pool); err != nil {
		return fmt.Errorf("failed to add pool: %w", err)
	}
	if b.debug {
		b.debugLog("[handleStatePoolCreation] Pool added successfully")
	}

	// Send success response
	if b.debug {
		b.debugLog("[handleStatePoolCreation] Sending success response")
	}
	b.sendAckWS("state_pool_created", msg, conn, map[string]any{
		"pool": name,
	})
	return err
}

func (b *BusServer) WithCustomPath(path string) *BusServer {
	b.path = path
	return b
}

func (b *BusServer) StatefulPools() *kmap.SafeMap[string, *StatefulActorPool] {
	return b.pools
}

func (b *BusServer) App() *ksmux.Router {
	return b.app
}
func (b *BusServer) Path() string {
	return b.path
}
func (b *BusServer) Router() *ksmux.Router {
	return b.app
}
func (b *BusServer) Connections() *kmap.SafeMap[string, *wspool.Conn] {
	return b.connections
}
func (b *BusServer) ServerConnections() *kmap.SafeMap[string, *Client] {
	return b.serversConns
}
func (b *BusServer) WithCustomRouter(router *ksmux.Router) *BusServer {
	b.app = router
	return b
}
func (b *BusServer) Bus() *PubSub {
	return b.bus
}
func (b *BusServer) PubSub() *PubSub {
	return b.bus
}
func (b *BusServer) WithCustomPubsub(ps *PubSub) *BusServer {
	b.bus = ps
	return b
}

func (b *BusServer) Run() {
	// Load states if directory is configured
	b.app.Get(b.path, b.HandleWS)
	b.app.Run()
}

func (b *BusServer) RunTLS() {
	b.app.Get(b.path, b.HandleWS)
	b.app.RunTLS()
}

func (b *BusServer) RunAutoTLS() {
	b.app.Get(b.path, b.HandleWS)
	b.app.RunAutoTLS()
}

func (s *BusServer) WithPprof(path ...string) {
	s.App().WithPprof(path...)
}

func (s *BusServer) WithMetrics(httpHandler http.Handler, path ...string) {
	s.App().WithMetrics(httpHandler, path...)
}

// PublishToServer sends a message to another BusServer
func (b *BusServer) PublishToServer(secure bool, serverAddr string, msg map[string]any, opts *PublishOptions, path ...string) bool {
	if b.debug {
		b.debugLog("=== PUBLISH TO SERVER START ===")
		b.debugLog("Publishing to server %s", serverAddr)
	}
	// Get or create client connection to target server
	client, exists := b.serversConns.Get(serverAddr)
	if !exists {
		var err error
		client, err = NewClient(ClientConfig{
			Address: serverAddr,
			ID:      "server-" + serverAddr,
			Secure:  secure,
			Path: func() string {
				if len(path) > 0 {
					return path[0]
				}
				return b.path
			}(),
		})
		if err != nil {
			if b.debug {
				b.debugLog("Failed to create client connection: %v", err)
			}
			if opts != nil && opts.OnFailure != nil {
				opts.OnFailure(fmt.Errorf("failed to connect to server %s: %w", serverAddr, err))
			}
			return false
		}
		b.serversConns.Set(serverAddr, client)
	}

	// Send message to target server
	msg["server_addr"] = serverAddr
	msg["from_server"] = b.app.Address()
	msg["from_secure"] = b.app.IsTls()
	success := client.sendToServer(serverAddr, msg, opts)
	if !success {
		b.serversConns.Delete(serverAddr)
		if b.debug {
			b.debugLog("Failed to publish message to server %s", serverAddr)
		}
		return false
	}

	if b.debug {
		b.debugLog("Successfully published message to server %s", serverAddr)
		b.debugLog("=== PUBLISH TO SERVER END ===")
	}
	return true
}

// CloseServerConnections closes all server-to-server connections
func (b *BusServer) CloseServerConnections() {
	if b.serversConns != nil {
		b.serversConns.Range(func(_ string, client *Client) bool {
			client.Close()
			return true
		})
	}
}

func (b *BusServer) Stop() {
	if b != nil {
		b.CloseServerConnections()
		if b.bus != nil {
			b.bus.Close()
		}
		if b.app != nil {
			b.app.Stop()
		}
	}
}

// OnServerData triggered when this server receive data from another server
func (b *BusServer) OnServerData(fn func(map[string]any)) {
	b.onServerData = append(b.onServerData, fn)
}

// WithDebug enables or disables debug logging
func (b *BusServer) WithDebug(enabled bool) *BusServer {
	b.debug = enabled
	b.bus.debug = enabled
	return b
}

// Update the LowAllocPubSub interface to use wspool.Conn
func (b *BusServer) CleanupConnection(conn *wspool.Conn) {
	b.bus.CleanupConnection(conn)
}

func (b *BusServer) handleSubscribe(ctx *MessageContext) {
	// Subscribe to topic
	sub := b.bus.Subscribe(ctx.msg.Topic, ctx.msg.Target, func(payload map[string]any, sub Subscription) {
		response := b.messagePool.Get()
		response.Type = "message"
		response.Topic = ctx.msg.Topic
		response.Target = ctx.msg.Target
		response.ID = ctx.msg.ID
		// Copy payload data
		for k, v := range payload {
			response.Payload[k] = v
		}
		// Send response and return to pool
		ctx.conn.WriteJSON(response)
		b.messagePool.Put(response)
	})

	if sub == nil {
		response := b.messagePool.Get()
		response.Type = "error"
		response.ID = ctx.msg.ID
		response.Topic = ctx.msg.Topic
		response.Target = ctx.msg.Target
		response.Payload["error"] = "failed to subscribe"
		ctx.conn.WriteJSON(response)
		b.messagePool.Put(response)
		return
	}

	// Send immediate subscription confirmation
	response := b.messagePool.Get()
	response.Type = "subscribed"
	response.Topic = ctx.msg.Topic
	response.ID = ctx.msg.ID
	response.Target = ctx.msg.Target
	response.Payload["subID"] = ctx.msg.Target
	// Send synchronously to ensure subscription is confirmed before any publishes
	ctx.conn.WriteJSON(response)
	b.messagePool.Put(response)
}

func (b *BusServer) handlePoolMessage(ctx *MessageContext) {
	switch ctx.msg.Type {
	case "actor_pool_message":
		if err := b.handleActorPoolMessage(ctx.msg, ctx.conn); err != nil {
			if b.debug {
				b.debugLog("Error handling actor pool message: %v", err)
			}
		}
	case "state_pool_message":
		if err := b.handleStatePoolMessage(ctx.msg, ctx.conn); err != nil {
			if b.debug {
				b.debugLog("Error handling state pool message: %v", err)
			}
		}
	}
}
