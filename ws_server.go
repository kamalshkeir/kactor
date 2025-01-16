package kactor

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/ksmux/ws"
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

// BusServer handles WebSocket connections and message routing
type BusServer struct {
	id           string
	app          *ksmux.Router
	bus          *PubSub
	path         string
	onServerData func(map[string]any)
	// Server-to-server connections
	busServersConns *kmap.SafeMap[string, *Client]
	// Stateful actor pools
	pools *kmap.SafeMap[string, *StatefulActorPool]
	// Regular actor pools
	actorPools *kmap.SafeMap[string, *ActorPool]
	// Debug mode
	debug bool
	mu    sync.RWMutex // For thread-safe operations
}

// debugLog prints debug messages if debug mode is enabled
func (b *BusServer) debugLog(format string, args ...interface{}) {
	if b.debug {
		fmt.Printf("[SERVER DEBUG] "+format+"\n", args...)
	}
}

// SafeConn wraps a WebSocket connection with synchronized writes
type SafeConn struct {
	conn *ws.Conn
	mu   sync.Mutex
}

// WriteJSON safely writes a JSON message to the connection
func (sc *SafeConn) WriteJSON(v interface{}) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.conn.WriteJSON(v)
}

// ReadMessage safely reads a message from the connection
func (sc *SafeConn) ReadMessage() (messageType int, p []byte, err error) {
	return sc.conn.ReadMessage()
}

// Close closes the underlying connection
func (sc *SafeConn) Close() error {
	return sc.conn.Close()
}

func NewBusServer(config ...ksmux.Config) *BusServer {
	var conf *ksmux.Config
	if len(config) > 0 {
		conf = &config[0]
	}
	if conf == nil {
		conf = &ksmux.Config{}
	}
	ps := NewPubSub()
	app := ksmux.New(*conf)
	b := &BusServer{
		app:             app,
		bus:             ps,
		path:            "/ws/kactor",
		debug:           false,
		pools:           kmap.New[string, *StatefulActorPool](32),
		actorPools:      kmap.New[string, *ActorPool](32),
		busServersConns: kmap.New[string, *Client](32),
	}
	// Add shutdown handlers
	app.OnShutdown(func() error {
		ps.Close()
		b.CloseServerConnections()
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

// addPool adds a new stateful actor pool
func (b *BusServer) addPool(poolName string, pool *StatefulActorPool) error {
	if _, exists := b.pools.Get(poolName); exists {
		return ErrActorPoolAlreadyExists
	}
	b.pools.Set(poolName, pool)
	return nil
}

// RemovePool removes and stops a stateful actor pool
func (b *BusServer) RemovePool(poolName string) {
	if pool, exists := b.pools.Get(poolName); exists {
		pool.acpool.Stop()
		b.pools.Delete(poolName)
	}
}

// Handle pool-related WebSocket messages
func (b *BusServer) handleStatePoolMessage(msg *WSMessage, conn *SafeConn) error {
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
			response := WSMessage{
				Type: "state_pool_message_sent",
				ID:   msg.ID,
				Payload: map[string]any{
					"pool": poolName,
				},
			}
			if err := conn.WriteJSON(response); err != nil {
				return err
			}

			// Send message to client handler
			message := WSMessage{
				Type: "state_pool_message",
				ID:   msg.ID,
				Payload: map[string]any{
					"pool": poolName,
					"data": msg.Payload["data"],
				},
			}
			return conn.WriteJSON(message)
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

		response := WSMessage{
			Type: "state_pool_state",
			ID:   msg.ID,
			Payload: map[string]any{
				"pool":  poolName,
				"state": state,
			},
		}
		return conn.WriteJSON(response)

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

		response := WSMessage{
			Type: "state_pool_updated",
			ID:   msg.ID,
			Payload: map[string]any{
				"pool": poolName,
			},
		}
		return conn.WriteJSON(response)

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

		response := WSMessage{
			Type: "state_pool_keys_deleted",
			ID:   msg.ID,
			Payload: map[string]any{
				"pool": poolName,
			},
		}
		return conn.WriteJSON(response)

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

		response := WSMessage{
			Type: "state_pool_cleared",
			ID:   msg.ID,
			Payload: map[string]any{
				"pool": poolName,
			},
		}
		return conn.WriteJSON(response)

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

		response := WSMessage{
			Type: "state_pool_saved",
			ID:   msg.ID,
			Payload: map[string]any{
				"pool": poolName,
			},
		}
		return conn.WriteJSON(response)

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

		response := WSMessage{
			Type: "state_pool_loaded",
			ID:   msg.ID,
			Payload: map[string]any{
				"pool": poolName,
			},
		}
		return conn.WriteJSON(response)

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

		response := WSMessage{
			Type: "state_pool_stopped",
			ID:   msg.ID,
			Payload: map[string]any{
				"pool": poolName,
			},
		}
		return conn.WriteJSON(response)

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

		response := WSMessage{
			Type: "state_pool_removed",
			ID:   msg.ID,
			Payload: map[string]any{
				"pool": poolName,
			},
		}
		return conn.WriteJSON(response)

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

func (b *BusServer) handleActorPoolCreation(msg *WSMessage, conn *SafeConn) error {
	b.debugLog("Handling actor pool creation request")
	config, ok := msg.Payload["config"].(map[string]any)
	if !ok {
		b.debugLog("Invalid pool configuration: %v", msg.Payload)
		return fmt.Errorf("invalid pool configuration")
	}

	// Extract pool configuration
	name, ok := config["name"].(string)
	if !ok {
		b.debugLog("Pool name is required but not found in config: %v", config)
		return fmt.Errorf("pool name is required")
	}
	b.debugLog("Creating actor pool with name: %s", name)

	size, ok := config["size"].(float64)
	if !ok {
		b.debugLog("Size not found in config, defaulting to 1")
		size = 1
	}

	// Validate size
	if size <= 0 {
		b.debugLog("Invalid pool size: %v", size)
		return fmt.Errorf("pool size must be greater than 0")
	}

	// Create handler function
	handler := func(msgs []Message) {
		for _, msg := range msgs {
			if poolMsg, ok := msg.(PoolMessage); ok {
				response := WSMessage{
					Type: "actor_pool_message",
					ID:   poolMsg.ClientID,
					Payload: map[string]any{
						"pool": name,
						"data": poolMsg.Data,
					},
				}
				b.debugLog("Sending actor pool message response")
				conn.WriteJSON(response)
			}
		}
	}

	// Create the pool
	b.debugLog("Creating actor pool with size: %d", int(size))
	pool, err := b.CreateActorPool(name, int(size), handler)
	if err != nil {
		b.debugLog("Failed to create actor pool: %v", err)
		return fmt.Errorf("failed to create pool: %w", err)
	}

	// Send success response
	response := WSMessage{
		Type: "actor_pool_created",
		ID:   msg.ID,
		Payload: map[string]any{
			"pool": name,
			"size": pool.Size(),
		},
	}
	b.debugLog("Sending success response for actor pool creation")
	return conn.WriteJSON(response)
}

func (b *BusServer) handleActorPoolMessage(msg *WSMessage, conn *SafeConn) error {
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
			response := WSMessage{
				Type: "actor_pool_message_sent",
				ID:   msg.ID,
				Payload: map[string]any{
					"pool": poolName,
				},
			}
			return conn.WriteJSON(response)
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

		response := WSMessage{
			Type: "actor_pool_stopped",
			ID:   msg.ID,
			Payload: map[string]any{
				"pool": poolName,
			},
		}
		return conn.WriteJSON(response)

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

		response := WSMessage{
			Type: "actor_pool_removed",
			ID:   msg.ID,
			Payload: map[string]any{
				"pool": poolName,
			},
		}
		return conn.WriteJSON(response)

	default:
		return nil // Not an actor pool message, continue normal processing
	}
}

func (b *BusServer) handleStatePoolCreation(msg *WSMessage, conn *SafeConn) error {
	b.debugLog("Handling state pool creation request")
	config, ok := msg.Payload["config"].(map[string]any)
	if !ok {
		return fmt.Errorf("invalid pool configuration")
	}

	// Extract pool configuration
	name, ok := config["name"].(string)
	if !ok {
		return fmt.Errorf("pool name is required")
	}
	b.debugLog("Creating state pool with name: %s", name)

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
	b.debugLog("Creating pool with config: %+v", poolConfig)
	pool, err := NewStatefulPool(name, poolConfig)
	if err != nil {
		return fmt.Errorf("failed to create pool: %w", err)
	}

	// Add pool to server
	if err := b.addPool(name, pool); err != nil {
		return fmt.Errorf("failed to add pool: %w", err)
	}
	b.debugLog("Pool added successfully")

	// Send success response
	response := WSMessage{
		Type: "state_pool_created",
		ID:   msg.ID,
		Payload: map[string]any{
			"pool": name,
		},
	}
	b.debugLog("Sending success response")
	return conn.WriteJSON(response)
}

func (b *BusServer) HandleWS(c *ksmux.Context) {
	rawConn, err := c.UpgradeConnection()
	if err != nil {
		b.debugLog("WebSocket upgrade failed: %v", err)
		c.Error(ErrUpgradeWs)
		return
	}

	conn := &SafeConn{conn: rawConn}

	var clientID string
	// Ensure cleanup happens when the connection is closed
	defer func() {
		conn.Close()
		// Clean up all subscriptions for this client
		b.bus.CleanupConnection(rawConn)
		b.debugLog("Cleaned up client %s", clientID)
	}()

	// Handle incoming messages
	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			if ws.IsCloseError(err, ws.CloseNormalClosure, ws.CloseGoingAway) {
				b.debugLog("Client closed connection normally")
			} else if !ws.IsUnexpectedCloseError(err, ws.CloseNormalClosure, ws.CloseGoingAway) {
				b.debugLog("WebSocket error %v", err)
			}
			return
		}

		// Handle close messages
		if messageType == ws.CloseMessage {
			b.debugLog("Received close message from client %p", conn)
			return
		}

		var msg WSMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			b.debugLog("Invalid message format from client %p: %v", conn, err)
			continue
		}

		// Store client ID for cleanup
		if msg.ID != "" {
			clientID = msg.ID
		}

		b.debugLog("Received message: type=%s, id=%s",
			msg.Type, msg.ID)

		// Handle messages
		switch msg.Type {
		case "create_state_pool":
			if err := b.handleStatePoolCreation(&msg, conn); err != nil {
				response := WSMessage{
					Type: "error",
					ID:   msg.ID,
					Payload: map[string]any{
						"error": err.Error(),
					},
				}
				conn.WriteJSON(response)
				b.debugLog("Failed to create pool: %v", err)
			} else {
				b.debugLog("Successfully created pool")
			}
		case "create_actor_pool":
			if err := b.handleActorPoolCreation(&msg, conn); err != nil {
				response := WSMessage{
					Type: "error",
					ID:   msg.ID,
					Payload: map[string]any{
						"error": err.Error(),
					},
				}
				conn.WriteJSON(response)
				b.debugLog("Failed to create actor pool: %v", err)
			} else {
				b.debugLog("Successfully created actor pool")
			}
		case "actor_pool_message", "stop_actor_pool", "remove_actor_pool":
			if err := b.handleActorPoolMessage(&msg, conn); err != nil {
				response := WSMessage{
					Type: "error",
					ID:   msg.ID,
					Payload: map[string]any{
						"error": err.Error(),
					},
				}
				conn.WriteJSON(response)
			}
		case "state_pool_message", "state_pool_state", "update_state_pool",
			"delete_state_pool_keys", "clear_state_pool", "save_state_pool",
			"load_state_pool", "stop_state_pool", "remove_state_pool":
			if err := b.handleStatePoolMessage(&msg, conn); err != nil {
				response := WSMessage{
					Type: "error",
					ID:   msg.ID,
					Payload: map[string]any{
						"error": err.Error(),
					},
				}
				conn.WriteJSON(response)
			}

		case "subscribe":
			// Subscribe to topic
			sub := b.bus.Subscribe(msg.Topic, msg.Target, func(payload map[string]any, sub Subscription) {
				response := WSMessage{
					Type:    "message",
					Topic:   msg.Topic,
					Target:  msg.Target,
					ID:      msg.ID,
					Payload: payload,
				}
				conn.WriteJSON(response)
			})

			if sub == nil {
				response := WSMessage{
					Type:   "error",
					ID:     msg.ID,
					Topic:  msg.Topic,
					Target: msg.Target,
					Payload: map[string]any{
						"error": "failed to subscribe",
					},
				}
				conn.WriteJSON(response)
				break
			}

			response := WSMessage{
				Type:  "subscribed",
				Topic: msg.Topic,
				ID:    msg.ID,
				Payload: map[string]any{
					"subID": msg.Target,
				},
			}
			conn.WriteJSON(response)

		case "unsubscribe":
			// Extract subscription ID from payload or use message ID
			subID := msg.ID
			if sid, ok := msg.Payload["subID"].(string); ok && sid != "" {
				subID = sid
			}

			// Get subscription and unsubscribe
			if sub := b.bus.Subscribe(msg.Topic, subID, nil); sub != nil {
				sub.Unsubscribe()
				response := WSMessage{
					Type:  "unsubscribed",
					Topic: msg.Topic,
					ID:    msg.ID,
					Payload: map[string]any{
						"subID": subID,
					},
				}
				conn.WriteJSON(response)
			} else {
				response := WSMessage{
					Type: "error",
					ID:   msg.ID,
					Payload: map[string]any{
						"error": "unsubscribe failed: subscription not found",
					},
				}
				conn.WriteJSON(response)
			}

		case "publish":
			// Handle publish
			success := b.bus.Publish(msg.Topic, msg.Payload, &PublishOptions{
				OnSuccess: func() {
					response := WSMessage{
						Type:  "published",
						Topic: msg.Topic,
						ID:    msg.ID,
					}
					conn.WriteJSON(response)
				},
				OnFailure: func(err error) {
					response := WSMessage{
						Type: "error",
						ID:   msg.ID,
						Payload: map[string]any{
							"error": err.Error(),
						},
					}
					conn.WriteJSON(response)
				},
			})
			if !success {
				b.debugLog("Failed to publish message to topic %s", msg.Topic)
				// Send error response if no subscribers
				response := WSMessage{
					Type: "error",
					ID:   msg.ID,
					Payload: map[string]any{
						"error": "no subscribers found",
					},
				}
				conn.WriteJSON(response)
			}

		case "publishTo":
			// Handle direct publish
			success := b.bus.PublishTo(msg.Topic, msg.Target, msg.Payload, &PublishOptions{
				OnSuccess: func() {
					response := WSMessage{
						Type:   "published",
						Topic:  msg.Topic,
						ID:     msg.ID,
						Target: msg.Target,
					}
					conn.WriteJSON(response)
				},
				OnFailure: func(err error) {
					response := WSMessage{
						Type: "error",
						ID:   msg.ID,
						Payload: map[string]any{
							"error": err.Error(),
						},
					}
					conn.WriteJSON(response)
				},
			})
			if !success {
				b.debugLog("Failed to publish direct message to topic %s, target %s", msg.Topic, msg.Target)
			}

		case "publishWithRetry":
			// Handle publish with retry
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

			success := b.bus.PublishWithRetry(msg.Topic, dmap, &cfg, &PublishOptions{
				OnSuccess: func() {
					response := WSMessage{
						Type:   "published",
						Topic:  msg.Topic,
						Target: msg.Target,
						ID:     msg.ID,
					}
					conn.WriteJSON(response)
				},
				OnFailure: func(err error) {
					response := WSMessage{
						Type:   "error",
						ID:     msg.ID,
						Topic:  msg.Topic,
						Target: msg.Target,
						Payload: map[string]any{
							"error": err.Error(),
						},
					}
					conn.WriteJSON(response)
				},
			})
			if !success {
				b.debugLog("Failed to publish message with retry to topic %s", msg.Topic)
			}

		case "publishToWithRetry":
			// Handle publishTo with retry
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

			if msg.Target == "" {
				response := WSMessage{
					Type:   "error",
					ID:     msg.ID,
					Topic:  msg.Topic,
					Target: msg.Target,
					Payload: map[string]any{
						"error": "target field is required for publishTo",
					},
				}
				conn.WriteJSON(response)
				break
			}
			dmap, ok := msg.Payload["data"].(map[string]any)
			if !ok {
				dmap = map[string]any{
					"data": msg.Payload["data"],
				}
			}
			success := b.bus.PublishToWithRetry(msg.Topic, msg.Target, dmap, &cfg, &PublishOptions{
				OnSuccess: func() {
					response := WSMessage{
						Type:   "published",
						Topic:  msg.Topic,
						Target: msg.Target,
						ID:     msg.ID,
					}
					conn.WriteJSON(response)
				},
				OnFailure: func(err error) {
					response := WSMessage{
						Type:   "error",
						ID:     msg.ID,
						Topic:  msg.Topic,
						Target: msg.Target,
						Payload: map[string]any{
							"error": err.Error(),
						},
					}
					conn.WriteJSON(response)
				},
			})
			if !success {
				b.debugLog("Failed to publish direct message with retry to topic %s, target %s", msg.Topic, msg.Target)
			}

		case "send_to_server":
			// Handle sending message to another server
			serverAddr, ok := msg.Payload["server_addr"].(string)
			if !ok {
				response := WSMessage{
					Type: "error",
					ID:   msg.ID,
					Payload: map[string]any{
						"error": "missing or invalid server_addr",
					},
				}
				conn.WriteJSON(response)
				b.debugLog("Invalid server_addr in send_to_server message")
				continue
			}

			data, ok := msg.Payload["data"].(map[string]any)
			if !ok {
				response := WSMessage{
					Type: "error",
					ID:   msg.ID,
					Payload: map[string]any{
						"error": "missing or invalid data",
					},
				}
				conn.WriteJSON(response)
				b.debugLog("Invalid data in send_to_server message")
				continue
			}

			// Get custom path if provided
			var path []string
			if customPath, ok := msg.Payload["path"].(string); ok {
				path = []string{customPath}
			}

			// Send message to other server
			success := b.PublishToServer(serverAddr, data, &PublishOptions{
				OnSuccess: func() {
					response := WSMessage{
						Type: "published",
						ID:   msg.ID,
						Payload: map[string]any{
							"server_addr": serverAddr,
						},
					}
					conn.WriteJSON(response)
				},
				OnFailure: func(err error) {
					response := WSMessage{
						Type: "error",
						ID:   msg.ID,
						Payload: map[string]any{
							"error":       err.Error(),
							"server_addr": serverAddr,
						},
					}
					conn.WriteJSON(response)
				},
			}, path...)

			if !success {
				b.debugLog("Failed to send message to server %s", serverAddr)
			}

		case "publishToServer":
			// Handle incoming server message
			if data, ok := msg.Payload["data"].(map[string]any); ok {
				if b.onServerData != nil {
					b.onServerData(data)
				}
				// Send acknowledgment back
				response := WSMessage{
					Type: "published",
					ID:   msg.ID,
				}
				if err := conn.WriteJSON(response); err != nil {
					b.debugLog("Failed to send server message acknowledgment: %v", err)
				}
			} else {
				response := WSMessage{
					Type: "error",
					ID:   msg.ID,
					Payload: map[string]any{
						"error": "invalid data format in server message",
					},
				}
				conn.WriteJSON(response)
			}

		case "has_subscribers":
			// Check if topic has subscribers
			hasSubscribers := b.bus.HasSubscribers(msg.Topic)
			response := WSMessage{
				Type:  "subscribers_status",
				Topic: msg.Topic,
				ID:    msg.ID,
				Payload: map[string]any{
					"has_subscribers": hasSubscribers,
				},
			}
			if err := conn.WriteJSON(response); err != nil {
				b.debugLog("Failed to send subscribers status: %v", err)
			}

		default:
			b.debugLog("Unknown message type from client %p: %s", conn, msg.Type)
		}
	}
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

func (b *BusServer) Bus() *PubSub {
	return b.bus
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

// PublishToServer sends a message to another BusServer
func (b *BusServer) PublishToServer(serverAddr string, msg map[string]any, opts *PublishOptions, path ...string) bool {
	b.debugLog("=== PUBLISH TO SERVER START ===")
	b.debugLog("Publishing to server %s", serverAddr)

	// Get or create client connection to target server
	client, exists := b.busServersConns.Get(serverAddr)
	if !exists {
		var err error
		client, err = NewClient(ClientConfig{
			Address:       serverAddr,
			ClientID:      "server-" + serverAddr,
			AutoReconnect: true,
			Path: func() string {
				if len(path) > 0 {
					return path[0]
				}
				return b.path
			}(),
		})
		if err != nil {
			b.debugLog("Failed to create client connection: %v", err)
			if opts != nil && opts.OnFailure != nil {
				opts.OnFailure(fmt.Errorf("failed to connect to server %s: %w", serverAddr, err))
			}
			return false
		}
		b.busServersConns.Set(serverAddr, client)
	}

	// Send message to target server
	success := client.PublishToServer(serverAddr, msg, opts)
	if !success {
		b.debugLog("Failed to publish message to server %s", serverAddr)
		return false
	}

	b.debugLog("Successfully published message to server %s", serverAddr)
	b.debugLog("=== PUBLISH TO SERVER END ===")
	return true
}

// CloseServerConnections closes all server-to-server connections
func (b *BusServer) CloseServerConnections() {
	if b.busServersConns != nil {
		b.busServersConns.Range(func(_ string, client *Client) bool {
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
	b.onServerData = fn
}

// WithDebug enables or disables debug logging
func (b *BusServer) WithDebug(enabled bool) *BusServer {
	b.debug = enabled
	b.bus.debug = enabled
	return b
}

// Update the LowAllocPubSub interface to use SafeConn
func (b *BusServer) CleanupConnection(conn *SafeConn) {
	b.bus.CleanupConnection(conn.conn)
}
