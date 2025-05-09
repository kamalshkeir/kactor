package kactor

import (
	"errors"
	"fmt"

	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/ksmux/ws"
)

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
func (b *BusServer) handleStatePoolMessage(msg *WSMessage, conn *ws.Conn) error {
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

func (b *BusServer) handleActorPoolCreation(msg *WSMessage, conn *ws.Conn) error {
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

func (b *BusServer) handleActorPoolMessage(msg *WSMessage, conn *ws.Conn) error {
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

func (b *BusServer) handleStatePoolCreation(msg *WSMessage, conn *ws.Conn) error {
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

func (b *BusServer) StatefulPools() *kmap.SafeMap[string, *StatefulActorPool] {
	return b.pools
}
