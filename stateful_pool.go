package kactor

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/kamalshkeir/kmap"
)

func init() {
	// Register types for gob encoding
	gob.Register(map[string]interface{}{})
	gob.Register(map[string]bool{})
	gob.Register(map[string]any{})
	gob.Register(map[string]struct{}{})
}

// StateData represents the serializable state
type StateData struct {
	Data map[string]any
}

// StatefulHandler is the handler function type for stateful actor pools
type StatefulHandler func(msg Message, state *kmap.SafeMap[string, any]) error

// StatefulPoolConfig defines the configuration for a stateful actor pool
type StatefulPoolConfig struct {
	Size        int             // Number of workers in the pool
	Name        string          // Name used for persistence
	Initial     map[string]any  // Initial state values
	Handler     StatefulHandler // Message handler with state access
	StateSizeMB int             // Optional: Size limit for state in MB
}

// StatefulActorPool extends ActorPool with state management
type StatefulActorPool struct {
	acpool *ActorPool
	state  *kmap.SafeMap[string, any]
	mu     sync.RWMutex
}

// NewStatefulPool creates a new actor pool with state management
func NewStatefulPool(name string, config StatefulPoolConfig) (*StatefulActorPool, error) {
	if config.Size <= 0 {
		return nil, fmt.Errorf("pool size must be greater than 0")
	}

	// Create a new kmap for state with optional size limit
	var state *kmap.SafeMap[string, any]
	if config.StateSizeMB > 0 {
		state = kmap.New[string, any](config.StateSizeMB)
	} else {
		state = kmap.New[string, any]()
	}

	// Initialize state with provided values
	if config.Initial != nil {
		for k, v := range config.Initial {
			state.Set(k, v)
		}
	}

	// Wrap the stateful handler
	wrappedHandler := func(msgs []Message) {
		for _, msg := range msgs {
			if err := config.Handler(msg, state); err != nil {
				// Log error and continue
				log.Printf("error in stateful pool handler: %v", err)
			}
		}
	}

	pool, err := NewActorPool(name, config.Size, wrappedHandler)
	if err != nil {
		return nil, err
	}

	return &StatefulActorPool{
		acpool: pool,
		state:  state,
	}, nil
}

// SaveState persists the pool's state to a binary file using gob encoding
func (s *StatefulActorPool) SaveState(directory string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create state data structure
	stateData := StateData{
		Data: make(map[string]any),
	}

	// Get all state data
	s.state.Range(func(key string, value any) bool {
		stateData.Data[key] = value
		return true
	})

	// Ensure directory exists
	if err := os.MkdirAll(directory, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create file
	filename := filepath.Join(directory, s.acpool.Name()+".gob")
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create encoder and encode the data
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(stateData); err != nil {
		return fmt.Errorf("failed to encode state: %w", err)
	}

	return nil
}

// LoadState loads the pool's state from a binary file using gob encoding
func (s *StatefulActorPool) LoadState(directory string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	filename := filepath.Join(directory, s.acpool.Name()+".gob")
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var stateData StateData
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&stateData); err != nil {
		return fmt.Errorf("failed to decode state: %w", err)
	}

	// Clear current state
	s.state.Flush()

	// Update all state data
	for k, v := range stateData.Data {
		s.state.Set(k, v)
	}

	return nil
}

// State returns the underlying SafeMap for direct state access
func (s *StatefulActorPool) State() *kmap.SafeMap[string, any] {
	return s.state
}

// GetStateValue retrieves a value from the state
func (s *StatefulActorPool) GetStateValue(key string) (any, bool) {
	return s.state.Get(key)
}

// SetStateValue sets a value in the state
func (s *StatefulActorPool) SetStateValue(key string, value any) {
	s.state.Set(key, value)
}

// DeleteStateValue removes a value from the state
func (s *StatefulActorPool) DeleteStateValue(key string) {
	s.state.Delete(key)
}

// ClearState removes all values from the state
func (s *StatefulActorPool) ClearState() {
	s.state.Flush()
}

// Stop stop all actors
func (s *StatefulActorPool) Stop() {
	s.acpool.Stop()
}
