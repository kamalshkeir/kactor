package kactor

import (
	"fmt"
	"sync/atomic"
)

type ActorPool struct {
	actors  []*Actor
	size    int
	counter uint64 // for round-robin distribution
	name    string
}

func NewActorPool(name string, size int, handler func([]Message)) (*ActorPool, error) {
	if size <= 0 {
		return nil, fmt.Errorf("pool size must be greater than 0")
	}

	pool := &ActorPool{
		actors: make([]*Actor, size),
		size:   size,
		name:   name,
	}

	// Create actors in the pool
	for i := 0; i < size; i++ {
		actor := NewActor(0, 0, handler)
		pool.actors[i] = actor
		actor.Start()
	}

	return pool, nil
}

// Send distributes messages across the pool using round-robin
func (p *ActorPool) Send(msg Message) bool {
	if len(p.actors) == 0 {
		return false
	}

	// Get next actor using round-robin
	workerIndex := atomic.AddUint64(&p.counter, 1) % uint64(p.size)
	return p.actors[workerIndex].Send(msg, 0)
}

// SendBatch sends a batch of messages, distributing them across the pool
func (p *ActorPool) SendBatch(msgs []Message) bool {
	if len(p.actors) == 0 {
		return false
	}

	success := true
	for _, msg := range msgs {
		if !p.Send(msg) {
			success = false
		}
	}
	return success
}

// Stop stops all actors in the pool
func (p *ActorPool) Stop() {
	for _, actor := range p.actors {
		actor.Stop()
	}
}

// Size returns the number of actors in the pool
func (p *ActorPool) Size() int {
	return p.size
}

// Name returns the pool's name
func (p *ActorPool) Name() string {
	return p.name
}
