# kactor

A modern, high-performance distributed computing framework that combines:
- Actor-based concurrency with automatic work distribution
- Zero-allocation pub/sub messaging with nanosecond latencies
- Built-in state management and persistence
- Cross-platform SDKs for Go, Python, JavaScript, and Dart
- Production-ready features like automatic retry, reconnection, and error handling

[![Go Report Card](https://goreportcard.com/badge/github.com/kamalshkeir/kactor)](https://goreportcard.com/report/github.com/kamalshkeir/kactor)
[![GoDoc](https://godoc.org/github.com/kamalshkeir/kactor?status.svg)](https://godoc.org/github.com/kamalshkeir/kactor)

## Power of kactor: Real-World Example

Here's how kactor elegantly handles a complex multiplayer game server with real-time physics, AI, and state management:

```python
# Game Server Architecture using kactor

# 1. Create specialized actor pools for concurrent processing
await client.create_actor_pool("game-rooms", size=4)      # Handles game rooms
await client.create_actor_pool("physics", size=8)         # Physics calculations
await client.create_actor_pool("ai", size=2)              # AI processing

# 2. Distributed state management
await client.create_state_pool(StatePoolConfig(
    name="game-state",
    size=4,
    initial={
        "rooms": {},
        "players": {},
        "world_state": {}
    },
    state_size_mb=64
))

# 3. Pattern-based message routing with zero-allocation pub/sub
# Route player movements to physics actors
await client.subscribe("player.move", "physics-sub", 
    lambda msg, _: client.send_to_actor_pool("physics", {
        "type": "movement",
        "data": msg
    }))

# Pattern matching for room events (room.* matches room.join, room.leave, etc.)
await client.subscribe("room.*", "room-sub",
    lambda msg, sub: client.send_to_actor_pool("game-rooms", {
        "type": "room_event",
        "topic": sub.get_topic(),
        "data": msg
    }))

# AI behavior processing
await client.subscribe("npc.behavior", "ai-sub",
    lambda msg, _: client.send_to_actor_pool("ai", {
        "type": "behavior_update",
        "data": msg
    }))

# 4. Real-time state updates and broadcasts
client.on_actor_pool_message("physics", async def handle_physics(msg):
    # Process physics
    result = process_physics(msg["data"])
    # Atomic state updates
    await client.update_state_pool("game-state", {
        f"players.{msg['data']['player_id']}.position": result["new_position"]
    })
    # Broadcast updates to room participants
    await client.publish(f"room.{msg['data']['room_id']}.update", result))
```

This example demonstrates kactor's key strengths:
- **Concurrent Processing**: Dedicated actor pools for different game systems
- **Pattern Matching**: Intelligent message routing using wildcards (e.g., `room.*`)
- **State Management**: Distributed state with atomic updates
- **Real-time Performance**: Zero-allocation messaging with < 200ns latency
- **Scalability**: Independent scaling of physics, AI, and room management
- **Clean Architecture**: Clear separation of concerns with topic-based routing

## Features

- 🚀 **High Performance**: Zero-allocation pub/sub with nanosecond latencies
  - Single publisher to single subscriber: ~55ns/op
  - Direct message delivery: ~54ns/op
  - Scales efficiently with multiple publishers and subscribers
  
- 🌐 **Cross-Platform Client SDKs**:
  - Go (native implementation)
  - Python (async/await support)
  - JavaScript/TypeScript
  - Dart/Flutter
  
- 🎭 **Actor System**:
  - CPU-optimized actor pools
  - Automatic work distribution
  - Configurable batch processing
  - Message queue per worker

- 📦 **State Management**:
  - Distributed state pools
  - Configurable state size limits
  - Atomic updates
  - State persistence and recovery

- 🔄 **Advanced Messaging**:
  - Pub/Sub with topic-based routing
  - Direct messaging between clients
  - Message retry with configurable backoff
  - Automatic reconnection handling

- 🛡️ **Reliability Features**:
  - Configurable retry policies
  - Automatic reconnection
  - Message delivery guarantees
  - Error handling with callbacks

## Installation

### Go Server
```bash
go get github.com/kamalshkeir/kactor
```

## Quick Start

### Go Server
```go
package main

import "github.com/kamalshkeir/kactor"

func main() {
    server := kactor.NewBusServer(kactor.Config{
        Address: ":9313",
    })
    server.Run()
    //server.RunTLS()
    //server.RunAutoTLS()
}
```

### Python Client
```python
from kactor import Kactor, KactorConfig

client = Kactor(KactorConfig(
    address="localhost:9313",
    client_id="python-client"
))

# Create an actor pool
await client.create_actor_pool("my-actors", size=4)

# Subscribe to messages
await client.subscribe("my-topic", "sub1", lambda msg, sub: print(f"Received message on {sub.get_topic()}: {msg}"))

# Publish with retry
await client.publish_with_retry("my-topic", {"data": "Hello!"}, RetryConfig(
    max_attempts=3,
    max_backoff=5
))
```

### JavaScript/TypeScript Client
```javascript
import { Kactor, StatePoolConfig } from 'kactor';

const client = new Kactor({
    address: 'localhost:9313',
    clientId: 'js-client'
});

// Create a state pool
await client.createStatePool(new StatePoolConfig({
    name: 'my-state',
    size: 2,
    initial: { counter: 0 },
    state_size_mb: 32
}));

// Update state
await client.updateStatePool('my-state', { counter: 42 });

// Get state
const state = await client.getState('my-state');
console.log(state); // { counter: 42 }
```

### Dart/Flutter Client
```dart
import 'package:kactor/kactor.dart';

final client = Kactor(
    address: 'localhost:9313',
    clientId: 'dart-client',
    autoReconnect: true
);

// Subscribe to topic
final subscription = await client.subscribe(
    'my-topic',
    'sub1',
    (message, info) => print('Received: $message')
);

// Publish message
await client.publish(
    'my-topic',
    {'message': 'Hello from Dart!'},
    PublishOptions(
        onSuccess: () => print('Published successfully'),
        onFailure: (e) => print('Publish failed: $e')
    )
);
```

## Performance

Benchmarks on Intel(R) Core(TM) i5-7300HQ CPU @ 2.50GHz:

| Scenario | Operations/sec | Latency | Allocations |
|----------|---------------|---------|-------------|
| Single Publisher → Single Subscriber | 21.5M | 55.82 ns | 0 allocs |
| Single Publisher → Multiple Subscribers (32) | 3.8M | 312.5 ns | 0 allocs |
| Multiple Publishers → Single Subscriber | 11.4M | 111.8 ns | 0 allocs |
| Multiple Publishers → Multiple Subscribers | 2.0M | 729.5 ns | 0 allocs |
| Direct Message | 22.1M | 54.12 ns | 0 allocs |

### Retry Performance
| Configuration | Operations/sec | Latency |
|--------------|---------------|---------|
| Default Retry | 6.9M | 165.8 ns |
| Aggressive Retry | 6.3M | 178.6 ns |
| Light Retry | 6.6M | 162.3 ns |

## Advanced Features

### Actor Pools
```go
// Create an actor pool with custom handler
pool := kactor.NewActor(1<<21, 8192, func(msgs []Message) {
    for _, msg := range msgs {
        // Process messages in batches
    }
})
```

### State Management
```python
# Create state pool with configuration
config = StatePoolConfig(
    name="user-states",
    size=4,
    initial={"users": {}},
    state_size_mb=64
)
await client.create_state_pool(config)

# Update state atomically
await client.update_state_pool("user-states", {
    "users": {"user1": {"status": "online"}}
})
```

### Retry Policies
```javascript
const retryConfig = {
    maxAttempts: 3,
    maxBackoff: 5  // seconds
};

await client.publishWithRetry("critical-topic", payload, retryConfig, {
    onSuccess: () => console.log("Message delivered"),
    onFailure: (err) => console.error("Failed after retries:", err)
});
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 