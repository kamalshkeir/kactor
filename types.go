package kactor

import (
	"sync"

	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/ksmux/ws"
)

// PUBSUB
// PubSub implements a minimal allocation pubsub system
type PubSub struct {
	subscribers kmap.SafeMap[string, *psSubList]
	patterns    kmap.SafeMap[string, *psSubList]
	userCount   int32
	msgPool     sync.Pool
	payloadPool sync.Pool
	idPool      sync.Pool
	closed      int32
	debug       bool
	// Pre-allocated buffers for common topics
	topicBufs   [32][]byte
	topicLen    [32]int32
	pendingMsgs kmap.SafeMap[string, *PublishOptions]

	// Object pools
	subsPool sync.Pool
	bufPool  sync.Pool

	// Configuration
	config struct {
		maxSubscribersPerTopic int32
		maxTopicLength         int32
		maxPayloadSize         int32
	}
}

type WSMessage struct {
	Type    string         `json:"type"`              // "subscribe", "unsubscribe", "publish", "publishTo", "ack"
	Topic   string         `json:"topic"`             // Topic name
	ID      string         `json:"id,omitempty"`      // Client/Subscription ID
	Target  string         `json:"target,omitempty"`  // Target for direct messages
	Payload map[string]any `json:"payload,omitempty"` // Message payload
	MsgID   string         `json:"msg_id,omitempty"`  // Unique message ID for acknowledgment
}

type RetryConfig struct {
	MaxAttempts int
	MaxBackoff  int // maximum number of yields (runtime.Gosched()) in backoff
}

var DefaultRetryConfig = RetryConfig{
	MaxAttempts: 3,
	MaxBackoff:  4,
}

// PublishOptions configures the behavior of Publish operations
type PublishOptions struct {
	// OnSuccess is called when the message is successfully delivered
	OnSuccess func()
	// OnFailure is called when the message fails to be delivered
	OnFailure func(error)
}

type Subscription interface {
	Unsubscribe()
}

// psSubList is a fixed-size array of subscribers for a topic
type psSubList struct {
	mu    sync.RWMutex
	subs  [32]*psSubNode // Fixed size array for zero allocation
	count int32
}

// psSubNode is a node in the subscriber list
type psSubNode struct {
	handler func(map[string]any, Subscription)
	subID   string
	sub     *psSubscription
	conn    *ws.Conn // Reference to the WebSocket connection if this is a WebSocket subscription
}

// psSubscription represents a subscription
type psSubscription struct {
	topic  string
	subID  string
	pubsub *PubSub
}

// psMessage represents a message in the system
type psMessage struct {
	topic    string
	targetID string
	payload  map[string]any
	acks     *kmap.SafeMap[string, bool]
	options  *PublishOptions
	pending  int32
	msgID    []byte
	idBuf    []byte
}
