package kactor

import (
	"sync"

	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/ksmux/ws"
)

// PUBSUB
type PubSub struct {
	subscribers *kmap.SafeMap[string, *psSubList]
	patterns    *kmap.SafeMap[string, *psSubList]
	pendingMsgs *kmap.SafeMap[string, *PublishOptions]

	msgPool     sync.Pool
	payloadPool sync.Pool
	idPool      sync.Pool
	// Object pools
	subsPool  sync.Pool
	bufPool   sync.Pool
	userCount int32
	closed    int32
	debug     bool
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
	subs  [32]*psSubNode
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
	options  *PublishOptions
	pending  int32
	msgID    []byte
	idBuf    []byte
}

// PoolMessage represents a message sent to an actor pool
type PoolMessage struct {
	ClientID string
	Data     any
}

// Implement Message interface for PoolMessage
func (m PoolMessage) GetData() any {
	return m.Data
}
