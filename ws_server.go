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
	connections *kmap.SafeMap[*ws.Conn, string]
	// Batch processing channels
	batchSize int
	// Add write buffer pool
	writeBufferPool sync.Pool
	// Add JSON encoder pool
	encoderPool sync.Pool
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
		connections:  kmap.New[*ws.Conn, string](),
		batchSize:    100,
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

func (b *BusServer) WithCustomPath(path string) *BusServer {
	b.path = path
	return b
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

func (b *BusServer) App() *ksmux.Router {
	return b.app
}
func (b *BusServer) Path() string {
	return b.path
}
func (b *BusServer) Router() *ksmux.Router {
	return b.app
}
func (b *BusServer) Connections() *kmap.SafeMap[*ws.Conn, string] {
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

// Update the LowAllocPubSub interface to use wspool.Conn
func (b *BusServer) CleanupConnection(conn *ws.Conn) {
	b.connections.Delete(conn)
	b.bus.CleanupConnection(conn)
}

func (b *BusServer) Run() {
	// Load states if directory is configured
	b.app.Get(b.path, b.handleWS)
	b.app.Run()
}

func (b *BusServer) RunTLS() {
	b.app.Get(b.path, b.handleWS)
	b.app.RunTLS()
}

func (b *BusServer) RunAutoTLS() {
	b.app.Get(b.path, b.handleWS)
	b.app.RunAutoTLS()
}

func (s *BusServer) WithPprof(path ...string) {
	s.App().WithPprof(path...)
}

func (s *BusServer) WithMetrics(httpHandler http.Handler, path ...string) {
	s.App().WithMetrics(httpHandler, path...)
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

// debugLog prints debug messages if debug mode is enabled
func (b *BusServer) debugLog(format string, args ...interface{}) {
	if b.debug {
		fmt.Printf("[SERVER DEBUG] "+format+"\n", args...)
	}
}

func (b *BusServer) sendErrorWS(err string, req *WSMessage, conn *ws.Conn) {
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
func (b *BusServer) sendAckWS(acktype string, req *WSMessage, conn *ws.Conn, payload ...map[string]any) {
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

func (b *BusServer) handleWS(c *ksmux.Context) {
	conn, err := ws.DefaultUpgraderKSMUX.Upgrade(c.ResponseWriter, c.Request, nil)
	if err != nil {
		if b.debug {
			b.debugLog("[HandleWS] WebSocket upgrade failed: %v", err)
		}
		c.Error(ErrUpgradeWs)
		return
	}
	defer conn.Close()

	// Handle incoming messages
	for {
		var msg *WSMessage
		err := conn.ReadJSON(&msg)
		if err != nil {
			b.CleanupConnection(conn)
			return
		}
		// Store client ID for cleanup and connection tracking
		if msg.ID != "" {
			b.connections.Set(conn, msg.ID)
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
			sub := b.bus.SubscribeWS(msg.Topic, msg.Target, func(payload map[string]any, sub Subscription) {
				b.sendAckWS("message", msg, conn, payload)
			}, conn)

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
