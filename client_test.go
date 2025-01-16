package kactor

import (
	"sync"
	"testing"
	"time"

	"github.com/kamalshkeir/ksmux"
)

func TestClientPubSub(t *testing.T) {
	// Start server
	server := NewBusServer(ksmux.Config{
		Address: ":9316",
	})
	go server.Run()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(300 * time.Millisecond)

	// Create two clients for testing pub/sub
	client1, err := NewClient(ClientConfig{
		Address:  "localhost:9316",
		Path:     "/ws/kactor",
		ClientID: "test-client-1",
	})
	if err != nil {
		t.Fatalf("Failed to create client1: %v", err)
	}
	defer client1.Close()

	client2, err := NewClient(ClientConfig{
		Address:  "localhost:9316",
		Path:     "/ws/kactor",
		ClientID: "test-client-2",
	})
	if err != nil {
		t.Fatalf("Failed to create client2: %v", err)
	}
	defer client2.Close()

	// Wait for connections to be fully established
	time.Sleep(500 * time.Millisecond)

	// Test 1: Basic Subscribe and Publish
	messageReceived := make(chan bool)
	subscription := client1.Subscribe("test-topic", "", func(msg map[string]any, sub Subscription) {
		if data, ok := msg["data"].(string); ok && data == "Hello, World!" {
			messageReceived <- true
		}
	})
	if subscription == nil {
		t.Fatal("Failed to subscribe to topic")
	}

	// Wait for subscription to be established
	time.Sleep(500 * time.Millisecond)

	// Verify HasSubscribers
	if !client2.HasSubscribers("test-topic") {
		t.Error("Expected HasSubscribers to return true")
	}

	// Publish message
	success := client2.Publish("test-topic", map[string]any{
		"data": "Hello, World!",
	}, nil)
	if !success {
		t.Error("Failed to publish message")
	}

	// Wait for message
	select {
	case <-messageReceived:
		// Message received successfully
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for published message")
	}

	// Test 2.1: PublishTo specific client using clienID
	directMessageReceived := make(chan bool)
	client2.Subscribe("direct-topic", "", func(msg map[string]any, sub Subscription) {
		if data, ok := msg["data"].(string); ok && data == "Direct message using clientID" {
			directMessageReceived <- true
		}
	})

	time.Sleep(500 * time.Millisecond)

	success = client1.PublishTo("direct-topic", client2.config.ClientID, map[string]any{
		"data": "Direct message using clientID",
	}, nil)
	if !success {
		t.Error("Failed to publish direct message using clientID")
	}

	select {
	case <-directMessageReceived:
		// Direct message received successfully
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for direct message using clientID")
	}

	// Test 2.2: PublishTo specific client using subID
	directMessageReceived2 := make(chan bool)
	client2.Subscribe("direct-topic2", "sub2", func(msg map[string]any, sub Subscription) {
		if data, ok := msg["data"].(string); ok && data == "Direct message using subID" {
			directMessageReceived2 <- true
		}
	})

	time.Sleep(500 * time.Millisecond)

	success = client1.PublishTo("direct-topic2", "sub2", map[string]any{
		"data": "Direct message using subID",
	}, &PublishOptions{
		OnSuccess: func() {
			t.Log("successssssssssssssssssssssssss")
		},
		OnFailure: func(err error) {
			t.Error("FAILLLLLLLLED", err)
		},
	})
	if !success {
		t.Error("Failed to publish direct message using subID")
	}

	select {
	case <-directMessageReceived2:
		// Direct message received successfully
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for direct message using subID")
	}

	// Test 3: PublishWithRetry
	retryMessageReceived := make(chan bool)
	client2.Subscribe("retry-topic", "", func(msg map[string]any, sub Subscription) {
		if data, ok := msg["data"].(string); ok && data == "Retry message" {
			retryMessageReceived <- true
		}
	})

	time.Sleep(500 * time.Millisecond)

	success = client1.PublishWithRetry("retry-topic", map[string]any{
		"data": "Retry message",
	}, &RetryConfig{
		MaxAttempts: 3,
		MaxBackoff:  4,
	}, nil)
	if !success {
		t.Error("Failed to publish with retry")
	}

	select {
	case <-retryMessageReceived:
		// Retry message received successfully
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for retry message")
	}

	// Test 4: PublishToWithRetry
	retryDirectReceived := make(chan bool)
	client2.Subscribe("retry-direct-topic", "", func(msg map[string]any, sub Subscription) {
		if data, ok := msg["data"].(string); ok && data == "Retry direct message" {
			retryDirectReceived <- true
		}
	})

	time.Sleep(500 * time.Millisecond)

	success = client1.PublishToWithRetry("retry-direct-topic", "test-client-2", map[string]any{
		"data": "Retry direct message",
	}, &RetryConfig{
		MaxAttempts: 3,
		MaxBackoff:  4,
	}, nil)
	if !success {
		t.Error("Failed to publish direct message with retry")
	}

	select {
	case <-retryDirectReceived:
		// Retry direct message received successfully
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for retry direct message")
	}

	// Test 5: Publish with callbacks
	callbackReceived := make(chan error, 1)
	client1.Publish("callback-topic", map[string]any{
		"data": "Callback message",
	}, &PublishOptions{
		OnSuccess: func() {
			callbackReceived <- nil
		},
		OnFailure: func(err error) {
			callbackReceived <- err
		},
	})

	select {
	case err := <-callbackReceived:
		if err != nil {
			// This is expected since there are no subscribers
			if err.Error() != "no subscribers found for topic: callback-topic" {
				t.Errorf("Unexpected error: %v", err)
			}
		} else {
			t.Error("Expected failure but got success")
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for publish callback")
	}

	// Test 6: Unsubscribe
	subscription.Unsubscribe()
	time.Sleep(500 * time.Millisecond)

	if client2.HasSubscribers("test-topic") {
		t.Error("Expected HasSubscribers to return false after unsubscribe")
	}

	// Test 7: Multiple subscribers
	var multipleReceivers sync.WaitGroup
	multipleReceivers.Add(2)

	sub1 := client1.Subscribe("multi-topic", "", func(msg map[string]any, sub Subscription) {
		if data, ok := msg["data"].(string); ok && data == "Broadcast" {
			multipleReceivers.Done()
		}
	})
	if sub1 == nil {
		t.Fatal("Failed to create first subscription for broadcast test")
	}
	defer sub1.Unsubscribe()

	sub2 := client2.Subscribe("multi-topic", "", func(msg map[string]any, sub Subscription) {
		if data, ok := msg["data"].(string); ok && data == "Broadcast" {
			multipleReceivers.Done()
		}
	})
	if sub2 == nil {
		t.Fatal("Failed to create second subscription for broadcast test")
	}
	defer sub2.Unsubscribe()

	time.Sleep(500 * time.Millisecond)

	success = client1.Publish("multi-topic", map[string]any{
		"data": "Broadcast",
	}, nil)
	if !success {
		t.Error("Failed to publish broadcast message")
	}

	// Wait for both subscribers to receive the message
	broadcastDone := make(chan struct{})
	go func() {
		multipleReceivers.Wait()
		close(broadcastDone)
	}()

	select {
	case <-broadcastDone:
		// Both subscribers received the message
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for multiple subscribers")
	}

	// Give some time for the server to process
	time.Sleep(500 * time.Millisecond)
}

func TestStatePoolBasics(t *testing.T) {
	// Start server
	server := NewBusServer(ksmux.Config{
		Address: ":9313",
	})
	go server.Run()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create client config
	config := ClientConfig{
		Address:  "localhost:9313",
		Path:     "/ws/kactor",
		ClientID: "test-client",
	}

	// Create client
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Wait for connection
	time.Sleep(100 * time.Millisecond)

	// Test creating a state pool
	err = client.CreateStatePool(StatefulPoolConfig{
		Name:        "test-pool",
		Size:        2,
		StateSizeMB: 32,
		Initial:     map[string]any{"counter": 0},
	})
	if err != nil {
		t.Fatalf("Failed to create state pool: %v", err)
	}

	// Test sending a message to the pool
	err = client.SendToStatePool("test-pool", map[string]any{
		"action": "increment",
		"value":  1,
	})
	if err != nil {
		t.Fatalf("Failed to send message to pool: %v", err)
	}

	// Test getting pool state
	state, err := client.GetState("test-pool")
	if err != nil {
		t.Fatalf("Failed to get pool state: %v", err)
	}

	// Test updating pool state
	err = client.UpdateStatePool("test-pool", map[string]any{
		"counter": 42,
		"users":   []string{"user1", "user2"},
	})
	if err != nil {
		t.Fatalf("Failed to update pool state: %v", err)
	}

	// Test getting updated state
	state, err = client.GetState("test-pool")
	if err != nil {
		t.Fatalf("Failed to get updated pool state: %v", err)
	}

	// Verify state was updated correctly
	if counter, ok := state["counter"].(float64); !ok || counter != 42 {
		t.Errorf("Expected counter to be 42, got %v", state["counter"])
	}
	if users, ok := state["users"].([]any); !ok || len(users) != 2 {
		t.Errorf("Expected users to be [user1, user2], got %v", state["users"])
	}

	// Test message handler
	messageReceived := make(chan bool)
	client.OnStatePoolMessage("test-pool", func(data any) {
		messageReceived <- true
	})

	// Send a test message
	err = client.SendToStatePool("test-pool", map[string]any{
		"action": "test",
		"data":   "test message",
	})
	if err != nil {
		t.Fatalf("Failed to send test message: %v", err)
	}

	// Wait for message to be received
	select {
	case <-messageReceived:
		// Message handler triggered successfully
	case <-time.After(time.Second):
		t.Error("Timeout waiting for message handler")
	}

	// Test deleting specific keys
	err = client.DeleteStatePoolKeys("test-pool", []string{"users"})
	if err != nil {
		t.Fatalf("Failed to delete state keys: %v", err)
	}

	// Verify key was deleted
	state, err = client.GetState("test-pool")
	if err != nil {
		t.Fatalf("Failed to get state after deletion: %v", err)
	}
	if _, exists := state["users"]; exists {
		t.Error("Expected users key to be deleted")
	}

	// Test clearing state
	err = client.ClearStatePool("test-pool")
	if err != nil {
		t.Fatalf("Failed to clear pool state: %v", err)
	}

	// Verify state was cleared
	state, err = client.GetState("test-pool")
	if err != nil {
		t.Fatalf("Failed to get state after clearing: %v", err)
	}
	if len(state) != 0 {
		t.Errorf("Expected empty state, got %v", state)
	}

	// Test saving and loading state
	err = client.UpdateStatePool("test-pool", map[string]any{
		"test_key": "test_value",
	})
	if err != nil {
		t.Fatalf("Failed to update state before save: %v", err)
	}

	err = client.SaveStatePool("test-pool", "test_state")
	if err != nil {
		t.Fatalf("Failed to save pool state: %v", err)
	}

	// Clear state before loading
	err = client.ClearStatePool("test-pool")
	if err != nil {
		t.Fatalf("Failed to clear state before load: %v", err)
	}

	err = client.LoadStatePool("test-pool", "test_state")
	if err != nil {
		t.Fatalf("Failed to load pool state: %v", err)
	}

	// Verify loaded state
	state, err = client.GetState("test-pool")
	if err != nil {
		t.Fatalf("Failed to get state after loading: %v", err)
	}
	if val, ok := state["test_key"].(string); !ok || val != "test_value" {
		t.Errorf("Expected test_key to be 'test_value', got %v", state["test_key"])
	}

	// Test removing message handler
	client.RemoveStatePoolHandler("test-pool")

	// Send another message to verify handler is removed
	err = client.SendToStatePool("test-pool", map[string]any{
		"action": "test",
		"data":   "should not trigger handler",
	})
	if err != nil {
		t.Fatalf("Failed to send test message: %v", err)
	}

	// Verify handler is not triggered
	select {
	case <-messageReceived:
		t.Error("Message handler was triggered after removal")
	case <-time.After(100 * time.Millisecond):
		// Message handler successfully removed
	}

	// Test stopping the pool
	err = client.StopStatePool("test-pool")
	if err != nil {
		t.Fatalf("Failed to stop pool: %v", err)
	}

	// Test removing the pool
	err = client.RemoveStatePool("test-pool")
	if err != nil {
		t.Fatalf("Failed to remove pool: %v", err)
	}

	// Give some time for the server to process
	time.Sleep(100 * time.Millisecond)
}

func TestStatePoolErrors(t *testing.T) {
	// Start server
	server := NewBusServer(ksmux.Config{
		Address: ":9319",
	})
	go server.Run()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create client
	client, err := NewClient(ClientConfig{
		Address:  "localhost:9319",
		Path:     "/ws/kactor",
		ClientID: "test-client",
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test 1: Try to get state from non-existent pool
	_, err = client.GetState("non-existent-pool")
	if err == nil {
		t.Error("Expected error when getting state from non-existent pool")
	}

	// Test 2: Try to create pool with invalid config
	err = client.CreateStatePool(StatefulPoolConfig{
		Name: "invalid-pool",
		Size: -1,
	})
	if err == nil {
		t.Error("Expected error when creating pool with invalid size")
	}

	// Test 3: Try to send message to non-existent pool
	err = client.SendToStatePool("non-existent-pool", map[string]any{
		"action": "test",
	})
	if err == nil {
		t.Error("Expected error when sending message to non-existent pool")
	}

	// Test 4: Try to update non-existent pool
	err = client.UpdateStatePool("non-existent-pool", map[string]any{
		"key": "value",
	})
	if err == nil {
		t.Error("Expected error when updating non-existent pool")
	}

	// Test 5: Try to delete keys from non-existent pool
	err = client.DeleteStatePoolKeys("non-existent-pool", []string{"key"})
	if err == nil {
		t.Error("Expected error when deleting keys from non-existent pool")
	}

	// Test 6: Try to clear non-existent pool
	err = client.ClearStatePool("non-existent-pool")
	if err == nil {
		t.Error("Expected error when clearing non-existent pool")
	}

	// Test 7: Try to save non-existent pool
	err = client.SaveStatePool("non-existent-pool", "test_state")
	if err == nil {
		t.Error("Expected error when saving non-existent pool")
	}

	// Test 8: Try to load non-existent pool
	err = client.LoadStatePool("non-existent-pool", "test_state")
	if err == nil {
		t.Error("Expected error when loading non-existent pool")
	}

	// Test 9: Try to stop non-existent pool
	err = client.StopStatePool("non-existent-pool")
	if err == nil {
		t.Error("Expected error when stopping non-existent pool")
	}

	// Test 10: Try to remove non-existent pool
	err = client.RemoveStatePool("non-existent-pool")
	if err == nil {
		t.Error("Expected error when removing non-existent pool")
	}

	// Test 11: Try to create pool with duplicate name
	err = client.CreateStatePool(StatefulPoolConfig{
		Name:        "test-pool",
		StateSizeMB: 32,
		Initial:     map[string]any{},
		Size:        2,
	})
	if err != nil {
		t.Fatalf("Failed to create first pool: %v", err)
	}

	// Try to create pool with same name
	err = client.CreateStatePool(StatefulPoolConfig{
		Name:        "test-pool",
		Size:        2,
		StateSizeMB: 32,
		Initial:     map[string]any{},
	})
	if err == nil {
		t.Error("Expected error when creating pool with duplicate name")
	}

	// Give some time for the server to process
	time.Sleep(100 * time.Millisecond)
}

func TestActorPoolBasics(t *testing.T) {
	// Start server
	server := NewBusServer(ksmux.Config{
		Address: ":9314",
	})
	go server.Run()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create client config
	config := ClientConfig{
		Address:  "localhost:9314",
		Path:     "/ws/kactor",
		ClientID: "test-client",
	}

	// Create client
	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Wait for connection
	time.Sleep(100 * time.Millisecond)

	// Test creating an actor pool
	err = client.CreateActorPool("test-actors", 2)
	if err != nil {
		t.Fatalf("Failed to create actor pool: %v", err)
	}

	// Test message handler
	messageReceived := make(chan bool)
	client.OnActorPoolMessage("test-actors", func(data any) {
		messageReceived <- true
	})

	// Send a test message
	err = client.SendToActorPool("test-actors", map[string]any{
		"action": "test",
		"data":   "test message",
	})
	if err != nil {
		t.Fatalf("Failed to send message to actor pool: %v", err)
	}

	// Wait for message to be received
	select {
	case <-messageReceived:
		// Message handler triggered successfully
	case <-time.After(time.Second):
		t.Error("Timeout waiting for message handler")
	}

	// Test removing message handler
	client.RemoveActorPoolHandler("test-actors")

	// Send another message to verify handler is removed
	err = client.SendToActorPool("test-actors", map[string]any{
		"action": "test",
		"data":   "should not trigger handler",
	})
	if err != nil {
		t.Fatalf("Failed to send test message: %v", err)
	}

	// Verify handler is not triggered
	select {
	case <-messageReceived:
		t.Error("Message handler was triggered after removal")
	case <-time.After(100 * time.Millisecond):
		// Message handler successfully removed
	}

	// Test stopping the pool
	err = client.StopActorPool("test-actors")
	if err != nil {
		t.Fatalf("Failed to stop actor pool: %v", err)
	}

	// Test removing the pool
	err = client.RemoveActorPool("test-actors")
	if err != nil {
		t.Fatalf("Failed to remove actor pool: %v", err)
	}

	// Give some time for the server to process
	time.Sleep(100 * time.Millisecond)
}

func TestActorPoolErrors(t *testing.T) {
	// Start server
	server := NewBusServer(ksmux.Config{
		Address: ":9315",
	})
	go server.Run()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create client
	client, err := NewClient(ClientConfig{
		Address:  "localhost:9315",
		Path:     "/ws/kactor",
		ClientID: "test-client",
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test 1: Try to create pool with invalid size
	err = client.CreateActorPool("invalid-pool", -1)
	if err == nil {
		t.Error("Expected error when creating pool with invalid size")
	}

	// Test 2: Try to send message to non-existent pool
	err = client.SendToActorPool("non-existent-pool", map[string]any{
		"action": "test",
	})
	if err == nil {
		t.Error("Expected error when sending message to non-existent pool")
	}

	// Test 3: Try to stop non-existent pool
	err = client.StopActorPool("non-existent-pool")
	if err == nil {
		t.Error("Expected error when stopping non-existent pool")
	}

	// Test 4: Try to remove non-existent pool
	err = client.RemoveActorPool("non-existent-pool")
	if err == nil {
		t.Error("Expected error when removing non-existent pool")
	}

	// Test 5: Try to create pool with duplicate name
	err = client.CreateActorPool("test-pool", 2)
	if err != nil {
		t.Fatalf("Failed to create first pool: %v", err)
	}

	// Try to create pool with same name
	err = client.CreateActorPool("test-pool", 2)
	if err == nil {
		t.Error("Expected error when creating pool with duplicate name")
	}

	// Give some time for the server to process
	time.Sleep(100 * time.Millisecond)
}
