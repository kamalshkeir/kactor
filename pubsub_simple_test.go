package kactor

import (
	"testing"
	"time"
)

func TestSimplePubSub(t *testing.T) {
	ps := NewPubSub()
	defer ps.Close()

	// Test basic pub/sub
	received := make(chan map[string]any, 1)
	sub := ps.Subscribe("test", "client1", func(msg map[string]any, s Subscription) {
		received <- msg
	})

	if sub == nil {
		t.Fatal("Failed to subscribe")
	}

	testMsg := map[string]any{"data": "test"}
	if !ps.Publish("test", testMsg, nil) {
		t.Fatal("Failed to publish")
	}

	select {
	case msg := <-received:
		if msg["data"] != "test" {
			t.Errorf("Got wrong message: %v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Test direct message
	if !ps.PublishTo("test", "client1", testMsg, nil) {
		t.Fatal("Failed to publish direct message")
	}

	select {
	case msg := <-received:
		if msg["data"] != "test" {
			t.Errorf("Got wrong direct message: %v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for direct message")
	}

	// Cleanup
	sub.Unsubscribe()
}
