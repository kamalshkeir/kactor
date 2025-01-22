package kactor

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kamalshkeir/ksmux"
)

func BenchmarkWebSocketPubSub(b *testing.B) {
	// Create server
	server := NewBusServer(ksmux.Config{
		Address: "localhost:9888",
	})
	server.WithDebug(false)
	go server.Run()
	defer server.Stop()
	time.Sleep(time.Second)

	b.Run("Single_Publisher_Single_Subscriber", func(b *testing.B) {
		// Create client
		client, err := NewClient(ClientConfig{
			Address:       "localhost:9888",
			ClientID:      "bench-client-1",
			Path:          "/ws/kactor",
			AutoReconnect: true,
		})
		client.WithDebug(false)
		if err != nil {
			b.Fatal("Failed to create client:", err)
		}
		// defer client.Close()
		time.Sleep(1000 * time.Millisecond)

		//Subscribe
		var received int32
		sub := client.Subscribe("test", "sub1", func(payload map[string]any, s Subscription) {
			atomic.AddInt32(&received, 1)
		})
		if sub == nil {
			b.Fatal("Failed to subscribe")
		}
		defer sub.Unsubscribe()
		time.Sleep(300 * time.Millisecond)

		// Pre-allocate payload
		payload := map[string]any{"msg": "hello"}

		// Verify subscription works
		if !client.Publish("test", payload, nil) {
			b.Fatal("Failed to publish test message")
		}
		deadline := time.Now().Add(5 * time.Second)
		for atomic.LoadInt32(&received) == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if atomic.LoadInt32(&received) == 0 {
			b.Fatal("Failed to receive test message")
		}

		// Reset for benchmark
		atomic.StoreInt32(&received, 0)
		b.ResetTimer()
		b.ReportAllocs()

		// Run benchmark
		for i := 0; i < b.N; i++ {
			if !client.Publish("test", payload, nil) {
				b.Errorf("Failed to publish message %d", i)
				continue
			}
		}

		b.StopTimer()
		// Wait for remaining messages
		deadline = time.Now().Add(5 * time.Second)
		for atomic.LoadInt32(&received) < int32(b.N) && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}

		msgs := atomic.LoadInt32(&received)
		b.ReportMetric(float64(msgs), "msgs_received")
		if msgs < int32(b.N) {
			b.Errorf("Only received %d out of %d messages", msgs, b.N)
		}
	})

	b.Run("Single_Publisher_Single_Subscriber2", func(b *testing.B) {
		// Create client
		client, err := NewClient(ClientConfig{
			Address:       "localhost:9888",
			ClientID:      "bench-client-1",
			Path:          "/ws/kactor",
			AutoReconnect: true,
		})
		client.WithDebug(false)
		if err != nil {
			b.Fatal("Failed to create client:", err)
		}
		// defer client.Close()
		time.Sleep(1000 * time.Millisecond)

		//Subscribe
		var received int32
		sub := client.Subscribe("test", "sub1", func(payload map[string]any, s Subscription) {
			atomic.AddInt32(&received, 1)
		})
		if sub == nil {
			b.Fatal("Failed to subscribe")
		}
		defer sub.Unsubscribe()
		time.Sleep(300 * time.Millisecond)

		// Pre-allocate payload
		payload := map[string]any{"msg": "hello"}

		// Verify subscription works
		if !client.Publish("test", payload, nil) {
			b.Fatal("Failed to publish test message")
		}
		deadline := time.Now().Add(5 * time.Second)
		for atomic.LoadInt32(&received) == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if atomic.LoadInt32(&received) == 0 {
			b.Fatal("Failed to receive test message")
		}

		// Reset for benchmark
		atomic.StoreInt32(&received, 0)
		b.ResetTimer()
		b.ReportAllocs()

		// Run benchmark
		for i := 0; i < b.N; i++ {
			if !client.Publish("test", payload, nil) {
				b.Errorf("Failed to publish message %d", i)
				continue
			}
		}

		b.StopTimer()
		// Wait for remaining messages
		deadline = time.Now().Add(5 * time.Second)
		for atomic.LoadInt32(&received) < int32(b.N) && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}

		msgs := atomic.LoadInt32(&received)
		b.ReportMetric(float64(msgs), "msgs_received")
		if msgs < int32(b.N) {
			b.Errorf("Only received %d out of %d messages", msgs, b.N)
		}
	})

	b.Run("Single_Publisher_Multiple_Subscribers", func(b *testing.B) {
		// Create client
		client, err := NewClient(ClientConfig{
			Address:       "localhost:9888",
			ClientID:      "bench-client-2",
			Path:          "/ws/kactor",
			AutoReconnect: true,
		})
		if err != nil {
			b.Fatal("Failed to create client:", err)
		}
		defer client.Close()
		time.Sleep(time.Second)

		// Create multiple subscribers
		var received int32
		handler := func(payload map[string]any, s Subscription) {
			atomic.AddInt32(&received, 1)
		}
		subCount := 32
		var subs []Subscription
		for i := 0; i < subCount; i++ {
			sub := client.Subscribe("test", fmt.Sprintf("sub%d", i), handler)
			if sub == nil {
				b.Fatal("Failed to subscribe")
			}
			subs = append(subs, sub)
		}
		defer func() {
			for _, sub := range subs {
				sub.Unsubscribe()
			}
		}()
		time.Sleep(500 * time.Millisecond)

		// Pre-allocate payload
		payload := map[string]any{"msg": "hello"}

		// Verify subscriptions work
		if !client.Publish("test", payload, nil) {
			b.Fatal("Failed to publish test message")
		}
		deadline := time.Now().Add(5 * time.Second)
		for atomic.LoadInt32(&received) < int32(subCount) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if atomic.LoadInt32(&received) < int32(subCount) {
			b.Fatal("Failed to receive test message on all subscribers")
		}

		// Warmup phase
		b.Log("Starting warmup phase...")
		warmupCount := 1000
		for i := 0; i < warmupCount; i++ {
			if !client.Publish("test", payload, nil) {
				b.Error("Failed to publish warmup message")
				continue
			}
			if i%100 == 0 {
				time.Sleep(time.Millisecond * 10)
			}
		}

		// Wait for warmup messages
		deadline2 := time.Now().Add(5 * time.Second)
		for atomic.LoadInt32(&received) < int32(warmupCount*subCount) && time.Now().Before(deadline2) {
			time.Sleep(time.Millisecond * 50)
		}
		b.Logf("Warmup complete: received %d/%d messages", atomic.LoadInt32(&received), warmupCount*subCount)

		// Reset counter for benchmark
		atomic.StoreInt32(&received, 0)
		b.ResetTimer()
		b.ReportAllocs()

		// Run benchmark with controlled pacing
		messagesSent := 0
		for messagesSent < b.N {
			// Send messages in small chunks with controlled timing
			chunkSize := 50
			if messagesSent+chunkSize > b.N {
				chunkSize = b.N - messagesSent
			}

			for i := 0; i < chunkSize; i++ {
				if !client.Publish("test", payload, nil) {
					// b.Errorf("Failed to publish message %d", messagesSent+i)
					continue
				}
			}
			messagesSent += chunkSize

			// Allow time for processing between chunks
			time.Sleep(time.Millisecond * 100)

			// // Check progress
			// current := atomic.LoadInt32(&received)
			// expected := int32(messagesSent * subCount)
			// if current < expected/2 {
			// 	b.Logf("Progress: received %d/%d messages (%.1f%%)",
			// 		current, expected, float64(current)/float64(expected)*100)
			// }
		}

		b.StopTimer()
		// Wait for remaining messages
		deadline = time.Now().Add(5 * time.Second)
		expectedMsgs := int32(b.N * subCount)
		for atomic.LoadInt32(&received) < expectedMsgs && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond * 50)
		}

		msgs := atomic.LoadInt32(&received)
		b.ReportMetric(float64(msgs), "msgs_received")
		if msgs < expectedMsgs {
			b.Errorf("Only received %d out of %d messages", msgs, expectedMsgs)
		} else {
			b.Logf("received total %d", expectedMsgs)
		}
	})

	b.Run("Multiple_Publishers_Single_Subscriber", func(b *testing.B) {
		// Create client for subscriber
		subClient, err := NewClient(ClientConfig{
			Address:       "localhost:9888",
			ClientID:      "bench-sub-client",
			Path:          "/ws/kactor",
			AutoReconnect: true,
		})
		if err != nil {
			b.Fatal("Failed to create subscriber client:", err)
		}
		defer subClient.Close()
		time.Sleep(1000 * time.Millisecond)

		// Subscribe
		var received int32
		sub := subClient.Subscribe("test", "sub1", func(payload map[string]any, s Subscription) {
			atomic.AddInt32(&received, 1)
		})
		if sub == nil {
			b.Fatal("Failed to subscribe")
		}
		defer sub.Unsubscribe()
		time.Sleep(300 * time.Millisecond)

		// Pre-allocate payload
		payload := map[string]any{"msg": "hello"}

		// Verify subscription works
		if !subClient.Publish("test", payload, nil) {
			b.Fatal("Failed to publish test message")
		}
		deadline := time.Now().Add(5 * time.Second)
		for atomic.LoadInt32(&received) == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if atomic.LoadInt32(&received) == 0 {
			b.Fatal("Failed to receive test message")
		}

		// Reset for benchmark
		atomic.StoreInt32(&received, 0)
		b.ResetTimer()
		b.ReportAllocs()

		// Run parallel benchmark
		b.RunParallel(func(pb *testing.PB) {
			// Create client for publisher
			pubClient, err := NewClient(ClientConfig{
				Address:       "localhost:9888",
				ClientID:      fmt.Sprintf("bench-pub-client-%d", time.Now().UnixNano()),
				Path:          "/ws/kactor",
				AutoReconnect: true,
			})
			if err != nil {
				b.Fatal("Failed to create publisher client:", err)
			}
			defer pubClient.Close()
			time.Sleep(500 * time.Millisecond)

			// Create a local payload for this goroutine
			localPayload := map[string]any{"msg": "hello"}

			for pb.Next() {
				if !pubClient.Publish("test", localPayload, nil) {
					b.Error("Failed to publish message")
					continue
				}
			}
		})

		b.StopTimer()
		// Wait for remaining messages
		deadline = time.Now().Add(5 * time.Second)
		for atomic.LoadInt32(&received) < int32(b.N) && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}

		msgs := atomic.LoadInt32(&received)
		b.ReportMetric(float64(msgs), "msgs_received")
		if msgs < int32(b.N) {
			b.Errorf("Only received %d out of %d messages", msgs, b.N)
		}
	})

	b.Run("Multiple_Publishers_Multiple_Subscribers", func(b *testing.B) {
		// Create client for subscribers
		subClient, err := NewClient(ClientConfig{
			Address:       "localhost:9888",
			ClientID:      "bench-sub-client-multi",
			Path:          "/ws/kactor",
			AutoReconnect: true,
		})
		if err != nil {
			b.Fatal("Failed to create subscriber client:", err)
		}
		defer subClient.Close()
		time.Sleep(1000 * time.Millisecond)

		// Create multiple subscribers
		var received int32
		handler := func(payload map[string]any, s Subscription) {
			atomic.AddInt32(&received, 1)
		}

		var subs []Subscription
		for i := 0; i < 32; i++ {
			sub := subClient.Subscribe("test", fmt.Sprintf("sub%d", i), handler)
			if sub == nil {
				b.Fatal("Failed to subscribe")
			}
			subs = append(subs, sub)
		}
		defer func() {
			for _, sub := range subs {
				sub.Unsubscribe()
			}
		}()
		time.Sleep(300 * time.Millisecond)

		// Pre-allocate payload
		payload := map[string]any{"msg": "hello"}

		// Verify subscriptions work
		if !subClient.Publish("test", payload, nil) {
			b.Fatal("Failed to publish test message")
		}
		deadline := time.Now().Add(5 * time.Second)
		for atomic.LoadInt32(&received) < 32 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if atomic.LoadInt32(&received) < 32 {
			b.Fatal("Failed to receive test message on all subscribers")
		}

		// Reset for benchmark
		atomic.StoreInt32(&received, 0)
		b.ResetTimer()
		b.ReportAllocs()

		// Run parallel benchmark
		b.RunParallel(func(pb *testing.PB) {
			// Create client for publisher
			pubClient, err := NewClient(ClientConfig{
				Address:       "localhost:9888",
				ClientID:      fmt.Sprintf("bench-pub-client-multi-%d", time.Now().UnixNano()),
				Path:          "/ws/kactor",
				AutoReconnect: true,
			})
			if err != nil {
				b.Fatal("Failed to create publisher client:", err)
			}
			defer pubClient.Close()
			time.Sleep(500 * time.Millisecond)

			// Create a local payload for this goroutine
			localPayload := map[string]any{"msg": "hello"}

			for pb.Next() {
				if !pubClient.Publish("test", localPayload, nil) {
					b.Error("Failed to publish message")
					continue
				}
			}
		})

		b.StopTimer()
		// Wait for remaining messages
		deadline = time.Now().Add(5 * time.Second)
		expectedMsgs := int32(b.N * 32)
		for atomic.LoadInt32(&received) < expectedMsgs && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}

		msgs := atomic.LoadInt32(&received)
		b.ReportMetric(float64(msgs), "msgs_received")
		if msgs < expectedMsgs {
			b.Errorf("Only received %d out of %d messages", msgs, expectedMsgs)
		}
	})

	b.Run("Direct_Message_Performance", func(b *testing.B) {
		// Create client
		client, err := NewClient(ClientConfig{
			Address:       "localhost:9888",
			ClientID:      "bench-client-direct",
			Path:          "/ws/kactor",
			AutoReconnect: true,
		})
		if err != nil {
			b.Fatal("Failed to create client:", err)
		}
		defer client.Close()
		time.Sleep(1000 * time.Millisecond)

		// Subscribe
		var received int32
		sub := client.Subscribe("test", "sub1", func(payload map[string]any, s Subscription) {
			atomic.AddInt32(&received, 1)
		})
		if sub == nil {
			b.Fatal("Failed to subscribe")
		}
		defer sub.Unsubscribe()
		time.Sleep(300 * time.Millisecond)

		// Pre-allocate payload
		payload := map[string]any{"msg": "hello"}

		// Verify subscription works
		if !client.PublishTo("test", "sub1", payload, nil) {
			b.Fatal("Failed to publish test message")
		}
		deadline := time.Now().Add(5 * time.Second)
		for atomic.LoadInt32(&received) == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if atomic.LoadInt32(&received) == 0 {
			b.Fatal("Failed to receive test message")
		}

		// Reset for benchmark
		atomic.StoreInt32(&received, 0)
		b.ResetTimer()
		b.ReportAllocs()

		// Run benchmark
		for i := 0; i < b.N; i++ {
			if !client.PublishTo("test", "sub1", payload, nil) {
				b.Errorf("Failed to publish message %d", i)
				continue
			}
		}

		b.StopTimer()
		// Wait for remaining messages
		deadline = time.Now().Add(5 * time.Second)
		for atomic.LoadInt32(&received) < int32(b.N) && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}

		msgs := atomic.LoadInt32(&received)
		b.ReportMetric(float64(msgs), "msgs_received")
		if msgs < int32(b.N) {
			b.Errorf("Only received %d out of %d messages", msgs, b.N)
		}
	})
}
