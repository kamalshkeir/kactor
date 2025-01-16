package kactor

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// cpu: Intel(R) Core(TM) i5-7300HQ CPU @ 2.50GHz
// BenchmarkPubSub/Single_Publisher_Single_Subscriber-4            21505723                55.82 ns/op       21505723 msgs_received               0 B/op          0 allocs/op
// BenchmarkPubSub/Single_Publisher_Multiple_Subscribers-4          3841304               312.5 ns/op       122921728 msgs_received               0 B/op          0 allocs/op
// BenchmarkPubSub/Multiple_Publishers_Single_Subscriber-4         11387337               111.8 ns/op        11387337 msgs_received               0 B/op          0 allocs/op
// BenchmarkPubSub/Multiple_Publishers_Multiple_Subscribers-4       2041062               729.5 ns/op        65313984 msgs_received               0 B/op          0 allocs/op
// BenchmarkPubSub/Direct_Message_Performance-4                    22120386                54.12 ns/op       22120386 msgs_received               0 B/op          0 allocs/op
// BenchmarkPubSubRetry/Default_Retry_Config-4                      6903525               165.8 ns/op         6903525 msgs_received               0 B/op          0 allocs/op
// BenchmarkPubSubRetry/Aggressive_Retry-4                          6323730               178.6 ns/op         6323730 msgs_received               0 B/op          0 allocs/op
// BenchmarkPubSubRetry/Light_Retry-4                               6599740               162.3 ns/op         6599740 msgs_received               0 B/op          0 allocs/op
// PASS
// ok      github.com/kamalshkeir/kactor   32.287s

func BenchmarkPubSub(b *testing.B) {
	b.Run("Single_Publisher_Single_Subscriber", func(b *testing.B) {
		ps := NewPubSub()
		defer ps.Close()

		var received int32
		ps.Subscribe("test", "sub1", func(payload map[string]any, sub Subscription) {
			atomic.AddInt32(&received, 1)
		})

		payload := map[string]any{"msg": "hello"}
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ps.Publish("test", payload, nil)
		}

		b.ReportMetric(float64(received), "msgs_received")
	})

	b.Run("Single_Publisher_Multiple_Subscribers", func(b *testing.B) {
		ps := NewPubSub()
		defer ps.Close()

		var received int32
		handler := func(payload map[string]any, sub Subscription) {
			atomic.AddInt32(&received, 1)
		}

		for i := 0; i < 32; i++ {
			ps.Subscribe("test", fmt.Sprintf("sub%d", i), handler)
		}

		payload := map[string]any{"msg": "hello"}
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ps.Publish("test", payload, nil)
		}

		b.ReportMetric(float64(received), "msgs_received")
	})

	b.Run("Multiple_Publishers_Single_Subscriber", func(b *testing.B) {
		ps := NewPubSub()
		defer ps.Close()

		var received int32
		ps.Subscribe("test", "sub1", func(payload map[string]any, sub Subscription) {
			atomic.AddInt32(&received, 1)
		})

		payload := map[string]any{"msg": "hello"}
		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ps.Publish("test", payload, nil)
			}
		})

		b.ReportMetric(float64(received), "msgs_received")
	})

	b.Run("Multiple_Publishers_Multiple_Subscribers", func(b *testing.B) {
		ps := NewPubSub()
		defer ps.Close()

		var received int32
		handler := func(payload map[string]any, sub Subscription) {
			atomic.AddInt32(&received, 1)
		}

		for i := 0; i < 32; i++ {
			ps.Subscribe("test", fmt.Sprintf("sub%d", i), handler)
		}

		payload := map[string]any{"msg": "hello"}
		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ps.Publish("test", payload, nil)
			}
		})

		b.ReportMetric(float64(received), "msgs_received")
	})

	b.Run("Direct_Message_Performance", func(b *testing.B) {
		ps := NewPubSub()
		defer ps.Close()

		var received int32
		ps.Subscribe("test", "sub1", func(payload map[string]any, sub Subscription) {
			atomic.AddInt32(&received, 1)
		})

		payload := map[string]any{"msg": "hello"}
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ps.PublishTo("test", "sub1", payload, nil)
		}

		b.ReportMetric(float64(received), "msgs_received")
	})
}

func BenchmarkPubSubRetry(b *testing.B) {
	b.Run("Default_Retry_Config", func(b *testing.B) {
		ps := NewPubSub()
		defer ps.Close()

		var received int32
		ps.Subscribe("test", "sub1", func(payload map[string]any, sub Subscription) {
			atomic.AddInt32(&received, 1)
		})

		payload := map[string]any{"msg": "hello"}
		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ps.PublishWithRetry("test", payload, nil, nil)
			}
		})

		b.ReportMetric(float64(received), "msgs_received")
	})

	b.Run("Aggressive_Retry", func(b *testing.B) {
		ps := NewPubSub()
		defer ps.Close()

		var received int32
		ps.Subscribe("test", "sub1", func(payload map[string]any, sub Subscription) {
			atomic.AddInt32(&received, 1)
		})

		payload := map[string]any{"msg": "hello"}
		cfg := &RetryConfig{
			MaxAttempts: 5,
			MaxBackoff:  3,
		}

		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ps.PublishWithRetry("test", payload, cfg, nil)
			}
		})

		b.ReportMetric(float64(received), "msgs_received")
	})

	b.Run("Light_Retry", func(b *testing.B) {
		ps := NewPubSub()
		defer ps.Close()

		var received int32
		ps.Subscribe("test", "sub1", func(payload map[string]any, sub Subscription) {
			atomic.AddInt32(&received, 1)
		})

		payload := map[string]any{"msg": "hello"}
		cfg := &RetryConfig{
			MaxAttempts: 2,
			MaxBackoff:  1,
		}

		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ps.PublishWithRetry("test", payload, cfg, nil)
			}
		})

		b.ReportMetric(float64(received), "msgs_received")
	})
}
