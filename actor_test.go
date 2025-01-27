package kactor

import (
	"runtime"
	"sync"
	"testing"
)

// cpu: Intel(R) Core(TM) i5-7300HQ CPU @ 2.50GHz
// BenchmarkActorVsChannel
// BenchmarkActorVsChannel/Actor/\x01
// BenchmarkActorVsChannel/Actor/\x01-4            12454398                84.91 ns/op            0 B/op              0 allocs/op
// BenchmarkActorVsChannel/BufferedChannel/\x01
// BenchmarkActorVsChannel/BufferedChannel/\x01-4           1303060               890.2 ns/op             0 B/op          0 allocs/op
// BenchmarkActorVsChannel/MultiGoroutine/\x01
// BenchmarkActorVsChannel/MultiGoroutine/\x01-4            2200990               546.9 ns/op             0 B/op          0 allocs/op
// BenchmarkActorVsChannel/Actor/_
// BenchmarkActorVsChannel/Actor/_-4                       14099002                76.95 ns/op            0 B/op          0 allocs/op
// BenchmarkActorVsChannel/BufferedChannel/_
// BenchmarkActorVsChannel/BufferedChannel/_-4              2705827               440.0 ns/op             0 B/op          0 allocs/op
// BenchmarkActorVsChannel/MultiGoroutine/_
// BenchmarkActorVsChannel/MultiGoroutine/_-4               4239594               283.4 ns/op             0 B/op          0 allocs/op
// BenchmarkActorVsChannel/Actor/d
// BenchmarkActorVsChannel/Actor/d-4                       14526086                73.67 ns/op            0 B/op          0 allocs/op
// BenchmarkActorVsChannel/BufferedChannel/d
// BenchmarkActorVsChannel/BufferedChannel/d-4              4286130               281.4 ns/op             0 B/op          0 allocs/op
// BenchmarkActorVsChannel/MultiGoroutine/d
// BenchmarkActorVsChannel/MultiGoroutine/d-4               6885091               169.3 ns/op             0 B/op          0 allocs/op
// BenchmarkActorVsChannel/Actor/Ϩ
// BenchmarkActorVsChannel/Actor/Ϩ-4                       16986211                68.26 ns/op            0 B/op          0 allocs/op
// BenchmarkActorVsChannel/BufferedChannel/Ϩ
// BenchmarkActorVsChannel/BufferedChannel/Ϩ-4              4469100               269.8 ns/op             0 B/op          0 allocs/op
// BenchmarkActorVsChannel/MultiGoroutine/Ϩ
// BenchmarkActorVsChannel/MultiGoroutine/Ϩ-4               7563508               156.7 ns/op             0 B/op          0 allocs/op
// BenchmarkActorVsChannel/Actor/✐
// BenchmarkActorVsChannel/Actor/✐-4                       10660014                97.68 ns/op            0 B/op          0 allocs/op
// BenchmarkActorVsChannel/BufferedChannel/✐
// BenchmarkActorVsChannel/BufferedChannel/✐-4              4603777               265.0 ns/op             0 B/op          0 allocs/op
// BenchmarkActorVsChannel/MultiGoroutine/✐
// BenchmarkActorVsChannel/MultiGoroutine/✐-4               7971742               155.9 ns/op             0 B/op          0 allocs/op
// BenchmarkActorThroughput
// BenchmarkActorThroughput-4                              13171784                82.26 ns/op            0 B/op          0 allocs/op
// BenchmarkActorLatency
// BenchmarkActorLatency-4                                   127534              9390 ns/op               0 B/op          0 allocs/op
// BenchmarkActorBackPressure
// BenchmarkActorBackPressure-4                            15562069                68.67 ns/op            0 B/op          0 allocs/op

type benchMessage struct {
	value int64
}

// BenchmarkActorVsChannel runs comparative benchmarks between Actor and channels
func BenchmarkActorVsChannel(b *testing.B) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	sizes := []int{1, 10, 100, 1000, 10000}

	for _, size := range sizes {
		// Benchmark Actor
		b.Run("Actor/"+string(rune(size)), func(b *testing.B) {
			actor := NewActor(0, size, func(msgs []Message) {
				// Process messages in batch
				for range msgs {
					// Simulate work
					runtime.Gosched()
				}
			})
			actor.Start()
			defer actor.Stop()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				msg := benchMessage{value: 1}
				workerID := 0
				for pb.Next() {
					actor.Send(msg, workerID)
				}
			})
		})

		// Benchmark Channel with Buffer
		b.Run("BufferedChannel/"+string(rune(size)), func(b *testing.B) {
			ch := make(chan benchMessage, size)
			var wg sync.WaitGroup
			wg.Add(1)

			// Start consumer
			go func() {
				defer wg.Done()
				batch := make([]benchMessage, size)
				count := 0

				for msg := range ch {
					batch[count] = msg
					count++

					if count == size {
						// Process batch
						for range batch[:count] {
							runtime.Gosched()
						}
						count = 0
					}
				}

				// Process remaining messages
				if count > 0 {
					for range batch[:count] {
						runtime.Gosched()
					}
				}
			}()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				msg := benchMessage{value: 1}
				for pb.Next() {
					ch <- msg
				}
			})

			b.StopTimer()
			close(ch)
			wg.Wait()
		})

		// Benchmark Multiple Goroutines with Channels
		b.Run("MultiGoroutine/"+string(rune(size)), func(b *testing.B) {
			numWorkers := runtime.NumCPU()
			channels := make([]chan benchMessage, numWorkers)
			var wg sync.WaitGroup

			// Create channels and start workers
			for i := 0; i < numWorkers; i++ {
				channels[i] = make(chan benchMessage, size)
				wg.Add(1)

				go func(ch chan benchMessage) {
					defer wg.Done()
					batch := make([]benchMessage, size)
					count := 0

					for msg := range ch {
						batch[count] = msg
						count++

						if count == size {
							// Process batch
							for range batch[:count] {
								runtime.Gosched()
							}
							count = 0
						}
					}

					// Process remaining messages
					if count > 0 {
						for range batch[:count] {
							runtime.Gosched()
						}
					}
				}(channels[i])
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				msg := benchMessage{value: 1}
				workerID := 0
				for pb.Next() {
					channels[workerID] <- msg
					workerID = (workerID + 1) % numWorkers
				}
			})

			b.StopTimer()
			for _, ch := range channels {
				close(ch)
			}
			wg.Wait()
		})
	}
}

// BenchmarkActorThroughput measures message throughput
func BenchmarkActorThroughput(b *testing.B) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	actor := NewActor(0, 8192, func(msgs []Message) {
		for range msgs {
			runtime.Gosched()
		}
	})
	actor.Start()
	defer actor.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		msg := benchMessage{value: 1}
		workerID := 0
		for pb.Next() {
			actor.Send(msg, workerID)
		}
	})
}

// BenchmarkActorLatency measures message latency
func BenchmarkActorLatency(b *testing.B) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	done := make(chan struct{})
	actor := NewActor(0, 1, func(msgs []Message) {
		for range msgs {
			done <- struct{}{}
		}
	})
	actor.Start()
	defer actor.Stop()

	msg := benchMessage{value: 1}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		actor.Send(msg, 0)
		<-done
	}
}

// BenchmarkActorBackPressure tests behavior under back pressure
func BenchmarkActorBackPressure(b *testing.B) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	slowActor := NewActor(1<<16, 100, func(msgs []Message) {
		// Simulate slow processing
		for range msgs {
			runtime.Gosched()
			runtime.Gosched()
			runtime.Gosched()
		}
	})
	slowActor.Start()
	defer slowActor.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		msg := benchMessage{value: 1}
		workerID := 0
		successCount := 0
		for pb.Next() {
			if slowActor.Send(msg, workerID) {
				successCount++
			}
		}
	})
}
