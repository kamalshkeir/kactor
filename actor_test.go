package kactor

// cpu: Intel(R) Core(TM) i5-7300HQ CPU @ 2.50GHz
// BenchmarkActorVsChannel/Actor/\x01-4            42670762                55.07 ns/op           25 B/op          0 allocs/op
// BenchmarkActorVsChannel/BufferedChannel/\x01-4             38718             31214 ns/op               0 B/op          0 allocs/op
// BenchmarkActorVsChannel/MultiGoroutine/\x01-4             154094              7748 ns/op               0 B/op          0 allocs/op
// BenchmarkActorVsChannel/Actor/_-4                       33829784                64.30 ns/op           32 B/op          0 allocs/op
// BenchmarkActorVsChannel/BufferedChannel/_-4                39073             31019 ns/op               0 B/op          0 allocs/op
// BenchmarkActorVsChannel/MultiGoroutine/_-4                163326              7399 ns/op               0 B/op          0 allocs/op
// BenchmarkActorVsChannel/Actor/d-4                       53804420                54.98 ns/op           20 B/op          0 allocs/op
// BenchmarkActorVsChannel/BufferedChannel/d-4                40250             31044 ns/op               0 B/op          0 allocs/op
// BenchmarkActorVsChannel/MultiGoroutine/d-4                175705              7216 ns/op               0 B/op          0 allocs/op
// BenchmarkActorVsChannel/Actor/Ϩ-4                       23910456                46.31 ns/op           20 B/op          0 allocs/op
// BenchmarkActorVsChannel/BufferedChannel/Ϩ-4                48908             29325 ns/op               0 B/op          0 allocs/op
// BenchmarkActorVsChannel/MultiGoroutine/Ϩ-4                408698              7178 ns/op               0 B/op          0 allocs/op
// BenchmarkActorVsChannel/Actor/✐-4                        9758835               108.9 ns/op            21 B/op          0 allocs/op
// BenchmarkActorVsChannel/BufferedChannel/✐-4              1000000             30666 ns/op               0 B/op          0 allocs/op
// BenchmarkActorVsChannel/MultiGoroutine/✐-4               1000000              6749 ns/op               0 B/op          0 allocs/op
// BenchmarkActorThroughput-4                              14490916                72.73 ns/op           14 B/op          0 allocs/op
// BenchmarkActorLatency-4                                    29018             40975 ns/op               0 B/op          0 allocs/op
// BenchmarkActorBackPressure-4                            26844535                53.76 ns/op           20 B/op          0 allocs/op

// type benchMessage struct {
// 	value int64
// }

// // BenchmarkActorVsChannel runs comparative benchmarks between Actor and channels
// func BenchmarkActorVsChannel(b *testing.B) {
// 	runtime.GOMAXPROCS(runtime.NumCPU())
// 	sizes := []int{1, 10, 100, 1000, 10000}

// 	for _, size := range sizes {
// 		// Benchmark Actor
// 		b.Run("Actor/"+string(rune(size)), func(b *testing.B) {
// 			actor := NewActor(0, size, func(msgs []Message) {
// 				// Process messages in batch
// 				for range msgs {
// 					// Simulate work
// 					runtime.Gosched()
// 				}
// 			})
// 			actor.Start()
// 			defer actor.Stop()

// 			b.ResetTimer()
// 			b.RunParallel(func(pb *testing.PB) {
// 				msg := benchMessage{value: 1}
// 				workerID := 0
// 				for pb.Next() {
// 					actor.Send(msg, workerID)
// 				}
// 			})
// 		})

// 		// Benchmark Channel with Buffer
// 		b.Run("BufferedChannel/"+string(rune(size)), func(b *testing.B) {
// 			ch := make(chan benchMessage, size)
// 			var wg sync.WaitGroup
// 			wg.Add(1)

// 			// Start consumer
// 			go func() {
// 				defer wg.Done()
// 				batch := make([]benchMessage, size)
// 				count := 0

// 				for msg := range ch {
// 					batch[count] = msg
// 					count++

// 					if count == size {
// 						// Process batch
// 						for range batch[:count] {
// 							runtime.Gosched()
// 						}
// 						count = 0
// 					}
// 				}

// 				// Process remaining messages
// 				if count > 0 {
// 					for range batch[:count] {
// 						runtime.Gosched()
// 					}
// 				}
// 			}()

// 			b.ResetTimer()
// 			b.RunParallel(func(pb *testing.PB) {
// 				msg := benchMessage{value: 1}
// 				for pb.Next() {
// 					ch <- msg
// 				}
// 			})

// 			b.StopTimer()
// 			close(ch)
// 			wg.Wait()
// 		})

// 		// Benchmark Multiple Goroutines with Channels
// 		b.Run("MultiGoroutine/"+string(rune(size)), func(b *testing.B) {
// 			numWorkers := runtime.NumCPU()
// 			channels := make([]chan benchMessage, numWorkers)
// 			var wg sync.WaitGroup

// 			// Create channels and start workers
// 			for i := 0; i < numWorkers; i++ {
// 				channels[i] = make(chan benchMessage, size)
// 				wg.Add(1)

// 				go func(ch chan benchMessage) {
// 					defer wg.Done()
// 					batch := make([]benchMessage, size)
// 					count := 0

// 					for msg := range ch {
// 						batch[count] = msg
// 						count++

// 						if count == size {
// 							// Process batch
// 							for range batch[:count] {
// 								runtime.Gosched()
// 							}
// 							count = 0
// 						}
// 					}

// 					// Process remaining messages
// 					if count > 0 {
// 						for range batch[:count] {
// 							runtime.Gosched()
// 						}
// 					}
// 				}(channels[i])
// 			}

// 			b.ResetTimer()
// 			b.RunParallel(func(pb *testing.PB) {
// 				msg := benchMessage{value: 1}
// 				workerID := 0
// 				for pb.Next() {
// 					channels[workerID] <- msg
// 					workerID = (workerID + 1) % numWorkers
// 				}
// 			})

// 			b.StopTimer()
// 			for _, ch := range channels {
// 				close(ch)
// 			}
// 			wg.Wait()
// 		})
// 	}
// }

// // BenchmarkActorThroughput measures message throughput
// func BenchmarkActorThroughput(b *testing.B) {
// 	runtime.GOMAXPROCS(runtime.NumCPU())

// 	actor := NewActor(0, 8192, func(msgs []Message) {
// 		for range msgs {
// 			runtime.Gosched()
// 		}
// 	})
// 	actor.Start()
// 	defer actor.Stop()

// 	b.ResetTimer()
// 	b.RunParallel(func(pb *testing.PB) {
// 		msg := benchMessage{value: 1}
// 		workerID := 0
// 		for pb.Next() {
// 			actor.Send(msg, workerID)
// 		}
// 	})
// }

// // BenchmarkActorLatency measures message latency
// func BenchmarkActorLatency(b *testing.B) {
// 	runtime.GOMAXPROCS(runtime.NumCPU())

// 	done := make(chan struct{})
// 	actor := NewActor(0, 1, func(msgs []Message) {
// 		for range msgs {
// 			done <- struct{}{}
// 		}
// 	})
// 	actor.Start()
// 	defer actor.Stop()

// 	msg := benchMessage{value: 1}
// 	b.ResetTimer()

// 	for i := 0; i < b.N; i++ {
// 		actor.Send(msg, 0)
// 		<-done
// 	}
// }

// // BenchmarkActorBackPressure tests behavior under back pressure
// func BenchmarkActorBackPressure(b *testing.B) {
// 	runtime.GOMAXPROCS(runtime.NumCPU())

// 	slowActor := NewActor(1<<16, 100, func(msgs []Message) {
// 		// Simulate slow processing
// 		for range msgs {
// 			runtime.Gosched()
// 			runtime.Gosched()
// 			runtime.Gosched()
// 		}
// 	})
// 	slowActor.Start()
// 	defer slowActor.Stop()

// 	b.ResetTimer()
// 	b.RunParallel(func(pb *testing.PB) {
// 		msg := benchMessage{value: 1}
// 		workerID := 0
// 		successCount := 0
// 		for pb.Next() {
// 			if slowActor.Send(msg, workerID) {
// 				successCount++
// 			}
// 		}
// 	})
// }
