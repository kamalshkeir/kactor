package kactor

import (
	"fmt"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
)

type Message interface{}

type messageQueue struct {
	mu     sync.RWMutex // Add mutex for queue operations
	buffer []Message
	head   int32
	tail   int32
}

type Actor struct {
	queues      []messageQueue
	done        chan struct{}
	handler     func(msgs []Message)
	batchSize   int
	processWg   sync.WaitGroup
	workerCount int32
	mask        int32
	mu          sync.RWMutex // Add mutex for actor state
	stopped     bool
}

func NewActor(queueSize, batchSize int, handler func(msgs []Message)) *Actor {
	if queueSize <= 0 {
		queueSize = 1 << 21
	}
	if batchSize <= 0 {
		batchSize = 8192
	}
	if handler == nil {
		panic("handler cannot be nil")
	}

	queueSize = 1 << uint(32-bits.LeadingZeros32(uint32(queueSize-1)))
	workerCount := int32(runtime.NumCPU())
	queues := make([]messageQueue, workerCount)
	for i := range queues {
		queues[i] = messageQueue{
			buffer: make([]Message, queueSize),
		}
	}

	return &Actor{
		queues:      queues,
		done:        make(chan struct{}),
		handler:     handler,
		batchSize:   batchSize,
		workerCount: workerCount,
		mask:        int32(queueSize - 1),
		stopped:     false,
	}
}

func (a *Actor) Start() {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		panic("cannot start a stopped actor")
	}
	a.mu.Unlock()

	workerCount := atomic.LoadInt32(&a.workerCount)
	a.processWg.Add(int(workerCount))

	for i := 0; i < int(workerCount); i++ {
		go func(id int) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Worker %d recovered from panic: %v\n", id, r)
				}
				a.processWg.Done()
			}()
			runtime.LockOSThread()
			a.processBatch(id)
		}(i)
	}
}

func (a *Actor) Stop() {
	a.mu.Lock()
	if !a.stopped {
		a.stopped = true
		if a.done != nil {
			close(a.done)
		}
	}
	a.mu.Unlock()
	a.processWg.Wait()
}

func (a *Actor) Send(msg Message, workerID int) bool {
	if msg == nil {
		return false
	}

	a.mu.RLock()
	if a.stopped {
		a.mu.RUnlock()
		return false
	}
	a.mu.RUnlock()

	q := &a.queues[workerID]
	q.mu.Lock()
	defer q.mu.Unlock()

	tail := atomic.LoadInt32(&q.tail)
	nextTail := (tail + 1) & a.mask

	if nextTail == atomic.LoadInt32(&q.head) {
		return false
	}

	q.buffer[tail] = msg
	atomic.StoreInt32(&q.tail, nextTail)
	return true
}

func (a *Actor) processBatch(workerID int) {
	q := &a.queues[workerID]
	batch := make([]Message, a.batchSize)
	localHead := atomic.LoadInt32(&q.head)

	for {
		select {
		case <-a.done:
			// Process any remaining messages before exiting
			q.mu.RLock()
			tail := atomic.LoadInt32(&q.tail)
			if localHead != tail {
				available := int((tail - localHead) & a.mask)
				if available > a.batchSize {
					available = a.batchSize
				}
				idx := localHead & a.mask
				for i := 0; i < available; i++ {
					batch[i] = q.buffer[idx]
					idx = (idx + 1) & a.mask
				}
				q.mu.RUnlock()

				func() {
					defer func() {
						if r := recover(); r != nil {
							fmt.Printf("Handler recovered from panic: %v\n", r)
						}
					}()
					a.handler(batch[:available])
				}()
			} else {
				q.mu.RUnlock()
			}
			return
		default:
			q.mu.RLock()
			tail := atomic.LoadInt32(&q.tail)
			if localHead == tail {
				q.mu.RUnlock()
				runtime.Gosched()
				continue
			}

			available := int((tail - localHead) & a.mask)
			if available > a.batchSize {
				available = a.batchSize
			}

			idx := localHead & a.mask
			for i := 0; i < available; i++ {
				batch[i] = q.buffer[idx]
				idx = (idx + 1) & a.mask
			}
			q.mu.RUnlock()

			localHead = (localHead + int32(available)) & a.mask
			atomic.StoreInt32(&q.head, localHead)

			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Printf("Handler recovered from panic: %v\n", r)
					}
				}()
				a.handler(batch[:available])
			}()
		}
	}
}
