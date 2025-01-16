package kactor

import (
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
)

type Message interface{}

type messageQueue struct {
	_      [8]uint64
	buffer []Message
	head   int32
	tail   int32
	_      [8]uint64
}

type Actor struct {
	queues      []messageQueue
	done        chan struct{}
	handler     func(msgs []Message)
	batchSize   int
	processWg   sync.WaitGroup
	workerCount int32
	mask        int32
	mu          sync.Mutex
	stopped     bool
}

func NewActor(queueSize, batchSize int, handler func(msgs []Message)) *Actor {
	if queueSize <= 0 {
		queueSize = 1 << 21
	}
	if batchSize <= 0 {
		batchSize = 8192
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
	workerCount := atomic.LoadInt32(&a.workerCount)
	a.processWg.Add(int(workerCount))

	for i := 0; i < int(workerCount); i++ {
		go func(id int) {
			runtime.LockOSThread()
			a.processBatch(id)
		}(i)
	}
}

func (a *Actor) Stop() {
	a.mu.Lock()
	if !a.stopped {
		a.stopped = true
		close(a.done)
	}
	a.mu.Unlock()
	a.processWg.Wait()
}

//go:nosplit
//go:noinline
func (a *Actor) Send(msg Message, workerID int) bool {
	q := &a.queues[workerID]
	tail := atomic.LoadInt32(&q.tail)
	nextTail := (tail + 1) & a.mask

	if nextTail == atomic.LoadInt32(&q.head) {
		return false
	}

	q.buffer[tail] = msg
	atomic.StoreInt32(&q.tail, nextTail)
	return true
}

//go:nosplit
func (a *Actor) processBatch(workerID int) {
	defer a.processWg.Done()

	q := &a.queues[workerID]
	batch := make([]Message, a.batchSize)
	localHead := atomic.LoadInt32(&q.head)

	for {
		select {
		case <-a.done:
			// Process any remaining messages before exiting
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
				a.handler(batch[:available])
			}
			return
		default:
			tail := atomic.LoadInt32(&q.tail)
			if localHead == tail {
				// No messages, check done channel more frequently
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

			localHead = (localHead + int32(available)) & a.mask
			atomic.StoreInt32(&q.head, localHead)
			a.handler(batch[:available])
		}
	}
}
