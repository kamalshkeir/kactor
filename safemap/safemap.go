package safemap

import "sync"

type SafeMap[K comparable, V any] struct {
	mu *sync.RWMutex
	m  map[K]V
}

func New[K comparable, V any]() *SafeMap[K, V] {
	return &SafeMap[K, V]{
		m:  make(map[K]V),
		mu: &sync.RWMutex{},
	}
}

func (sm *SafeMap[K, V]) Set(key K, value V) {
	sm.mu.Lock()
	sm.m[key] = value
	sm.mu.Unlock()
}

func (sm *SafeMap[K, V]) Mu() *sync.RWMutex {
	return sm.mu
}

func (sm *SafeMap[K, V]) Map() map[K]V {
	return sm.m
}

func (sm *SafeMap[K, V]) Get(key K) (V, bool) {
	sm.mu.RLock()
	if v, ok := sm.m[key]; ok {
		return v, true
	}
	sm.mu.RUnlock()
	return *new(V), false
}

func (sm *SafeMap[K, V]) Delete(key K) {
	sm.mu.Lock()
	delete(sm.m, key)
	sm.mu.Unlock()
}

func (sm *SafeMap[K, V]) RangeR(fn func(key K, value V)) {
	sm.mu.RLock()
	for k, v := range sm.m {
		fn(k, v)
	}
	sm.mu.RUnlock()
}

func (sm *SafeMap[K, V]) RangeRW(fn func(key K, value V)) {
	sm.mu.Lock()
	for k, v := range sm.m {
		fn(k, v)
	}
	sm.mu.Unlock()
}

func (sm *SafeMap[K, V]) Flush() {
	sm.mu.Lock()
	clear(sm.m)
	sm.mu.Unlock()
}
func (sm *SafeMap[K, V]) Clear() {
	sm.mu.Lock()
	clear(sm.m)
	sm.mu.Unlock()
}
