package server

import (
	"sync"
	"time"
	"unsafe"

	"blobcached/cache"
)

type InMemoryCache struct {
	m map[string]cache.Item

	options cache.CacheOptions

	stats   cache.CacheStats
	metrics cache.CacheMetrics
}

func NewInMemoryCache() *InMemoryCache {
	c := &InMemoryCache{}
	c.m = make(map[string]cache.Item)
	c.options.Allocator = cache.NewAllocatorPool()
	return c
}

func (c *InMemoryCache) Set(item cache.Item) error {
	c.metrics.SetTotal += 1
	item.Timestamp = time.Now().Unix()
	c.m[item.Key] = item
	c.updateStats()
	return nil
}

func (c *InMemoryCache) Get(key string) (cache.Item, error) {
	c.metrics.GetTotal += 1
	item, ok := c.m[key]
	if ok {
		c.metrics.GetHits += 1
		return item, nil
	}
	c.metrics.GetMisses += 1
	return item, cache.ErrNotFound
}

func (c *InMemoryCache) Del(key string) error {
	c.metrics.DelTotal += 1
	delete(c.m, key)
	c.updateStats()
	return nil
}

func (c *InMemoryCache) updateStats() {
	c.stats.Keys = uint64(len(c.m))
	c.stats.Bytes = 0
	for _, it := range c.m {
		c.stats.Bytes += uint64(len(it.Key) + len(it.Value))
	}
	c.stats.LastUpdate = time.Now().Unix()
}

func (c *InMemoryCache) GetOptions() cache.CacheOptions {
	return c.options
}

func (c *InMemoryCache) GetMetrics() cache.CacheMetrics {
	return c.metrics
}

func (c *InMemoryCache) GetMetricsByShards() []cache.CacheMetrics {
	return []cache.CacheMetrics{c.metrics}
}

func (c *InMemoryCache) GetStats() cache.CacheStats {
	return c.stats
}

func (c *InMemoryCache) GetStatsByShards() []cache.CacheStats {
	return []cache.CacheStats{c.stats}
}

type DebugAllocator struct {
	mu  sync.Mutex
	all map[unsafe.Pointer]bool
}

func NewDebugAllocator() *DebugAllocator {
	return &DebugAllocator{all: make(map[unsafe.Pointer]bool)}
}

func (a *DebugAllocator) Free(b []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p := unsafe.Pointer(&b[0])
	if !a.all[p] {
		panic(p)
	}
}

func (a *DebugAllocator) Malloc(n int) []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	b := make([]byte, n)
	p := unsafe.Pointer(&b[0])
	a.all[p] = true
	return b
}
