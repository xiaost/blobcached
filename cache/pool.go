package cache

import (
	"sync"
	"sync/atomic"
)

type AllocatorPoolMetrics struct {
	Malloc int64
	Free   int64
	New    int64

	ErrMalloc int64
	ErrFree   int64
}

type AllocatorPool struct {
	pool    sync.Pool
	metrics AllocatorPoolMetrics
}

func NewAllocatorPool(bufsize int) Allocator {
	p := AllocatorPool{}
	p.pool.New = func() interface{} {
		atomic.AddInt64(&p.metrics.New, 1)
		return &Item{Value: make([]byte, 0, bufsize)}
	}
	return &p
}

func (p *AllocatorPool) Alloc(n int) *Item {
	atomic.AddInt64(&p.metrics.Malloc, 1)
	item := p.pool.Get().(*Item)
	if cap(item.Value) < n {
		p.pool.Put(item)
		atomic.AddInt64(&p.metrics.New, 1)
		item = &Item{Value: make([]byte, 0, n)}
	}
	item.free = p.Free
	item.Key = ""
	item.Value = item.Value[:n]
	item.Timestamp = 0
	item.TTL = 0
	item.Flags = 0
	return item
}

func (p *AllocatorPool) Free(i *Item) {
	atomic.AddInt64(&p.metrics.Free, 1)
	p.pool.Put(i)
}

func (p *AllocatorPool) GetMetrics() AllocatorPoolMetrics {
	var ret AllocatorPoolMetrics
	ret.Malloc = atomic.LoadInt64(&p.metrics.Malloc)
	ret.Free = atomic.LoadInt64(&p.metrics.Free)
	ret.New = atomic.LoadInt64(&p.metrics.New)
	ret.ErrMalloc = atomic.LoadInt64(&p.metrics.ErrMalloc)
	ret.ErrFree = atomic.LoadInt64(&p.metrics.ErrFree)
	return ret
}
