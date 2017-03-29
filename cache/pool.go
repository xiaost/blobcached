package cache

import (
	"sync"
	"sync/atomic"
)

var bytessize = []int{
	4 << 10, 8 << 10, 16 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10, 512 << 10,
	1 << 20, 2 << 20, 4 << 20, 8 << 20, 16 << 20, 32 << 20, 64 << 20,
	int(MaxValueSize),
}

type AllocatorPoolMetrics struct {
	Malloc int64
	Free   int64
	New    int64

	ErrMalloc int64
	ErrFree   int64
}

type AllocatorPool struct {
	pools   []*sync.Pool
	metrics AllocatorPoolMetrics
}

func NewAllocatorPool() Allocator {
	var p AllocatorPool
	makeBytesPool := func(n int) *sync.Pool {
		return &sync.Pool{New: func() interface{} {
			atomic.AddInt64(&p.metrics.New, 1)
			return make([]byte, n, n)
		}}
	}
	p.pools = make([]*sync.Pool, len(bytessize))
	for i, n := range bytessize {
		p.pools[i] = makeBytesPool(n)
	}
	return &p
}

func (p *AllocatorPool) Malloc(n int) []byte {
	atomic.AddInt64(&p.metrics.Malloc, 1)
	for i, v := range bytessize {
		if v >= n {
			b := p.pools[i].Get().([]byte)
			return b[:n]
		}
	}
	atomic.AddInt64(&p.metrics.ErrMalloc, 1)
	return make([]byte, n, n)
}

func (p *AllocatorPool) Free(b []byte) {
	atomic.AddInt64(&p.metrics.Free, 1)
	for i, n := range bytessize {
		if n == cap(b) {
			p.pools[i].Put(b)
			return
		}
	}
	atomic.AddInt64(&p.metrics.ErrFree, 1)
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
