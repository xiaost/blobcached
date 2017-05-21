package cache

import "testing"

func TestAllocatorPool(t *testing.T) {
	allocator := NewAllocatorPool()
	b := allocator.Malloc(4000)
	if len(b) != 4000 || cap(b) != 4096 {
		t.Fatalf("malloc err: len:%d cap:%d", len(b), cap(b))
	}
	allocator.Free(b)
	b = allocator.Malloc(100 << 10)
	if len(b) != 100<<10 || cap(b) != 128<<10 {
		t.Fatalf("malloc err: len:%d cap:%d", len(b), cap(b))
	}
	allocator.Free(b)

	for i := 0; i < 1000; i++ {
		b := allocator.Malloc(8 << 10)
		allocator.Free(b)
	}

	metrics := allocator.(*AllocatorPool).GetMetrics()
	if metrics.Malloc != 1002 || metrics.Free != 1002 || metrics.New != 3 {
		t.Fatalf("metrics err %+v", metrics)
	}

	allocator.Malloc(int(MaxValueSize + 1))
	allocator.Free(make([]byte, 1))
	metrics = allocator.(*AllocatorPool).GetMetrics()
	if metrics.ErrMalloc != 1 || metrics.ErrFree != 1 {
		t.Fatal("metrics err", metrics)
	}
}
