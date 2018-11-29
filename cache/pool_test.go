package cache

import "testing"

func TestAllocatorPool(t *testing.T) {
	a := NewAllocatorPool(10).(*AllocatorPool)
	n := a.Alloc(1)
	n.Free()
	if a.metrics.Malloc != 1 {
		t.Fatal(a.metrics.Malloc)
	}
	if a.metrics.New != 1 {
		t.Fatal(a.metrics.New)
	}
	if a.metrics.Free != 1 {
		t.Fatal(a.metrics.Malloc)
	}
	n = a.Alloc(1)
	n.Free()
	if a.metrics.Malloc != 2 {
		t.Fatal(a.metrics.Malloc)
	}
	if a.metrics.New != 1 {
		t.Fatal(a.metrics.New)
	}
	if a.metrics.Free != 2 {
		t.Fatal(a.metrics.Malloc)
	}
	n = a.Alloc(11)
	n.Free()
	if a.metrics.Malloc != 3 {
		t.Fatal(a.metrics.Malloc)
	}
	if a.metrics.New != 2 {
		t.Fatal(a.metrics.New)
	}
	if a.metrics.Free != 3 {
		t.Fatal(a.metrics.Malloc)
	}
}
