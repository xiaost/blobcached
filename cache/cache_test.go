package cache

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func TestCache(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_cache")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	c, err := NewCache(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 100)

	n := 1000
	for i := 0; i < n; i++ {
		key := strconv.FormatInt(rand.Int63(), 16)
		rand.Read(b)
		if err := c.Set(Item{Key: key, Value: b}); err != nil {
			t.Fatal(err)
		}
		item, err := c.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(item.Value, b) {
			t.Fatal("set get not equal")
		}
		if err := c.Del(key); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Get(key); err != ErrNotFound {
			t.Fatal("should not found")
		}
	}
	m := c.GetMetrics()

	k := int64(n)
	if m.GetTotal != 2*k || m.DelTotal != k || m.SetTotal != k || m.GetMisses != k || m.GetHits != k {
		t.Fatal("metrics err", m)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
