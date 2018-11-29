package cache

import (
	"bytes"
	"crypto/rand"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCacheMetrics(t *testing.T) {
	m1 := CacheMetrics{}
	m2 := CacheMetrics{1, 1, 1, 1, 1, 1, 1, 1, 1}
	m1.Add(m2)
	if m1 != m2 {
		t.Fatal("not equal", m1, m2)
	}
}

func TestShardSetGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_cachedata")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	s, err := LoadCacheShard(filepath.Join(dir, "shard"), &ShardOptions{Size: 1024, TTL: 1})
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 300)
	rand.Read(b)

	_, err = s.Get("k1")
	if err != ErrNotFound {
		t.Fatal("should not found")
	}
	if err := s.Set(&Item{Key: "k1", Value: b}); err != nil {
		t.Fatal(err)
	}
	ci, err := s.Get("k1")
	if err != nil {
		t.Fatal(err)
	}
	if ci.Key != "k1" {
		t.Fatal("key not equal")
	}
	if !bytes.Equal(ci.Value, b) {
		t.Fatal("set get not equal")
	}

	var st gcstat

	if testing.Short() {
		goto EndOfTest
	}

	s.Set(&Item{Key: "k2", Value: b})
	s.Set(&Item{Key: "k3", Value: b})
	s.Set(&Item{Key: "k4", Value: b})

	st.LastKey = ""
	s.scanKeysForGC(100, &st)

	time.Sleep(1020 * time.Millisecond)

	st.LastKey = ""
	s.scanKeysForGC(100, &st)

	for _, key := range []string{"k1", "k2", "k3", "k4"} {
		if _, err := s.Get(key); err != ErrNotFound {
			t.Fatal("should not found")
		}
	}
	{
		m1 := s.GetMetrics()
		m2 := CacheMetrics{6, 1, 5, 0, 4, 0, 3, 1, 0}
		if m1 != m2 {
			t.Logf("\nget %+v\nexpect %+v", m1, m2)
			t.Fatal("metrics err")
		}
	}

EndOfTest:
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
