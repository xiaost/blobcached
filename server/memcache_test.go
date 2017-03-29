package server

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
)

func getstat(lines string, name string, value interface{}) {
	for _, line := range strings.Split(string(lines), "\r\n") {
		if !strings.Contains(line, name) {
			continue
		}
		fmt.Sscanf(line, "STAT "+name+" %v", value)
		return
	}
}

func TestMemcacheServer(t *testing.T) {
	l, err := net.ListenTCP("tcp", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	s := NewMemcacheServer(l, NewInMemoryCache(), NewDebugAllocator())
	go s.Serv()

	mc := memcache.New(l.Addr().String())
	if err := mc.Set(&memcache.Item{Key: "k1", Value: []byte("v1")}); err != nil {
		t.Fatal(err)
	}
	if err := mc.Set(&memcache.Item{Key: "k2", Value: []byte("v2")}); err != nil {
		t.Fatal(err)
	}

	if err := mc.Touch("k2", 3); err != nil {
		t.Fatal(err)
	}

	if err := mc.Touch("k-notfound", 4); err != memcache.ErrCacheMiss {
		t.Fatal("should not found")
	}

	m, err := mc.GetMulti([]string{"k1", "k2", "k3"})
	if err != nil {
		t.Fatal(err)
	}

	if g, e := len(m), 2; g != e {
		t.Fatalf("GetMulti: got len(map) = %d, want = %d", g, e)
	}
	if _, ok := m["k1"]; !ok {
		t.Fatal("GetMulti: didn't get key 'k1'")
	}
	if _, ok := m["k2"]; !ok {
		t.Fatal("GetMulti: didn't get key 'k2'")
	}
	if g, e := string(m["k1"].Value), "v1"; g != e {
		t.Errorf("GetMulti: k1: got %q, want %q", g, e)
	}
	if g, e := string(m["k2"].Value), "v2"; g != e {
		t.Errorf("GetMulti: k2: got %q, want %q", g, e)
	}

	if err := mc.Delete("k1"); err != nil {
		t.Fatal(err)
	}

	if _, err := mc.Get("k1"); err != memcache.ErrCacheMiss {
		t.Fatal("should cache miss")
	}

	var buf bytes.Buffer
	s.HandleStats(&buf)
	t.Log("stat\n", buf.String())

	var n int
	getstat(buf.String(), "curr_connections", &n)
	if n != 1 {
		t.Fatal("stat err:", buf.String())
	}

}
