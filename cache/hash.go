package cache

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
)

const (
	vhashnodes = 1000
)

type consistentNode struct {
	pos uint64
	v   int
}

type consistentNodes []consistentNode

func (n consistentNodes) Len() int           { return len(n) }
func (n consistentNodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n consistentNodes) Less(i, j int) bool { return n[i].pos < n[j].pos }

// a simplify consistent hash impelment without weight
type ConsistentHash struct {
	nodes consistentNodes
}

// from https://golang.org/src/hash/fnv/fnv.go
// Equals to:
// h := fnv.New64a()
// h.Write(b)
// h.Sum64
func dohash(b []byte) uint64 {
	hash := uint64(14695981039346656037)
	for _, c := range b {
		hash ^= uint64(c)
		hash *= 1099511628211
	}
	return hash
}

// NewConsistentHashTable creates a ConsistentHash with value[0, n)
func NewConsistentHashTable(n int) ConsistentHash {
	var h ConsistentHash
	h.nodes = make(consistentNodes, 0, n*vhashnodes)
	for i := 0; i < n; i++ {
		for j := 0; j < vhashnodes; j++ {
			// we use md5 distribute vnodes
			b := md5.Sum([]byte(fmt.Sprintf("hash-%d-%d", i, j)))
			h.nodes = append(h.nodes, consistentNode{binary.BigEndian.Uint64(b[:]), i})
		}
	}
	sort.Sort(h.nodes)
	return h
}

func (c *ConsistentHash) search(k uint64) int {
	i := sort.Search(len(c.nodes), func(i int) bool {
		return c.nodes[i].pos >= k
	})
	if i == len(c.nodes) {
		return c.nodes[0].v
	}
	return c.nodes[i].v
}

func (c *ConsistentHash) Get(key string) int {
	h := dohash([]byte(key))
	return c.search(h)
}
