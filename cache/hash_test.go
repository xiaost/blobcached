package cache

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestConsistentHash(t *testing.T) {
	n1 := 5
	n2 := 16
	c1 := NewConsistentHashTable(n1)
	c2 := NewConsistentHashTable(n2)

	m1 := make(map[int]int)
	total := 100000
	match := 0
	for i := 0; i < total; i++ {
		s := strconv.FormatInt(rand.Int63(), 36) + strconv.FormatInt(rand.Int63(), 36)
		h1 := c1.Get(s)
		h2 := c2.Get(s)
		if h1 == h2 {
			match += 1
		}
		m1[h1] += 1
	}

	for i := 0; i < n1; i++ {
		diff := 0
		for j := 0; j < n1; j++ {
			diff += m1[i] - m1[j]
		}
		if diff > int(float32(total/n1)*0.1) {
			t.Fatal("total", total, "slot", i, "num", m1[i], "diff err")
		}
	}

	matchRate := float32(match) / float32(total)
	expectRate := float32(n1) / float32(n2)
	if matchRate < expectRate*0.9 || matchRate > expectRate*1.1 {
		t.Fatal("match rate err", matchRate, "expect", expectRate)
	}
}
