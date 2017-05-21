package cache

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

var (
	ErrNotFound  = errors.New("key not found")
	ErrValueSize = errors.New("value size exceeded")
	ErrValueCrc  = errors.New("value checksum err")
)

const (
	MaxShards    = 128
	MaxValueSize = int64(128 << 20) // 128MB
	MinShardSize = MaxValueSize + 4096
)

type CacheMetrics struct {
	GetTotal   int64 // number of get request
	GetHits    int64 // number of items that hit from data
	GetMisses  int64 // number of items that not found
	GetExpired int64 // number of items that expired when get
	SetTotal   int64 // number of set request
	DelTotal   int64 // number of del request
	Expired    int64 // number of items that expired
	Evicted    int64 // number of items evicted
	EvictedAge int64 // min age of the last evicted item
}

func (m *CacheMetrics) Add(o CacheMetrics) {
	m.GetTotal += o.GetTotal
	m.GetHits += o.GetHits
	m.GetMisses += o.GetMisses
	m.GetExpired += o.GetExpired
	m.SetTotal += o.SetTotal
	m.DelTotal += o.DelTotal
	m.Expired += o.Expired
	m.Evicted += o.Evicted
	// use min age
	if m.EvictedAge <= 0 || (o.EvictedAge > 0 && o.EvictedAge < m.EvictedAge) {
		m.EvictedAge = o.EvictedAge
	}
}

type CacheStats struct {
	Keys       uint64 // number of keys
	Bytes      uint64 // bytes of keys that used
	LastUpdate int64  // stat time, the stat is async updated
}

func (st *CacheStats) Add(o CacheStats) {
	st.Keys += o.Keys
	st.Bytes += o.Bytes
	// use oldest time
	if o.LastUpdate < st.LastUpdate {
		st.LastUpdate = o.LastUpdate
	}
}

type Cache struct {
	hash   ConsistentHash
	shards []*Shard

	options CacheOptions
}

type Allocator interface {
	Malloc(n int) []byte
	Free([]byte)
}

type CacheOptions struct {
	ShardNum  int
	Size      int64
	TTL       int64
	Allocator Allocator
}

var DefualtCacheOptions = CacheOptions{
	ShardNum:  16,
	Size:      32 * MaxValueSize, // 32*128MB = 4GB
	TTL:       0,
	Allocator: NewAllocatorPool(),
}

func NewCache(path string, options *CacheOptions) (*Cache, error) {
	os.MkdirAll(path, 0700)
	if options == nil {
		options = &CacheOptions{}
		*options = DefualtCacheOptions
	}
	if options.Allocator == nil {
		options.Allocator = NewAllocatorPool()
	}
	if options.ShardNum > MaxShards {
		options.ShardNum = MaxShards
	}
	if options.Size/int64(options.ShardNum) < MinShardSize {
		options.ShardNum = int(options.Size / MinShardSize)
	}
	var err error
	cache := Cache{options: *options}
	cache.shards = make([]*Shard, options.ShardNum)
	for i := 0; i < MaxShards; i++ {
		fn := filepath.Join(path, fmt.Sprintf("shard.%03d", i))
		if i >= options.ShardNum { // rm unused files
			os.Remove(fn + indexSubfix)
			os.Remove(fn + dataSubfix)
			continue
		}
		sopts := &ShardOptions{
			Size:      options.Size / int64(options.ShardNum),
			TTL:       options.TTL,
			Allocator: options.Allocator,
		}
		cache.shards[i], err = LoadCacheShard(fn, sopts)
		if err != nil {
			return nil, errors.Wrap(err, "load cache shard")
		}
	}
	cache.hash = NewConsistentHashTable(len(cache.shards))
	return &cache, nil
}

func (c *Cache) Close() error {
	var err error
	for _, s := range c.shards {
		er := s.Close()
		if er != nil {
			err = er
		}
	}
	return err
}

func (c *Cache) getshard(key string) *Shard {
	return c.shards[c.hash.Get(key)]
}

type Item struct {
	Key       string
	Value     []byte
	Timestamp int64
	TTL       uint32
	Flags     uint32

	allocator Allocator
}

func (i *Item) Free() {
	if i.Value != nil && i.allocator != nil {
		i.allocator.Free(i.Value)
		i.allocator = nil
		i.Value = nil
	}
}

func (c *Cache) Set(item Item) error {
	if int64(len(item.Value)) > MaxValueSize {
		return ErrValueSize
	}
	s := c.getshard(item.Key)
	return s.Set(item)
}

func (c *Cache) Get(key string) (Item, error) {
	s := c.getshard(key)
	return s.Get(key)
}

func (c *Cache) Del(key string) error {
	s := c.getshard(key)
	return s.Del(key)
}

func (c *Cache) GetMetrics() CacheMetrics {
	var m CacheMetrics
	for _, s := range c.GetMetricsByShards() {
		m.Add(s)
	}
	return m
}

func (c *Cache) GetStats() CacheStats {
	var st CacheStats
	for _, s := range c.GetStatsByShards() {
		st.Add(s)
	}
	return st
}

func (c *Cache) GetMetricsByShards() []CacheMetrics {
	var ret = make([]CacheMetrics, len(c.shards), len(c.shards))
	for i, s := range c.shards {
		ret[i] = s.GetMetrics()
	}
	return ret
}

func (c *Cache) GetStatsByShards() []CacheStats {
	var ret = make([]CacheStats, len(c.shards), len(c.shards))
	for i, s := range c.shards {
		ret[i] = s.GetStats()
	}
	return ret
}

func (c *Cache) GetOptions() CacheOptions {
	return c.options
}
