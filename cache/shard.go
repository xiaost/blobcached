package cache

import (
	"hash/crc32"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

const (
	indexSubfix = ".idx"
	dataSubfix  = ".dat"
)

type Shard struct {
	mu    sync.RWMutex
	index *CacheIndex
	data  *CacheData

	options ShardOptions

	stats   CacheStats
	metrics CacheMetrics
	exit    chan struct{}
}

type ShardOptions struct {
	Size      int64
	TTL       int64
	Allocator Allocator
	DisableGC bool
}

var DefualtShardOptions = ShardOptions{
	Size:      MinShardSize,
	TTL:       0,
	Allocator: NewAllocatorPool(4096),
}

func LoadCacheShard(fn string, options *ShardOptions) (*Shard, error) {
	if options == nil {
		options = &ShardOptions{}
		*options = DefualtShardOptions
	}
	if options.Size <= 0 {
		options.Size = MinShardSize
	}
	if options.Allocator == nil {
		options.Allocator = NewAllocatorPool(4096)
	}

	var err error
	s := Shard{options: *options}
	s.index, err = LoadCacheIndex(fn+indexSubfix, options.Size)
	if err != nil {
		return nil, errors.Wrap(err, "LoadIndex")
	}
	s.data, err = LoadCacheData(fn+dataSubfix, options.Size)
	if err != nil {
		return nil, errors.Wrap(err, "LoadData")
	}

	s.stats.Keys, err = s.index.GetKeys()
	if err != nil {
		return nil, err
	}
	var st gcstat // sample 10000 keys for stats.Bytes
	if err := s.scanKeysForGC(10000, &st); err != nil {
		return nil, err
	}
	s.stats.Bytes = uint64(float64(s.stats.Keys) * float64(st.ActiveBytes) / float64(st.Active))
	s.stats.LastUpdate = time.Now().Unix()

	s.exit = make(chan struct{})

	if !options.DisableGC {
		go s.GCLoop()
	}
	return &s, nil
}

func (s *Shard) Close() error {
	close(s.exit)
	err1 := s.index.Close()
	err2 := s.data.Close()
	if err1 != nil {
		return errors.Wrap(err1, "close index")
	}
	if err2 != nil {
		return errors.Wrap(err1, "close data")
	}
	return nil
}

func (s *Shard) GetMetrics() CacheMetrics {
	var m CacheMetrics
	m.GetTotal = atomic.LoadInt64(&s.metrics.GetTotal)
	m.GetHits = atomic.LoadInt64(&s.metrics.GetHits)
	m.GetMisses = atomic.LoadInt64(&s.metrics.GetMisses)
	m.GetExpired = atomic.LoadInt64(&s.metrics.GetExpired)
	m.SetTotal = atomic.LoadInt64(&s.metrics.SetTotal)
	m.DelTotal = atomic.LoadInt64(&s.metrics.DelTotal)
	m.Expired = atomic.LoadInt64(&s.metrics.Expired)
	m.Evicted = atomic.LoadInt64(&s.metrics.Evicted)
	m.EvictedAge = atomic.LoadInt64(&s.metrics.EvictedAge)
	return m
}

func (s *Shard) GetStats() CacheStats {
	var st CacheStats
	st.Keys = atomic.LoadUint64(&s.stats.Keys)
	st.Bytes = atomic.LoadUint64(&s.stats.Bytes)
	st.LastUpdate = atomic.LoadInt64(&s.stats.LastUpdate)
	return st
}

func (s *Shard) Set(ci *Item) error {
	atomic.AddInt64(&s.metrics.SetTotal, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	ii, err := s.index.Reserve(int32(len(ci.Value)))
	if err != nil {
		return errors.Wrap(err, "reserve index")
	}
	ii.TTL = ci.TTL
	ii.Flags = ci.Flags
	ii.Crc32 = crc32.ChecksumIEEE(ci.Value)
	if err := s.data.Write(ii.Offset, ci.Value); err != nil {
		return errors.Wrap(err, "write data")
	}
	if err := s.index.Set(ci.Key, ii); err != nil {
		return errors.Wrap(err, "update index")
	}
	return nil
}

func (s *Shard) Get(key string) (*Item, error) {
	atomic.AddInt64(&s.metrics.GetTotal, 1)
	s.mu.RLock()
	defer s.mu.RUnlock()
	ii, err := s.index.Get(key)
	if err != nil {
		if err == ErrNotFound {
			atomic.AddInt64(&s.metrics.GetMisses, 1)
		}
		return nil, err
	}

	age := time.Now().Unix() - ii.Timestamp
	if (s.options.TTL > 0 && age >= s.options.TTL) || (ii.TTL > 0 && age > int64(ii.TTL)) {
		atomic.AddInt64(&s.metrics.GetMisses, 1)
		atomic.AddInt64(&s.metrics.GetExpired, 1)
		atomic.AddInt64(&s.metrics.Expired, 1)
		s.index.Del(key)
		return nil, ErrNotFound
	}

	ci := s.options.Allocator.Alloc(int(ii.ValueSize))
	ci.Key = key
	ci.Timestamp = ii.Timestamp
	ci.TTL = ii.TTL
	ci.Flags = ii.Flags

	err = s.data.Read(ii.Offset, ci.Value)
	if err == ErrOutOfRange {
		err = ErrNotFound // data size changed?
	}
	if err != nil {
		if err == ErrNotFound {
			atomic.AddInt64(&s.metrics.GetMisses, 1)
		}
		ci.Free()
		return nil, err
	}
	if ii.Crc32 != 0 && ii.Crc32 != crc32.ChecksumIEEE(ci.Value) {
		ci.Free()
		return nil, ErrValueCrc
	}
	atomic.AddInt64(&s.metrics.GetHits, 1)
	return ci, nil
}

func (s *Shard) Del(key string) error {
	atomic.AddInt64(&s.metrics.DelTotal, 1)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Del(key)
}

type gcstat struct {
	Scanned     uint64
	Purged      uint64
	Active      uint64
	ActiveBytes uint64

	LastKey    string
	LastFinish time.Time
}

func timeSub(t1, t2 time.Time, round time.Duration) time.Duration {
	t1 = t1.Round(round)
	t2 = t2.Round(round)
	return t1.Sub(t2)
}

func (s *Shard) GCLoop() {
	const scanItemsPerRound = 100
	const sleepTimePerRound = 50 * time.Millisecond // ~ scan 2k items per second

	var st gcstat

	st.LastFinish = time.Now()

	for {
		select {
		case <-s.exit:
			return
		case <-time.After(sleepTimePerRound):
		}
		n := st.Scanned
		err := s.scanKeysForGC(scanItemsPerRound, &st)
		if err != nil {
			log.Printf("Iter keys err: %s", err)
			continue
		}
		newScanned := st.Scanned - n
		if newScanned >= scanItemsPerRound {
			continue // scanning
		}
		// newScanned < scanItemsPerRound, end of index
		now := time.Now()
		cost := timeSub(now, st.LastFinish, time.Millisecond)
		log.Printf("shard[%p] gc scanned:%d purged:%d keys cost %v",
			s, st.Scanned, st.Purged, cost)

		// save stats to CacheStats
		atomic.StoreUint64(&s.stats.Keys, st.Active)
		atomic.StoreUint64(&s.stats.Bytes, st.ActiveBytes)
		atomic.StoreInt64(&s.stats.LastUpdate, now.Unix())

		st.Scanned = 0
		st.Purged = 0
		st.Active = 0
		st.ActiveBytes = 0
		st.LastKey = ""

		if cost < time.Minute { // rate limit
			select {
			case <-s.exit:
				return
			case <-time.After(time.Minute - cost):
			}
		}
		st.LastFinish = time.Now()
	}
}

func (s *Shard) scanKeysForGC(maxIter int, st *gcstat) error {
	now := time.Now().Unix()
	meta := s.index.GetIndexMeta()
	var pendingDeletes []string
	err := s.index.Iter(st.LastKey, maxIter, func(key string, ii IndexItem) error {
		st.Scanned += 1
		st.LastKey = key
		age := time.Now().Unix() - ii.Timestamp
		if (s.options.TTL > 0 && age >= s.options.TTL) || (ii.TTL > 0 && age > int64(ii.TTL)) {
			st.Purged += 1
			atomic.AddInt64(&s.metrics.Expired, 1)
			pendingDeletes = append(pendingDeletes, key)
			return nil
		}
		if !meta.IsValidate(ii) { // the item may overwritten by other keys
			st.Purged += 1
			atomic.AddInt64(&s.metrics.Evicted, 1)
			atomic.StoreInt64(&s.metrics.EvictedAge, now-ii.Timestamp)
			pendingDeletes = append(pendingDeletes, key)
			return nil
		}
		st.Active += 1
		st.ActiveBytes += uint64(int64(len(key)) + ii.TotalSize())
		return nil
	})
	err2 := s.index.Dels(pendingDeletes)
	if err == nil {
		err = err2
	}
	return err
}
