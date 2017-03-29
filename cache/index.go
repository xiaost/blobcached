package cache

import (
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

type CacheIndex struct {
	db *bolt.DB

	mu   sync.RWMutex
	meta IndexMeta
}

var (
	indexMetaBucket = []byte("meta")
	indexDataBucket = []byte("data")
	indexMetaKey    = []byte("indexmeta")
)

func LoadCacheIndex(fn string, datasize int64) (*CacheIndex, error) {
	var err error
	var index CacheIndex
	index.db, err = bolt.Open(fn, 0600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		return nil, errors.Wrap(err, "bolt.Open")
	}
	index.db.NoSync = true // for improve performance
	err = index.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(indexMetaBucket)
		if err != nil {
			return err
		}
		v := bucket.Get(indexMetaKey)
		if v == nil {
			return nil
		}
		return index.meta.Unmarshal(v)
	})
	if err != nil {
		return nil, errors.Wrap(err, "bolt.Update")
	}
	meta := &index.meta
	if meta.DataSize != datasize {
		meta.DataSize = datasize
	}
	if meta.Head > datasize {
		meta.Head = 0
		meta.Term += 1 // datasize changed, move to next term
	}
	return &index, nil
}

func (i *CacheIndex) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.db.Close()
}

func (i *CacheIndex) Get(key string) (*IndexItem, error) {
	var item IndexItem
	err := i.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexDataBucket)
		if bucket == nil {
			return ErrNotFound
		}
		v := bucket.Get([]byte(key))
		if v == nil {
			return ErrNotFound
		}
		return item.Unmarshal(v)
	})
	if err != nil {
		return nil, err
	}
	meta := i.GetIndexMeta()
	if meta.IsValidate(item) {
		return &item, nil
	}
	return nil, ErrNotFound
}

func (i *CacheIndex) Reserve(size int32) (*IndexItem, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	meta := &i.meta
	if int64(size) > meta.DataSize {
		return nil, errors.New("not enough space")
	}
	if meta.Head+datahdrsize+int64(size) > meta.DataSize {
		meta.Head = 0
		meta.Term += 1
	}
	idx := &IndexItem{Term: meta.Term, Offset: meta.Head, ValueSize: size, Timestamp: time.Now().Unix()}
	meta.Head += datahdrsize + int64(size)

	err := i.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(indexMetaBucket)
		if err != nil {
			return err
		}
		b, err := i.meta.Marshal()
		if err != nil {
			return err
		}
		return bucket.Put(indexMetaKey, b)
	})
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *CacheIndex) Set(key string, item *IndexItem) error {
	return i.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(indexDataBucket)
		if err != nil {
			return err
		}
		b, _ := item.Marshal()
		return bucket.Put([]byte(key), b)
	})
}

func (i *CacheIndex) Del(key string) error {
	return i.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(indexDataBucket)
		if err != nil {
			return err
		}
		return bucket.Delete([]byte(key))
	})
}

func (i *CacheIndex) Dels(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	return i.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(indexDataBucket)
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := bucket.Delete([]byte(key)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (i *CacheIndex) GetIndexMeta() IndexMeta {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.meta
}

func (i *CacheIndex) Iter(lastkey string, maxIter int, f func(key string, item IndexItem) error) error {
	var item IndexItem
	err := i.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexDataBucket)
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()

		var k, v []byte

		if lastkey == "" {
			k, v = c.First()
		} else {
			k, v = c.Seek([]byte(lastkey))
			if string(k) == lastkey {
				k, v = c.Next()
			}
		}
		for k != nil && maxIter > 0 {
			maxIter -= 1
			if err := item.Unmarshal(v); err != nil {
				return err
			}
			if err := f(string(k), item); err != nil {
				return err
			}
			k, v = c.Next()
		}
		return nil
	})
	return err
}

func (i *CacheIndex) GetKeys() (n uint64, err error) {
	err = i.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexDataBucket)
		if bucket != nil {
			st := bucket.Stats()
			n = uint64(st.KeyN)
		}
		return nil
	})
	return
}

// extends index.pb.go >>>>>>>>>>>>>>>>>>>>>>

// IsValidate return if the item is validate
func (m IndexMeta) IsValidate(i IndexItem) bool {
	if i.Term > m.Term {
		return false
	}
	if i.Term == m.Term {
		return i.Offset < m.Head && i.Offset+int64(i.ValueSize) <= m.DataSize
	}
	// i.Term < m.Term
	if i.Term+1 != m.Term {
		return false
	}
	return m.Head <= i.Offset && i.Offset+int64(i.ValueSize) <= m.DataSize
}

// TotalSize returns bytes used including index & data of the item
func (i IndexItem) TotalSize() int64 {
	return int64(i.Size()) + datahdrsize + int64(i.ValueSize)
}
