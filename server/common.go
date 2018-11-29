package server

import (
	"io"

	"github.com/xiaost/blobcached/cache"
)

const Version = "1.0"

type Cache interface {
	Set(item *cache.Item) error
	Get(key string) (*cache.Item, error)
	Del(key string) error
	GetOptions() cache.CacheOptions
	GetMetrics() cache.CacheMetrics
	GetMetricsByShards() []cache.CacheMetrics
	GetStats() cache.CacheStats
	GetStatsByShards() []cache.CacheStats
}

type WriterCounter struct {
	W io.Writer
	N int64
}

func (w *WriterCounter) Write(p []byte) (int, error) {
	n, err := w.W.Write(p)
	if n > 0 {
		w.N += int64(n)
	}
	return n, err
}
