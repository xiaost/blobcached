package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/xiaost/blobcached/cache"
	"github.com/xiaost/blobcached/server"
)

const VERSION = "0.1.1-dev"

func main() {
	var (
		bindAddr    string
		cachePath   string
		cacheSize   int64
		cacheShards int64
		cacheTTL    int64

		printVersion bool
	)

	flag.StringVar(&bindAddr,
		"addr", ":11211",
		"the addr that blobcached listen on.")

	flag.StringVar(&cachePath,
		"path", "cachedata",
		"the cache path used by blobcached to store items.")

	flag.Int64Var(&cacheSize,
		"size", int64(cache.DefualtCacheOptions.Size),
		"cache file size used by blobcached to store items. ")

	flag.Int64Var(&cacheShards,
		"shards", int64(cache.DefualtCacheOptions.ShardNum),
		"cache shards for performance purpose. max shards is 128.")

	flag.Int64Var(&cacheTTL,
		"ttl", 0,
		"the global ttl of cache items.")

	flag.BoolVar(&printVersion, "v", false,
		"print the version and exit")

	flag.Parse()

	if printVersion {
		fmt.Println("Blobcached", VERSION)
		return
	}

	l, err := net.Listen("tcp", bindAddr)
	if err != nil {
		log.Fatal(err)
	}

	// cacheSize: limit to [2*MaxValueSize, +)
	if cacheSize <= 2*cache.MaxValueSize {
		cacheSize = 2 * cache.MaxValueSize
		log.Printf("warn: cache size invaild. set to %d", cacheSize)
	}

	// cacheShards: limit to (0, cache.MaxShards] and per cache shard size >= cache.MaxValueSize
	if (cacheShards <= 0 || cacheShards > cache.MaxShards) ||
		(cacheSize/cacheShards < cache.MaxValueSize) {
		cacheShards = int64(cache.DefualtCacheOptions.ShardNum)
		if cacheSize/cacheShards < cache.MaxValueSize {
			cacheShards = cacheSize / cache.MaxValueSize
		}
		log.Printf("warn: cache shards invaild. set to %d", cacheShards)
	}

	if cacheTTL < 0 {
		cacheTTL = 0
	}

	allocator := cache.NewAllocatorPool()

	options := &cache.CacheOptions{
		ShardNum:  int(cacheShards),
		Size:      cacheSize,
		TTL:       cacheTTL,
		Allocator: allocator,
	}
	c, err := cache.NewCache(cachePath, options)
	if err != nil {
		log.Fatal(err)
	}
	s := server.NewMemcacheServer(l, c, allocator)
	log.Fatal(s.Serv())
}
