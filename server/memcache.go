package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/xiaost/blobcached/cache"
	"github.com/xiaost/blobcached/protocol/memcache"
)

var errNotSupportedCommand = errors.New("not supported command")

type ServerMetrics struct {
	BytesRead        uint64 // Total number of bytes read by this server
	BytesWritten     uint64 // Total number of bytes sent by this server
	CurrConnections  int64  // Number of active connections
	TotalConnections uint64 // Total number of connections opened since the server started running
}

type MemcacheServer struct {
	l         net.Listener
	cache     Cache
	allocator Allocator
	metrics   ServerMetrics

	startTime time.Time
}

func NewMemcacheServer(l net.Listener, cache Cache, allocator Allocator) *MemcacheServer {
	return &MemcacheServer{l: l, cache: cache, allocator: allocator}
}

func (s *MemcacheServer) Serv() error {
	s.startTime = time.Now()
	log.Printf("memcache server listening on %s", s.l.Addr())
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return err
		}
		if tcpconn, ok := conn.(*net.TCPConn); ok {
			tcpconn.SetKeepAlive(true)
			tcpconn.SetKeepAlivePeriod(30 * time.Second)
		}
		atomic.AddUint64(&s.metrics.TotalConnections, 1)
		go func(conn net.Conn) {
			atomic.AddInt64(&s.metrics.CurrConnections, 1)

			s.Handle(conn)
			conn.Close()

			atomic.AddInt64(&s.metrics.CurrConnections, -1)
		}(conn)
	}
}

func (s *MemcacheServer) Handle(conn net.Conn) {
	const maxReadPerRequest = 2 * cache.MaxValueSize
	r := &io.LimitedReader{R: conn, N: maxReadPerRequest}
	w := &WriterCounter{conn, 0}
	var rbuf *bufio.Reader
	for {
		atomic.AddUint64(&s.metrics.BytesRead, uint64(maxReadPerRequest-r.N))
		atomic.AddUint64(&s.metrics.BytesWritten, uint64(w.N))

		r.N = maxReadPerRequest
		w.N = 0

		conn.SetDeadline(time.Now().Add(48 * time.Hour))

		if rbuf == nil {
			rbuf = bufio.NewReader(r)
		}
		b, err := rbuf.ReadSlice('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("read %s err: %s", conn.RemoteAddr(), err)
			}
			return
		}
		advance, cmdinfo, err := memcache.ParseCommand(b)
		if err != nil {
			log.Printf("parse %s command err: %s", conn.RemoteAddr(), err)
			w.Write(memcache.MakeRspClientErr(err))
			return
		}
		if advance != len(b) {
			panic("advance != len(b)")
		}

		// avoid blocking on reading data block or writing rsp
		conn.SetDeadline(time.Now().Add(60 * time.Second))

		var ww io.Writer = w
		if cmdinfo.NoReply {
			ww = ioutil.Discard
		}

		switch cmdinfo.Cmd {
		case "get", "gets":
			// some clients always use "gets" instead of "get"
			// we implement "gets" with fake cas uniq
			err = s.HandleGet(ww, cmdinfo)
		case "set":
			err = s.HandleSet(ww, rbuf, cmdinfo)
		case "delete":
			err = s.HandleDel(ww, cmdinfo)
		case "touch":
			err = s.HandleTouch(ww, cmdinfo)
		case "stats":
			err = s.HandleStats(ww)
		default:
			ww.Write(memcache.MakeRspServerErr(errNotSupportedCommand))
			return
		}

		if err != nil {
			log.Printf("client %s process err: %s", conn.RemoteAddr(), err)
			return
		}

		if advance > (4<<10) && rbuf.Buffered() == 0 {
			// if parse cmd use memory > 4kb, free Reader to avoid memory issue
			// it is safe to free it if buffered nothing
			rbuf = nil
		}
	}
}

func (s *MemcacheServer) HandleSet(w io.Writer, r *bufio.Reader, cmdinfo *memcache.CommandInfo) error {
	if cmdinfo.PayloadLen > cache.MaxValueSize-4096 {
		w.Write(memcache.MakeRspClientErr(cache.ErrValueSize))
		return cache.ErrValueSize
	}

	b := s.allocator.Malloc(int(cmdinfo.PayloadLen) + 2) // including \r\n
	defer s.allocator.Free(b)

	_, err := io.ReadFull(r, b)
	if err != nil {
		return err
	}
	value := b[:len(b)-2] // remove \r\n
	item := cache.Item{Key: cmdinfo.Key, Value: value, Flags: cmdinfo.Flags}

	/* https://github.com/memcached/memcached/blob/master/doc/protocol.txt

	Expiration times
	----------------

	Some commands involve a client sending some kind of expiration time
	(relative to an item or to an operation requested by the client) to
	the server. In all such cases, the actual value sent may either be
	Unix time (number of seconds since January 1, 1970, as a 32-bit
	value), or a number of seconds starting from current time. In the
	latter case, this number of seconds may not exceed 60*60*24*30 (number
	of seconds in 30 days); if the number sent by a client is larger than
	that, the server will consider it to be real Unix time value rather
	than an offset from current time.

	*/
	if cmdinfo.Exptime <= 30*86400 {
		item.TTL = cmdinfo.Exptime
	} else {
		now := time.Now().Unix()
		if now >= int64(cmdinfo.Exptime) {
			_, err = w.Write(memcache.RspStored)
			return err
		} else {
			item.TTL = uint32(int64(cmdinfo.Exptime) - now)
		}
	}

	if err := s.cache.Set(item); err != nil {
		w.Write(memcache.MakeRspServerErr(err))
		return err
	}
	_, err = w.Write(memcache.RspStored)
	return err
}

func (s *MemcacheServer) HandleGet(w io.Writer, cmdinfo *memcache.CommandInfo) error {
	var prepend string
	for _, k := range cmdinfo.Keys {
		item, err := s.cache.Get(k)
		if err == cache.ErrNotFound {
			continue
		}
		if err != nil {
			log.Printf("get key %s err: %s", k, err)
			continue
		}
		// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
		// <data block>\r\n
		if cmdinfo.Cmd == "gets" {
			fmt.Fprintf(w, "%sVALUE %s %d %d %d\r\n", prepend, k, item.Flags, len(item.Value), 0)
		} else {
			fmt.Fprintf(w, "%sVALUE %s %d %d\r\n", prepend, k, item.Flags, len(item.Value))
		}
		w.Write(item.Value)
		item.Free()
		prepend = "\r\n" // reduce len(cmdinfo.Keys) times w.Write("\r\n")
	}
	_, err := w.Write([]byte(prepend + "END\r\n"))
	return err
}

func (s *MemcacheServer) HandleTouch(w io.Writer, cmdinfo *memcache.CommandInfo) error {
	item, err := s.cache.Get(cmdinfo.Key)
	if err != nil {
		if err == cache.ErrNotFound {
			_, err = w.Write(memcache.RspNotFound)
		} else {
			_, err = w.Write(memcache.MakeRspServerErr(err))
		}
		return err
	}
	defer item.Free()

	if cmdinfo.Exptime <= 30*86400 {
		item.TTL = cmdinfo.Exptime
	} else {
		now := time.Now().Unix()
		if now >= int64(cmdinfo.Exptime) { // already expired
			w.Write(memcache.RspTouched)
			return s.cache.Del(cmdinfo.Key)
		} else {
			item.TTL = uint32(int64(cmdinfo.Exptime) - now)
		}
	}
	if err := s.cache.Set(item); err != nil {
		w.Write(memcache.MakeRspServerErr(err))
		return err
	}
	_, err = w.Write(memcache.RspTouched)
	return err
}

func (s *MemcacheServer) HandleDel(w io.Writer, cmdinfo *memcache.CommandInfo) error {
	err := s.cache.Del(cmdinfo.Key)
	if err != nil {
		w.Write(memcache.MakeRspServerErr(err))
		return nil
	}
	_, err = w.Write(memcache.RspDeleted)
	return err
}

func (s *MemcacheServer) HandleStats(w io.Writer) error {
	var buf bytes.Buffer
	writeStat := func(name string, v interface{}) {
		fmt.Fprintf(&buf, "STAT %s %v\r\n", name, v)
	}

	now := time.Now()
	writeStat("pid", os.Getpid())
	writeStat("uptime", int64(now.Sub(s.startTime).Seconds()))
	writeStat("time", now.Unix())
	writeStat("version", Version+"(blobcached)")

	// options
	options := s.cache.GetOptions()
	writeStat("limit_maxbytes", options.Size)

	// server metrics
	writeStat("curr_connections", atomic.LoadInt64(&s.metrics.CurrConnections))
	writeStat("total_connections", atomic.LoadUint64(&s.metrics.TotalConnections))
	writeStat("bytes_read", atomic.LoadUint64(&s.metrics.BytesRead))
	writeStat("bytes_written", atomic.LoadUint64(&s.metrics.BytesWritten))

	// cache stats
	stats := s.cache.GetStats()
	writeStat("curr_items", stats.Keys)
	writeStat("bytes", stats.Bytes)

	// cache metrics
	metrics := s.cache.GetMetrics()
	writeStat("cmd_get", metrics.GetTotal)
	writeStat("cmd_set", metrics.SetTotal)
	writeStat("get_hits", metrics.GetHits)
	writeStat("get_misses", metrics.GetMisses)
	writeStat("get_expired", metrics.GetExpired)
	writeStat("reclaimed", metrics.Expired)
	writeStat("evictions", metrics.Evicted)
	writeStat("last_evicted_age", metrics.EvictedAge)

	buf.Write(memcache.RspEnd)
	_, err := w.Write(buf.Bytes())
	return err
}
