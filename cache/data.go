package cache

import (
	"encoding/binary"
	"os"
	"sync"

	mmap "github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
)

var (
	ErrOutOfRange = errors.New("out of data range")
	ErrHeader     = errors.New("header err")
	ErrShortRead  = errors.New("short read")

	datamagic   = uint64(20126241245322)
	datahdrsize = int64(16)
	maxsize     = int64(1 << 30) // 1GB
)

type dataheader struct {
	magic uint64
	size  int64
}

func (h *dataheader) Unmarshal(b []byte) error {
	if len(b) < int(datahdrsize) {
		return ErrOutOfRange
	}
	h.magic = binary.BigEndian.Uint64(b[:8])
	h.size = int64(binary.BigEndian.Uint64(b[8:16]))
	return nil
}

func (h *dataheader) MarshalTo(b []byte) {
	binary.BigEndian.PutUint64(b[:8], h.magic)
	binary.BigEndian.PutUint64(b[8:16], uint64(h.size))
}

type CacheData struct {
	mu   sync.RWMutex
	data mmap.MMap
}

func LoadCacheData(fn string, sz int64) (*CacheData, error) {
	f, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, errors.Wrap(err, "os.OpenFile")
	}
	if err := f.Truncate(sz); err != nil {
		return nil, errors.Wrap(err, "truncate data")
	}
	defer f.Close()
	d, err := mmap.MapRegion(f, int(sz), mmap.RDWR, 0, 0)
	if err != nil {
		return nil, errors.Wrap(err, "mmap.MapRegion")
	}
	if len(d) != int(sz) {
		panic("mmap bytes len err")
	}
	return &CacheData{data: d}, nil
}

func (d *CacheData) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.data.Unmap()
}

func (d *CacheData) Size() int64 {
	return int64(len(d.data))
}

func (d *CacheData) Read(offset int64, b []byte) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if int(offset) >= len(d.data) {
		return ErrOutOfRange
	}
	var hdr dataheader
	err := hdr.Unmarshal(d.data[offset:])
	if err != nil {
		return err
	}
	if hdr.magic != datamagic || hdr.size != int64(len(b)) || hdr.size > MaxValueSize {
		return ErrHeader
	}
	offset += datahdrsize
	if int(offset+int64(hdr.size)) > len(d.data) {
		return ErrOutOfRange
	}
	copy(b, d.data[offset:offset+hdr.size])
	return nil
}

func (d *CacheData) Write(offset int64, b []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	sz := int64(len(b))
	voffset := offset + datahdrsize
	if int(voffset+sz) > len(d.data) {
		return ErrOutOfRange
	}
	hdr := dataheader{magic: datamagic, size: sz}
	hdr.MarshalTo(d.data[offset : offset+datahdrsize])
	copy(d.data[voffset:voffset+sz], b)
	return nil
}
