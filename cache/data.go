package cache

import (
	"os"
	"sync"
	"syscall"

	"github.com/pkg/errors"
)

var (
	ErrOutOfRange = errors.New("out of data range")
)

type CacheData struct {
	mu sync.RWMutex
	f  *os.File
	sz int64
}

func LoadCacheData(fn string, sz int64) (*CacheData, error) {
	f, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, errors.Wrap(err, "os.OpenFile")
	}
	if err := f.Truncate(sz); err != nil {
		return nil, errors.Wrap(err, "truncate data")
	}
	// prealloc blocks in disk, ok if it have any errors
	syscall.Fallocate(int(f.Fd()), 0, 0, sz)
	return &CacheData{f: f, sz: sz}, nil
}

func (d *CacheData) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.f.Close()
}

func (d *CacheData) Size() int64 {
	return d.sz
}

func (d *CacheData) Read(offset int64, b []byte) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if offset+int64(len(b)) > d.sz {
		return ErrOutOfRange
	}
	// ReadAt always returns a non-nil error when n < len(b)
	_, err := d.f.ReadAt(b, offset)
	return err
}

func (d *CacheData) Write(offset int64, b []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if offset+int64(len(b)) > d.sz {
		return ErrOutOfRange
	}
	_, err := d.f.WriteAt(b, offset)
	return err
}
