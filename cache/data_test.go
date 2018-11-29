package cache

import (
	"bytes"
	"crypto/rand"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestCacheData(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_cachedata")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fn := filepath.Join(dir, "data")
	c, err := LoadCacheData(fn, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if c.Size() != 1024 {
		t.Fatal("size err")
	}
	b := make([]byte, 8)
	rand.Read(b)
	if err := c.Write(1000, b); err != nil {
		t.Fatal(err)
	}

	bb := make([]byte, 8)
	if err := c.Read(1000, bb); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bb, b) {
		t.Fatal("bytes not equal")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCacheDataReadErr(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_cachedata")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fn := filepath.Join(dir, "data")
	c, err := LoadCacheData(fn, 1024)
	if err != nil {
		t.Fatal(err)
	}

	bb := make([]byte, 8)
	if err := c.Read(1024, bb); err != ErrOutOfRange {
		t.Fatal("should out of range")
	}
	if err := c.Read(1023, bb); err != ErrOutOfRange {
		t.Fatal("should out of range")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestCacheDataWriteErr(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_cachedata")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fn := filepath.Join(dir, "data")
	c, err := LoadCacheData(fn, 1024)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 8)
	rand.Read(b)
	if err := c.Write(1017, b); err != ErrOutOfRange {
		t.Fatal("should out of range")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
