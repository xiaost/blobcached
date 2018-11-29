package cache

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestCacheIndexReserve(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_cacheindex")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fn := filepath.Join(dir, "index")
	c, err := LoadCacheIndex(fn, 1024)
	if err != nil {
		t.Fatal(err)
	}
	item, err := c.Reserve(400)
	if err != nil {
		t.Fatal(err)
	}
	if item.Offset != 0 || item.Term != 0 {
		t.Fatal("item err", item)
	}
	item1, err := c.Reserve(400)
	if item1.Offset != item.Offset+400 {
		t.Fatal("item err", item1)
	}
	item2, err := c.Reserve(400)
	if item2.Offset != 0 || item2.Term != 1 {
		t.Fatal("item err", item2)
	}
	meta := c.GetIndexMeta()
	if meta.Term != 1 || meta.Head != 400 {
		t.Fatal("meta err", meta)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCacheIndexGetSet(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_cacheindex")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fn := filepath.Join(dir, "index")
	c, err := LoadCacheIndex(fn, 1024)
	if err != nil {
		t.Fatal(err)
	}
	var item1, item2 *IndexItem
	item1, _ = c.Reserve(400)
	if err := c.Set("k1", item1); err != nil {
		t.Fatal(err)
	}
	item2, _ = c.Reserve(400)
	if err := c.Set("k2", item2); err != nil {
		t.Fatal(err)
	}
	t1, err := c.Get("k1")
	if err != nil {
		t.Fatal(err)
	}
	t2, err := c.Get("k2")
	if err != nil {
		t.Fatal(err)
	}
	if *t1 != *item1 && *t2 != *item2 {
		t.Fatal("get err", *t1, *t2)
	}
	c.Reserve(400) // overwrite item1
	_, err = c.Get("k1")
	if err != ErrNotFound {
		t.Fatal("should not found")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCacheIndexSizeChanged(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_cacheindex")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fn := filepath.Join(dir, "index")
	c, err := LoadCacheIndex(fn, 1024)
	if err != nil {
		t.Fatal(err)
	}
	var item *IndexItem
	item, _ = c.Reserve(300)
	c.Set("k1", item)
	item, _ = c.Reserve(300)
	c.Set("k2", item)
	item, _ = c.Reserve(300)
	c.Set("k3", item)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = LoadCacheIndex(fn, 500)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Get("k1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Get("k2")
	if err != ErrNotFound {
		t.Fatal("should not found")
	}
	_, err = c.Get("k3")
	if err != ErrNotFound {
		t.Fatal("should not found")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCacheIndexDel(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_cacheindex")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fn := filepath.Join(dir, "index")
	c, err := LoadCacheIndex(fn, 1024)
	if err != nil {
		t.Fatal(err)
	}
	var item *IndexItem
	item, _ = c.Reserve(300)
	c.Set("k1", item)
	item, _ = c.Reserve(300)
	c.Set("k2", item)
	item, _ = c.Reserve(300)
	c.Set("k3", item)
	if err := c.Del("k1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Dels([]string{"k2", "k3"}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"k1", "k2", "k3"} {
		if _, err := c.Get(k); err != ErrNotFound {
			t.Fatal(k, "should not found")
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCacheIndexIter(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_cacheindex")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	fn := filepath.Join(dir, "index")
	c, err := LoadCacheIndex(fn, 1024)
	if err != nil {
		t.Fatal(err)
	}
	item1, _ := c.Reserve(300)
	c.Set("k1", item1)
	item2, _ := c.Reserve(300)
	c.Set("k2", item2)

	iterkeys := 0

	err = c.Iter("", 100, func(key string, item IndexItem) error {
		iterkeys += 1
		if key == "k1" && item != *item1 {
			t.Fatal(key, "item err", item)
		}
		if key == "k2" && item != *item2 {
			t.Fatal(key, "item err", item)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if iterkeys != 2 {
		t.Fatal("iter keys", iterkeys)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
