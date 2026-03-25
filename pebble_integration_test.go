package s3vfs_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
)

// openPebble opens a pebble DB using our S3FS as the storage backend.
func openPebble(t *testing.T, dir string) *pebble.DB {
	t.Helper()
	fs := newTestFS(t)
	db, err := pebble.Open(dir, &pebble.Options{
		FS: fs,
	})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestPebbleSetGet writes a key and reads it back.
func TestPebbleSetGet(t *testing.T) {
	db := openPebble(t, "testdb")

	if err := db.Set([]byte("hello"), []byte("world"), pebble.Sync); err != nil {
		t.Fatal(err)
	}

	val, closer, err := db.Get([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()

	if string(val) != "world" {
		t.Fatalf("Get(hello) = %q, want %q", string(val), "world")
	}
}

// TestPebbleDelete writes a key then deletes it.
func TestPebbleDelete(t *testing.T) {
	db := openPebble(t, "testdb")

	db.Set([]byte("key"), []byte("val"), pebble.Sync)
	if err := db.Delete([]byte("key"), pebble.Sync); err != nil {
		t.Fatal(err)
	}

	_, _, err := db.Get([]byte("key"))
	if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestPebbleBatch writes multiple keys via a batch.
func TestPebbleBatch(t *testing.T) {
	db := openPebble(t, "testdb")

	b := db.NewBatch()
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		val := []byte(fmt.Sprintf("val%02d", i))
		b.Set(key, val, nil)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		want := fmt.Sprintf("val%02d", i)
		got, closer, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%s) = %q, want %q", key, string(got), want)
		}
		closer.Close()
	}
}

// TestPebbleIterator scans a range of keys using an iterator.
func TestPebbleIterator(t *testing.T) {
	db := openPebble(t, "testdb")

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		db.Set([]byte(k), []byte("v:"+k), pebble.Sync)
	}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("b"),
		UpperBound: []byte("e"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	var got []string
	for iter.First(); iter.Valid(); iter.Next() {
		got = append(got, string(iter.Key()))
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}

	want := []string{"banana", "cherry", "date"}
	if len(got) != len(want) {
		t.Fatalf("iterator returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("iter[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestPebbleReopenPersistence closes and reopens the DB, verifying data survives.
func TestPebbleReopenPersistence(t *testing.T) {
	fs := newTestFS(t)

	open := func() *pebble.DB {
		db, err := pebble.Open("persist-db", &pebble.Options{FS: fs})
		if err != nil {
			t.Fatalf("pebble.Open: %v", err)
		}
		return db
	}

	// Write data and close.
	db := open()
	db.Set([]byte("persistent"), []byte("yes"), pebble.Sync)
	db.Close()

	// Reopen and read back.
	db2 := open()
	defer db2.Close()

	val, closer, err := db2.Get([]byte("persistent"))
	if err != nil {
		t.Fatalf("after reopen, Get: %v", err)
	}
	defer closer.Close()

	if string(val) != "yes" {
		t.Fatalf("after reopen, Get = %q, want %q", string(val), "yes")
	}
}

// TestPebbleCompaction writes enough data to trigger compaction.
func TestPebbleCompaction(t *testing.T) {
	db := openPebble(t, "compact-db")

	// Write enough keys to trigger compaction.
	const n = 1000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("compactkey%06d", i))
		val := make([]byte, 256)
		for j := range val {
			val[j] = byte(i)
		}
		if err := db.Set(key, val, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// Force a compaction.
	if err := db.Compact([]byte("compactkey"), []byte("compactkeyz"), true); err != nil {
		t.Fatal(err)
	}

	// Spot-check a few keys survive compaction.
	for _, i := range []int{0, 499, 999} {
		key := []byte(fmt.Sprintf("compactkey%06d", i))
		_, closer, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%s) after compaction: %v", key, err)
		}
		closer.Close()
	}
}

// TestPebbleMerge uses pebble's Merge operation (append-style update).
func TestPebbleMerge(t *testing.T) {
	fs := newTestFS(t)
	db, err := pebble.Open("merge-db", &pebble.Options{
		FS:     fs,
		Merger: pebble.DefaultMerger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// DefaultMerger appends values with a null byte separator.
	db.Merge([]byte("log"), []byte("first"), pebble.Sync)
	db.Merge([]byte("log"), []byte("second"), pebble.Sync)

	val, closer, err := db.Get([]byte("log"))
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()

	if len(val) == 0 {
		t.Fatal("expected non-empty merged value")
	}
}

// TestPebbleDeleteRange deletes a range of keys at once.
func TestPebbleDeleteRange(t *testing.T) {
	db := openPebble(t, "delrange-db")

	for i := 0; i < 10; i++ {
		db.Set([]byte(fmt.Sprintf("r%02d", i)), []byte("v"), pebble.Sync)
	}

	if err := db.DeleteRange([]byte("r03"), []byte("r07"), pebble.Sync); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("r%02d", i))
		_, closer, err := db.Get(key)
		deleted := i >= 3 && i < 7
		if deleted && err != pebble.ErrNotFound {
			t.Errorf("key %s should be deleted, got err=%v", key, err)
		}
		if !deleted && err != nil {
			t.Errorf("key %s should exist, got err=%v", key, err)
		}
		if err == nil {
			closer.Close()
		}
	}
}
