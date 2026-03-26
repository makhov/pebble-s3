package s3vfs_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

// openPebble opens a pebble DB using our S3FS as the storage backend.
func openPebble(t *testing.T, dir string) *pebble.DB {
	t.Helper()
	fs := newTestFS(t)
	db, err := pebble.Open(dir, &pebble.Options{FS: fs})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// TestPebbleSetGet writes a key and reads it back.
func TestPebbleSetGet(t *testing.T) {
	db := openPebble(t, "testdb")

	require.NoError(t, db.Set([]byte("hello"), []byte("world"), pebble.Sync))

	val, closer, err := db.Get([]byte("hello"))
	require.NoError(t, err)
	defer closer.Close()

	require.Equal(t, "world", string(val))
}

// TestPebbleDelete writes a key then deletes it.
func TestPebbleDelete(t *testing.T) {
	db := openPebble(t, "testdb")

	require.NoError(t, db.Set([]byte("key"), []byte("val"), pebble.Sync))
	require.NoError(t, db.Delete([]byte("key"), pebble.Sync))

	_, _, err := db.Get([]byte("key"))
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

// TestPebbleBatch writes multiple keys via a batch.
func TestPebbleBatch(t *testing.T) {
	db := openPebble(t, "testdb")

	b := db.NewBatch()
	for i := 0; i < 10; i++ {
		err := b.Set([]byte(fmt.Sprintf("key%02d", i)), []byte(fmt.Sprintf("val%02d", i)), nil)
		require.NoError(t, err)
	}
	require.NoError(t, b.Commit(pebble.Sync))

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		val, closer, err := db.Get(key)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("val%02d", i), string(val))
		closer.Close()
	}
}

// TestPebbleIterator scans a range of keys using an iterator.
func TestPebbleIterator(t *testing.T) {
	db := openPebble(t, "testdb")

	for _, k := range []string{"apple", "banana", "cherry", "date", "elderberry"} {
		require.NoError(t, db.Set([]byte(k), []byte("v:"+k), pebble.Sync))
	}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("b"),
		UpperBound: []byte("e"),
	})
	require.NoError(t, err)
	defer iter.Close()

	var got []string
	for iter.First(); iter.Valid(); iter.Next() {
		got = append(got, string(iter.Key()))
	}
	require.NoError(t, iter.Error())
	require.Equal(t, []string{"banana", "cherry", "date"}, got)
}

// TestPebbleReopenPersistence closes and reopens the DB, verifying data survives.
func TestPebbleReopenPersistence(t *testing.T) {
	fs := newTestFS(t)

	open := func() *pebble.DB {
		db, err := pebble.Open("persist-db", &pebble.Options{FS: fs})
		require.NoError(t, err)
		return db
	}

	db := open()
	require.NoError(t, db.Set([]byte("persistent"), []byte("yes"), pebble.Sync))
	require.NoError(t, db.Close())

	db2 := open()
	defer db2.Close()

	val, closer, err := db2.Get([]byte("persistent"))
	require.NoError(t, err)
	defer closer.Close()
	require.Equal(t, "yes", string(val))
}

// TestPebbleCompaction writes enough data to trigger compaction.
func TestPebbleCompaction(t *testing.T) {
	db := openPebble(t, "compact-db")

	const n = 1000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("compactkey%06d", i))
		val := make([]byte, 256)
		for j := range val {
			val[j] = byte(i)
		}
		require.NoError(t, db.Set(key, val, nil))
	}
	require.NoError(t, db.Flush())
	require.NoError(t, db.Compact([]byte("compactkey"), []byte("compactkeyz"), true))

	// Spot-check a few keys survive compaction.
	for _, i := range []int{0, 499, 999} {
		key := []byte(fmt.Sprintf("compactkey%06d", i))
		_, closer, err := db.Get(key)
		require.NoError(t, err, "Get(%s) after compaction", key)
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
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Merge([]byte("log"), []byte("first"), pebble.Sync))
	require.NoError(t, db.Merge([]byte("log"), []byte("second"), pebble.Sync))

	val, closer, err := db.Get([]byte("log"))
	require.NoError(t, err)
	defer closer.Close()
	require.NotEmpty(t, val)
}

// TestPebbleDeleteRange deletes a range of keys at once.
func TestPebbleDeleteRange(t *testing.T) {
	db := openPebble(t, "delrange-db")

	var err error
	for i := 0; i < 10; i++ {
		err = db.Set([]byte(fmt.Sprintf("r%02d", i)), []byte("v"), pebble.Sync)
		require.NoError(t, err)
	}

	err = db.DeleteRange([]byte("r03"), []byte("r07"), pebble.Sync)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("r%02d", i))
		_, closer, err := db.Get(key)
		if i >= 3 && i < 7 {
			require.ErrorIs(t, err, pebble.ErrNotFound, "key %s should be deleted", key)
		} else {
			require.NoError(t, err, "key %s should exist", key)
			closer.Close()
		}
	}
}
