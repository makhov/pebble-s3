package s3vfs_test

import (
	"context"
	"io"
	"net/http/httptest"
	"os"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	s3vfs "github.com/makhov/pebble-s3"
	"github.com/stretchr/testify/require"
)

const testBucket = "pebble-test"

// newTestFS creates an S3FS backed by an in-memory fake S3 server.
func newTestFS(t *testing.T) *s3vfs.S3FS {
	t.Helper()
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	srv := httptest.NewServer(faker.Server())
	t.Cleanup(srv.Close)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	})
	require.NoError(t, err)

	return s3vfs.New(client, testBucket, "")
}

func TestCreateAndRead(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("hello.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("hello world"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("hello.sst")
	require.NoError(t, err)
	defer f2.Close()

	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(data))
}

func TestReadAt(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("rand.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("ABCDEFGHIJ"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("rand.sst")
	require.NoError(t, err)
	defer f2.Close()

	buf := make([]byte, 3)
	n, err := f2.ReadAt(buf, 4)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "EFG", string(buf))
}

func TestReadAtNegativeOffset(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("neg.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("neg.sst")
	require.NoError(t, err)
	defer f2.Close()

	_, err = f2.ReadAt(make([]byte, 1), -1)
	require.Error(t, err)
}

func TestWriteAt(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("patch.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("hello world"))
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("WORLD"), 6)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("patch.sst")
	require.NoError(t, err)
	defer f2.Close()

	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	require.Equal(t, "hello WORLD", string(data))
}

func TestWriteAtNegativeOffset(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("neg.sst")
	require.NoError(t, err)
	defer f.Close()

	_, err = f.WriteAt([]byte("x"), -1)
	require.Error(t, err)
}

func TestSync(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("sync.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("synced data"))
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	f2, err := fs.Open("sync.sst")
	require.NoError(t, err)
	defer f2.Close()

	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	require.Equal(t, "synced data", string(data))
}

func TestRemove(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("todelete.sst")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, fs.Remove("todelete.sst"))

	_, err = fs.Open("todelete.sst")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestRemoveAll(t *testing.T) {
	fs := newTestFS(t)

	for _, name := range []string{"db/a.sst", "db/b.sst", "db/sub/c.sst"} {
		f, err := fs.Create(name)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	require.NoError(t, fs.RemoveAll("db"))

	names, err := fs.List("db")
	require.NoError(t, err)
	require.Empty(t, names)
}

func TestRename(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("old.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, fs.Rename("old.sst", "new.sst"))

	_, err = fs.Open("old.sst")
	require.ErrorIs(t, err, os.ErrNotExist)

	f2, err := fs.Open("new.sst")
	require.NoError(t, err)
	defer f2.Close()

	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	require.Equal(t, "data", string(data))
}

func TestLink(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("src.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("linked"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, fs.Link("src.sst", "dst.sst"))

	f2, err := fs.Open("dst.sst")
	require.NoError(t, err)
	defer f2.Close()

	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	require.Equal(t, "linked", string(data))
}

func TestList(t *testing.T) {
	fs := newTestFS(t)

	for _, name := range []string{"dir/a.sst", "dir/b.sst", "dir/c.sst"} {
		f, err := fs.Create(name)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	got, err := fs.List("dir")
	require.NoError(t, err)

	sort.Strings(got)
	require.Equal(t, []string{"a.sst", "b.sst", "c.sst"}, got)
}

func TestStat(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("stat.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("12345"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	fi, err := fs.Stat("stat.sst")
	require.NoError(t, err)
	require.Equal(t, int64(5), fi.Size())
	require.False(t, fi.IsDir())
	require.Equal(t, "stat.sst", fi.Name())
}

func TestStatNotExist(t *testing.T) {
	fs := newTestFS(t)

	_, err := fs.Stat("missing.sst")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestStatRoot(t *testing.T) {
	fs := newTestFS(t)

	fi, err := fs.Stat(".")
	require.NoError(t, err)
	require.True(t, fi.IsDir())
}

func TestOpenReadWrite(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("rw.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("initial"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.OpenReadWrite("rw.sst")
	require.NoError(t, err)

	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	require.Equal(t, "initial", string(data))

	_, err = f2.Write([]byte(" appended"))
	require.NoError(t, err)
	require.NoError(t, f2.Close())

	f3, err := fs.Open("rw.sst")
	require.NoError(t, err)
	defer f3.Close()

	all, err := io.ReadAll(f3)
	require.NoError(t, err)
	require.Equal(t, "initial appended", string(all))
}

func TestOpenReadWriteNewFile(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.OpenReadWrite("new.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("new"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("new.sst")
	require.NoError(t, err)
	defer f2.Close()

	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	require.Equal(t, "new", string(data))
}

func TestMkdirAll(t *testing.T) {
	fs := newTestFS(t)
	require.NoError(t, fs.MkdirAll("a/b/c", 0755))
}

func TestOpenDir(t *testing.T) {
	fs := newTestFS(t)

	dir, err := fs.OpenDir("some/dir")
	require.NoError(t, err)
	require.NoError(t, dir.Sync())
	require.NoError(t, dir.Close())
}

func TestLock(t *testing.T) {
	fs := newTestFS(t)

	closer, err := fs.Lock("LOCK")
	require.NoError(t, err)
	require.NoError(t, closer.Close())
}

func TestLockExclusive(t *testing.T) {
	fs := newTestFS(t)

	closer, err := fs.Lock("LOCK")
	require.NoError(t, err)
	defer closer.Close()

	_, err = fs.Lock("LOCK")
	require.Error(t, err)
}

func TestReuseForWrite(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("old.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("AAAAAAAAAA"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// ReuseForWrite renames old→new and opens for writing at position 0
	// without truncation: new bytes overwrite the front, tail is preserved.
	f2, err := fs.ReuseForWrite("old.sst", "reused.sst")
	require.NoError(t, err)
	_, err = f2.Write([]byte("BBB"))
	require.NoError(t, err)
	require.NoError(t, f2.Close())

	_, err = fs.Open("old.sst")
	require.ErrorIs(t, err, os.ErrNotExist)

	f3, err := fs.Open("reused.sst")
	require.NoError(t, err)
	defer f3.Close()

	// First 3 bytes overwritten, remaining 7 preserved.
	data, err := io.ReadAll(f3)
	require.NoError(t, err)
	require.Equal(t, "BBBAAAAAAA", string(data))
}

func TestPathMethods(t *testing.T) {
	fs := newTestFS(t)

	require.Equal(t, "c.sst", fs.PathBase("a/b/c.sst"))
	require.Equal(t, "a/b", fs.PathDir("a/b/c.sst"))
	require.Equal(t, "a/b/c.sst", fs.PathJoin("a", "b", "c.sst"))
}

func TestGetDiskUsage(t *testing.T) {
	fs := newTestFS(t)

	_, err := fs.GetDiskUsage(".")
	require.NoError(t, err)
}

func TestFileStat(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("filestat.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("abc"))
	require.NoError(t, err)

	fi, err := f.(interface{ Stat() (os.FileInfo, error) }).Stat()
	require.NoError(t, err)
	require.Equal(t, int64(3), fi.Size())

	require.NoError(t, f.Close())
}

func TestPrefix(t *testing.T) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	srv := httptest.NewServer(faker.Server())
	t.Cleanup(srv.Close)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})
	_, err = client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	})
	require.NoError(t, err)

	fs := s3vfs.New(client, testBucket, "mydb")

	f, err := fs.Create("test.sst")
	require.NoError(t, err)
	_, err = f.Write([]byte("prefixed"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Verify the object was stored under the prefix.
	out, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("mydb/test.sst"),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	require.Equal(t, "prefixed", string(data))
}
