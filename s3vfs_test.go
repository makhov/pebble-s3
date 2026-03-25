package s3vfs_test

import (
	"context"
	"errors"
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
	if err != nil {
		t.Fatal(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})

	if _, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	}); err != nil {
		t.Fatal(err)
	}

	return s3vfs.New(client, testBucket, "")
}

func TestCreateAndRead(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.Create("hello.sst")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("hello world")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f2, err := fs.Open("hello.sst")
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	data, err := io.ReadAll(f2)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Fatalf("got %q, want %q", string(data), "hello world")
	}
}

func TestReadAt(t *testing.T) {
	fs := newTestFS(t)

	f, _ := fs.Create("rand.sst")
	f.Write([]byte("ABCDEFGHIJ"))
	f.Close()

	f2, err := fs.Open("rand.sst")
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	buf := make([]byte, 3)
	n, err := f2.ReadAt(buf, 4)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || string(buf) != "EFG" {
		t.Fatalf("ReadAt(4,3) = %q, want %q", string(buf[:n]), "EFG")
	}
}

func TestWriteAt(t *testing.T) {
	fs := newTestFS(t)

	f, _ := fs.Create("patch.sst")
	f.Write([]byte("hello world"))
	f.WriteAt([]byte("WORLD"), 6)
	f.Close()

	f2, _ := fs.Open("patch.sst")
	defer f2.Close()
	data, _ := io.ReadAll(f2)
	if string(data) != "hello WORLD" {
		t.Fatalf("got %q", string(data))
	}
}

func TestSync(t *testing.T) {
	fs := newTestFS(t)

	f, _ := fs.Create("sync.sst")
	f.Write([]byte("synced data"))
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	f2, _ := fs.Open("sync.sst")
	defer f2.Close()
	data, _ := io.ReadAll(f2)
	if string(data) != "synced data" {
		t.Fatalf("got %q", string(data))
	}
}

func TestRemove(t *testing.T) {
	fs := newTestFS(t)

	f, _ := fs.Create("todelete.sst")
	f.Close()

	if err := fs.Remove("todelete.sst"); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Open("todelete.sst"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected ErrNotExist, got %v", err)
	}
}

func TestRemoveAll(t *testing.T) {
	fs := newTestFS(t)

	for _, name := range []string{"db/a.sst", "db/b.sst", "db/sub/c.sst"} {
		f, _ := fs.Create(name)
		f.Close()
	}

	if err := fs.RemoveAll("db"); err != nil {
		t.Fatal(err)
	}

	names, err := fs.List("db")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		t.Fatalf("expected empty dir after RemoveAll, got %v", names)
	}
}

func TestRename(t *testing.T) {
	fs := newTestFS(t)

	f, _ := fs.Create("old.sst")
	f.Write([]byte("data"))
	f.Close()

	if err := fs.Rename("old.sst", "new.sst"); err != nil {
		t.Fatal(err)
	}

	if _, err := fs.Open("old.sst"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("old file should not exist")
	}

	f2, err := fs.Open("new.sst")
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	data, _ := io.ReadAll(f2)
	if string(data) != "data" {
		t.Fatalf("got %q", string(data))
	}
}

func TestLink(t *testing.T) {
	fs := newTestFS(t)

	f, _ := fs.Create("src.sst")
	f.Write([]byte("linked"))
	f.Close()

	if err := fs.Link("src.sst", "dst.sst"); err != nil {
		t.Fatal(err)
	}

	f2, _ := fs.Open("dst.sst")
	defer f2.Close()
	data, _ := io.ReadAll(f2)
	if string(data) != "linked" {
		t.Fatalf("got %q", string(data))
	}
}

func TestList(t *testing.T) {
	fs := newTestFS(t)

	files := []string{"dir/a.sst", "dir/b.sst", "dir/c.sst"}
	for _, name := range files {
		f, _ := fs.Create(name)
		f.Close()
	}

	got, err := fs.List("dir")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	want := []string{"a.sst", "b.sst", "c.sst"}
	if len(got) != len(want) {
		t.Fatalf("List = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("List[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestStat(t *testing.T) {
	fs := newTestFS(t)

	f, _ := fs.Create("stat.sst")
	f.Write([]byte("12345"))
	f.Close()

	fi, err := fs.Stat("stat.sst")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 5 {
		t.Fatalf("Size = %d, want 5", fi.Size())
	}
	if fi.IsDir() {
		t.Fatal("expected file, got dir")
	}
	if fi.Name() != "stat.sst" {
		t.Fatalf("Name = %q", fi.Name())
	}
}

func TestStatNotExist(t *testing.T) {
	fs := newTestFS(t)

	_, err := fs.Stat("missing.sst")
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected ErrNotExist, got %v", err)
	}
}

func TestOpenReadWrite(t *testing.T) {
	fs := newTestFS(t)

	// Create initial file.
	f, _ := fs.Create("rw.sst")
	f.Write([]byte("initial"))
	f.Close()

	// Open for read/write, append more data.
	f2, err := fs.OpenReadWrite("rw.sst")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(f2)
	if string(data) != "initial" {
		t.Fatalf("initial read = %q", string(data))
	}
	f2.Write([]byte(" appended"))
	f2.Close()

	f3, _ := fs.Open("rw.sst")
	defer f3.Close()
	all, _ := io.ReadAll(f3)
	if string(all) != "initial appended" {
		t.Fatalf("after append = %q", string(all))
	}
}

func TestOpenReadWriteNewFile(t *testing.T) {
	fs := newTestFS(t)

	f, err := fs.OpenReadWrite("new.sst")
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("new"))
	f.Close()

	f2, _ := fs.Open("new.sst")
	defer f2.Close()
	data, _ := io.ReadAll(f2)
	if string(data) != "new" {
		t.Fatalf("got %q", string(data))
	}
}

func TestMkdirAll(t *testing.T) {
	fs := newTestFS(t)
	// MkdirAll is a no-op on S3 but must not error.
	if err := fs.MkdirAll("a/b/c", 0755); err != nil {
		t.Fatal(err)
	}
}

func TestOpenDir(t *testing.T) {
	fs := newTestFS(t)
	dir, err := fs.OpenDir("some/dir")
	if err != nil {
		t.Fatal(err)
	}
	if err := dir.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := dir.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLock(t *testing.T) {
	fs := newTestFS(t)

	closer, err := fs.Lock("LOCK")
	if err != nil {
		t.Fatal(err)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReuseForWrite(t *testing.T) {
	fs := newTestFS(t)

	// Create an old file with 10 bytes of content.
	f, _ := fs.Create("old.sst")
	f.Write([]byte("AAAAAAAAAA"))
	f.Close()

	// ReuseForWrite renames old→new and opens for writing at position 0
	// without truncation: new bytes overwrite the front, tail is preserved.
	f2, err := fs.ReuseForWrite("old.sst", "reused.sst")
	if err != nil {
		t.Fatal(err)
	}
	f2.Write([]byte("BBB")) // overwrite first 3 bytes
	f2.Close()

	// old should no longer exist
	if _, err := fs.Open("old.sst"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("old file should be gone after ReuseForWrite")
	}

	f3, _ := fs.Open("reused.sst")
	defer f3.Close()
	data, _ := io.ReadAll(f3)
	// First 3 bytes overwritten, remaining 7 preserved.
	if string(data) != "BBBAAAAAAA" {
		t.Fatalf("got %q, want %q", string(data), "BBBAAAAAAA")
	}
}

func TestPathMethods(t *testing.T) {
	fs := newTestFS(t)

	if got := fs.PathBase("a/b/c.sst"); got != "c.sst" {
		t.Errorf("PathBase = %q", got)
	}
	if got := fs.PathDir("a/b/c.sst"); got != "a/b" {
		t.Errorf("PathDir = %q", got)
	}
	if got := fs.PathJoin("a", "b", "c.sst"); got != "a/b/c.sst" {
		t.Errorf("PathJoin = %q", got)
	}
}

func TestGetDiskUsage(t *testing.T) {
	fs := newTestFS(t)
	usage, err := fs.GetDiskUsage(".")
	if err != nil {
		t.Fatal(err)
	}
	_ = usage // S3 returns zero values; just check no error
}

func TestFileStat(t *testing.T) {
	fs := newTestFS(t)

	f, _ := fs.Create("filestat.sst")
	f.Write([]byte("abc"))
	fi, err := f.(interface{ Stat() (os.FileInfo, error) }).Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 3 {
		t.Fatalf("in-memory stat size = %d, want 3", fi.Size())
	}
	f.Close()
}

func TestPrefix(t *testing.T) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	srv := httptest.NewServer(faker.Server())
	t.Cleanup(srv.Close)

	cfg, _ := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})
	client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(testBucket)})

	fs := s3vfs.New(client, testBucket, "mydb")

	f, _ := fs.Create("test.sst")
	f.Write([]byte("prefixed"))
	f.Close()

	// Verify the object was stored under the prefix.
	out, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String("mydb/test.sst"),
	})
	if err != nil {
		t.Fatalf("expected object at mydb/test.sst: %v", err)
	}
	defer out.Body.Close()
	data, _ := io.ReadAll(out.Body)
	if string(data) != "prefixed" {
		t.Fatalf("got %q", string(data))
	}
}
