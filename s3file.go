package s3vfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cockroachdb/pebble/vfs"
)

// s3File implements vfs.File. Data is buffered in memory and flushed to S3 on
// Sync or Close.
type s3File struct {
	fs       *S3FS
	name     string
	mu       sync.Mutex
	data     []byte
	pos      int64 // sequential read/write cursor
	writable bool
	dirty    bool
	closed   bool
}

var _ vfs.File = (*s3File)(nil)

func (f *s3File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	if f.writable && f.dirty {
		return f.uploadLocked()
	}
	return nil
}

func (f *s3File) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if f.pos >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += int64(n)
	return n, nil
}

func (f *s3File) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("s3vfs: negative offset %d", off)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *s3File) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if !f.writable {
		return 0, fmt.Errorf("s3vfs: file %q is read-only", f.name)
	}
	end := f.pos + int64(len(p))
	f.growLocked(end)
	copy(f.data[f.pos:], p)
	f.pos = end
	f.dirty = true
	return len(p), nil
}

func (f *s3File) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("s3vfs: negative offset %d", off)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	if !f.writable {
		return 0, fmt.Errorf("s3vfs: file %q is read-only", f.name)
	}

	end := off + int64(len(p))
	f.growLocked(end)
	copy(f.data[off:], p)
	f.dirty = true
	return len(p), nil
}

// growLocked extends f.data to at least minLen bytes. Must be called with
// f.mu held.
func (f *s3File) growLocked(minLen int64) {
	if minLen > int64(len(f.data)) {
		f.data = append(f.data, make([]byte, minLen-int64(len(f.data)))...)
	}
}

func (f *s3File) Preallocate(offset, length int64) error { return nil }

func (f *s3File) Stat() (os.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &s3FileInfo{
		name:    f.name,
		size:    int64(len(f.data)),
		modTime: time.Now(),
	}, nil
}

func (f *s3File) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writable && f.dirty {
		return f.uploadLocked()
	}
	return nil
}

func (f *s3File) SyncTo(length int64) (bool, error) {
	return true, f.Sync()
}

func (f *s3File) SyncData() error { return f.Sync() }

func (f *s3File) Prefetch(offset, length int64) error { return nil }

func (f *s3File) Fd() uintptr { return vfs.InvalidFd }

// uploadLocked uploads the buffered data to S3. Must be called with f.mu held.
func (f *s3File) uploadLocked() error {
	_, err := f.fs.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(f.fs.bucket),
		Key:    aws.String(f.fs.s3Key(f.name)),
		Body:   bytes.NewReader(f.data),
	})
	if err == nil {
		f.dirty = false
	}
	return err
}

// s3DirFile is a no-op vfs.File representing a directory.
type s3DirFile struct {
	fs   *S3FS
	name string
}

var _ vfs.File = (*s3DirFile)(nil)

func (f *s3DirFile) Close() error { return nil }
func (f *s3DirFile) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("s3vfs: %q is a directory", f.name)
}
func (f *s3DirFile) ReadAt(p []byte, off int64) (int, error) {
	return 0, fmt.Errorf("s3vfs: %q is a directory", f.name)
}
func (f *s3DirFile) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("s3vfs: %q is a directory", f.name)
}
func (f *s3DirFile) WriteAt(p []byte, off int64) (int, error) {
	return 0, fmt.Errorf("s3vfs: %q is a directory", f.name)
}
func (f *s3DirFile) Preallocate(offset, length int64) error { return nil }
func (f *s3DirFile) Stat() (os.FileInfo, error) {
	return &s3FileInfo{name: f.name, isDir: true, modTime: time.Now()}, nil
}
func (f *s3DirFile) Sync() error                         { return nil }
func (f *s3DirFile) SyncTo(length int64) (bool, error)   { return true, nil }
func (f *s3DirFile) SyncData() error                     { return nil }
func (f *s3DirFile) Prefetch(offset, length int64) error { return nil }
func (f *s3DirFile) Fd() uintptr                         { return vfs.InvalidFd }
