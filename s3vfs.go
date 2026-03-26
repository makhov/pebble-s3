package s3vfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/cockroachdb/pebble/vfs"
)

// S3FS implements vfs.FS backed by Amazon S3.
type S3FS struct {
	client *s3.Client
	bucket string
	prefix string // without trailing slash
}

var _ vfs.FS = (*S3FS)(nil)

// New creates a new S3FS using the given S3 client, bucket name, and optional
// key prefix (e.g. "mydb" to namespace all files under that prefix).
func New(client *s3.Client, bucket, prefix string) *S3FS {
	return &S3FS{
		client: client,
		bucket: bucket,
		prefix: strings.Trim(prefix, "/"),
	}
}

// s3Key converts a VFS path to an S3 object key.
func (fs *S3FS) s3Key(name string) string {
	// Normalize to forward slashes and clean the path.
	name = path.Clean("/" + strings.ReplaceAll(name, "\\", "/"))
	name = strings.TrimPrefix(name, "/")
	if name == "" {
		return fs.prefix // may be "" for root; avoids trailing slash
	}
	if fs.prefix == "" {
		return name
	}
	return fs.prefix + "/" + name
}

// Create creates the named file for reading and writing.
func (fs *S3FS) Create(name string) (vfs.File, error) {
	return &s3File{
		fs:       fs,
		name:     name,
		writable: true,
		dirty:    true, // mark dirty so Close uploads even empty file
	}, nil
}

// Link creates newname as a copy of oldname (S3 has no hard links).
func (fs *S3FS) Link(oldname, newname string) error {
	_, err := fs.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket:     aws.String(fs.bucket),
		CopySource: aws.String(fs.bucket + "/" + fs.s3Key(oldname)),
		Key:        aws.String(fs.s3Key(newname)),
	})
	return err
}

// Open opens the named file for reading.
func (fs *S3FS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	resp, err := fs.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fs.s3Key(name)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
		}
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	f := &s3File{fs: fs, name: name, data: data}
	for _, opt := range opts {
		opt.Apply(f)
	}
	return f, nil
}

// OpenReadWrite opens or creates the named file for reading and writing.
func (fs *S3FS) OpenReadWrite(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	var data []byte
	resp, err := fs.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fs.s3Key(name)),
	})
	if err != nil && !isNotFound(err) {
		return nil, err
	}
	if err == nil {
		defer resp.Body.Close()
		data, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
	}
	f := &s3File{fs: fs, name: name, data: data, writable: true}
	for _, opt := range opts {
		opt.Apply(f)
	}
	return f, nil
}

// OpenDir opens the named directory for syncing.
func (fs *S3FS) OpenDir(name string) (vfs.File, error) {
	return &s3DirFile{fs: fs, name: name}, nil
}

// Remove removes the named file.
func (fs *S3FS) Remove(name string) error {
	_, err := fs.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fs.s3Key(name)),
	})
	return err
}

// RemoveAll removes the named path and all children it contains.
func (fs *S3FS) RemoveAll(name string) error {
	_ = fs.Remove(name)

	prefix := fs.s3Key(name)
	if prefix != "" {
		prefix += "/"
	}
	paginator := s3.NewListObjectsV2Paginator(fs.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(fs.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return err
		}
		for _, obj := range page.Contents {
			if _, err := fs.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: aws.String(fs.bucket),
				Key:    obj.Key,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// Rename renames a file, overwriting the destination if it exists.
func (fs *S3FS) Rename(oldname, newname string) error {
	if err := fs.Link(oldname, newname); err != nil {
		return err
	}
	return fs.Remove(oldname)
}

// ReuseForWrite renames oldname to newname and opens it for writing.
func (fs *S3FS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	if err := fs.Rename(oldname, newname); err != nil {
		return nil, err
	}
	return fs.OpenReadWrite(newname)
}

// MkdirAll is a no-op; S3 has no real directories.
func (fs *S3FS) MkdirAll(dir string, perm os.FileMode) error {
	return nil
}

// Lock creates an exclusive lock file, returning a Closer to release it.
// Exclusivity is enforced via an S3 conditional put (If-None-Match: *), which
// fails if the object already exists. This prevents two callers from both
// believing they hold the lock simultaneously.
func (fs *S3FS) Lock(name string) (io.Closer, error) {
	_, err := fs.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:      aws.String(fs.bucket),
		Key:         aws.String(fs.s3Key(name)),
		Body:        bytes.NewReader(nil),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		return nil, fmt.Errorf("s3vfs: lock %q is already held or could not be acquired: %w", name, err)
	}
	return &s3Lock{fs: fs, name: name}, nil
}

type s3Lock struct {
	fs   *S3FS
	name string
}

func (l *s3Lock) Close() error { return l.fs.Remove(l.name) }

// List returns relative filenames within the given directory.
func (fs *S3FS) List(dir string) ([]string, error) {
	prefix := fs.s3Key(dir)
	if prefix != "" {
		prefix += "/"
	}

	seen := map[string]bool{}
	var names []string

	paginator := s3.NewListObjectsV2Paginator(fs.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(fs.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			base := path.Base(aws.ToString(obj.Key))
			if !seen[base] {
				seen[base] = true
				names = append(names, base)
			}
		}
		for _, cp := range page.CommonPrefixes {
			p := strings.TrimSuffix(aws.ToString(cp.Prefix), "/")
			base := path.Base(p)
			if !seen[base] {
				seen[base] = true
				names = append(names, base)
			}
		}
	}
	return names, nil
}

// Stat returns os.FileInfo for the named file or directory.
func (fs *S3FS) Stat(name string) (os.FileInfo, error) {
	key := fs.s3Key(name)
	if key == "" {
		// Root of the namespace is always a virtual directory.
		return &s3FileInfo{name: name, isDir: true}, nil
	}

	resp, err := fs.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			// Check whether any objects exist under this prefix (virtual directory).
			prefix := key + "/"
			out, listErr := fs.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
				Bucket:  aws.String(fs.bucket),
				Prefix:  aws.String(prefix),
				MaxKeys: aws.Int32(1),
			})
			if listErr == nil && len(out.Contents) > 0 {
				return &s3FileInfo{name: name, isDir: true}, nil
			}
			return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
		}
		return nil, err
	}
	size := int64(0)
	if resp.ContentLength != nil {
		size = *resp.ContentLength
	}
	modTime := time.Time{}
	if resp.LastModified != nil {
		modTime = *resp.LastModified
	}
	return &s3FileInfo{name: name, size: size, modTime: modTime}, nil
}

func (fs *S3FS) PathBase(p string) string       { return path.Base(p) }
func (fs *S3FS) PathJoin(elem ...string) string { return path.Join(elem...) }
func (fs *S3FS) PathDir(p string) string        { return path.Dir(p) }

// GetDiskUsage returns disk usage statistics. Not meaningful for S3.
func (fs *S3FS) GetDiskUsage(p string) (vfs.DiskUsage, error) {
	return vfs.DiskUsage{}, nil
}

// isNotFound reports whether an S3 error is a 404 / key-not-found.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *s3types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "404") ||
		strings.Contains(msg, "NoSuchKey") ||
		strings.Contains(msg, "NotFound")
}

// s3FileInfo implements os.FileInfo for S3 objects and virtual directories.
type s3FileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (fi *s3FileInfo) Name() string       { return path.Base(fi.name) }
func (fi *s3FileInfo) Size() int64        { return fi.size }
func (fi *s3FileInfo) IsDir() bool        { return fi.isDir }
func (fi *s3FileInfo) ModTime() time.Time { return fi.modTime }
func (fi *s3FileInfo) Mode() os.FileMode {
	if fi.isDir {
		return os.ModeDir | 0755
	}
	return 0644
}
func (fi *s3FileInfo) Sys() interface{} { return nil }
