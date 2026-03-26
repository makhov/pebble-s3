# pebble-s3

[![CI](https://github.com/makhov/pebble-s3/actions/workflows/ci.yml/badge.svg)](https://github.com/makhov/pebble-s3/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/makhov/pebble-s3.svg)](https://pkg.go.dev/github.com/makhov/pebble-s3)
[![Go Report Card](https://goreportcard.com/badge/github.com/makhov/pebble-s3)](https://goreportcard.com/report/github.com/makhov/pebble-s3)

An S3-compatible [VFS](https://pkg.go.dev/github.com/cockroachdb/pebble/vfs#FS) backend for [Pebble](https://github.com/cockroachdb/pebble), allowing you to store a Pebble database entirely in any S3-compatible service.

## Requirements

### Bucket must exist

`S3FS` does **not** create the bucket for you. The bucket must already exist before opening a database. Make sure the bucket has versioning disabled (the VFS handles its own file lifecycle via object replacement and deletion).

### IAM permissions

The IAM principal used by the S3 client needs at minimum:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:CopyObject",
    "s3:HeadObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::your-bucket",
    "arn:aws:s3:::your-bucket/*"
  ]
}
```

### One database per prefix

Each Pebble database must have its own unique prefix (or its own bucket). Sharing a prefix between two open databases will cause corruption.

## Usage

```go
import (
    "context"

    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/cockroachdb/pebble"
    s3vfs "github.com/makhov/pebble-s3"
)

func main() {
    cfg, err := config.LoadDefaultConfig(context.Background())
    if err != nil {
        panic(err)
    }

    client := s3.NewFromConfig(cfg)

    // bucket must already exist; prefix namespaces all DB files under "myapp/db"
    fs := s3vfs.New(client, "my-bucket", "myapp/db")

    db, err := pebble.Open(".", &pebble.Options{FS: fs})
    if err != nil {
        panic(err)
    }
    defer db.Close()

    db.Set([]byte("hello"), []byte("world"), pebble.Sync)
}
```

The `dirname` passed to `pebble.Open` is used as an additional path component inside the prefix. Passing `"."` keeps all files directly under the prefix.

### Using a prefix

```go
// All Pebble files land under s3://my-bucket/myapp/db/
fs := s3vfs.New(client, "my-bucket", "myapp/db")
```

### No prefix

```go
// All Pebble files land at the root of s3://my-bucket/
fs := s3vfs.New(client, "my-bucket", "")
```

### S3-compatible services

Pass a custom endpoint to the S3 client:

```go
client := s3.NewFromConfig(cfg, func(o *s3.Options) {
    o.BaseEndpoint = aws.String("https://my-minio-host:9000")
    o.UsePathStyle = true // required for most S3-compatible services
})
```

## Design notes and limitations

### Files are buffered in memory

On `Open` and `OpenReadWrite`, the entire object is downloaded into memory. Writes are buffered locally and uploaded to S3 on `Sync` or `Close`. This is simple and correct but means:

- **Memory usage scales with file size.** Pebble SST files can range from a few KB to several hundred MB depending on compaction settings. Size them accordingly.
- **No partial reads from S3.** Every open downloads the full object. A range-read optimization is a natural next step.

### Directories are virtual

S3 has no real directory concept. `MkdirAll` is a no-op. `List` uses `ListObjectsV2` with a `/` delimiter to emulate directory listings. `Stat` on a path that has no matching object falls back to a prefix scan to detect virtual directories.

### Locking is local

`Lock` creates an S3 object as a marker but does **not** provide distributed mutual exclusion. Concurrent access from multiple processes or machines is not safe. For single-process use (the common case for Pebble) this is sufficient.

### `Link` is a server-side copy

S3 has no hard links. `Link(oldname, newname)` is implemented as `CopyObject`, which is a server-side operation and does not transfer data through the client.

## Testing

Tests use [gofakes3](https://github.com/johannesboyne/gofakes3) as an in-memory fake S3 server — no AWS account or real S3 bucket needed.

```
go test ./...
```

The test suite includes:

- Unit tests for every `vfs.FS` and `vfs.File` method (`s3vfs_test.go`)
- Integration tests that open a real Pebble database over the fake S3 backend (`pebble_integration_test.go`), covering: set/get, delete, batch commits, iterators, WAL replay after reopen, compaction, merge operator, and range deletion
