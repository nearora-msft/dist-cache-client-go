# dist-cache-client-go

Go client SDK for the distributed cache protocol. Files are split into
fixed-size chunks and distributed across the cluster via consistent hashing.
The client handles connection pooling, server discovery, and the lock-on-miss
protocol for stampede prevention.

> **Status:** temporary home. This module will move to a permanent location
> later; the import path will change once. Pin a version with `go get`.

## Install

```bash
go get github.com/nearora-msft/dist-cache-client-go@latest
```

## Quick start

```go
import dcache "github.com/nearora-msft/dist-cache-client-go"

client, err := dcache.New(
    dcache.WithDiscoveryURL("http://discovery.example.com"),
    dcache.WithChunkSize(16 * 1024 * 1024),
)
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

## Public API

Stable entry points consumed by callers:

- `New(opts ...Option) (*Client, error)`
- `Option` constructors: `WithDiscoveryURL`, `WithK8sDiscovery`,
  `WithServerList`, `WithPort`, `WithChunkSize`, `WithAuth`, `WithCachePrefix`,
  `WithMaxConnsPerServer`, `WithDiscoveryRefresh`
- Per-call options: `UploadOption` (`WithIgnoreLock`, `WithGroupID`,
  `WithMetadata`, `WithTTL`), `DownloadOption` (`WithLock`)
- Result/error types: `ChunkError`, `FileAttr`, `FileAttrEntry`,
  `ErrNotFound`, `ErrNotFoundGotLock`, `ErrNotFoundAlreadyLocked`,
  `IsRecoverableNetErr`

Anything not listed above is implementation detail and may change without
notice.

## Regenerating protobufs

Requires `protoc` with `protoc-gen-go`:

```bash
go generate ./proto/
```

## License

MIT — see [LICENSE](LICENSE).
