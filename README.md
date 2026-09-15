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
    // Optional: route cache hostname lookups through this caller-provided DNS server.
    dcache.WithDNSServer("192.0.2.53:53"),
    dcache.WithChunkSize(16 * 1024 * 1024),
    // Store and validate a CRC32 checksum for every chunk.
    dcache.WithChecksumVerification(true),
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
  `WithDNSServer`, `WithServerList`, `WithPort`, `WithChunkSize`,
  `WithCachePrefix`, `WithMaxConnsPerServer`, `WithDiscoveryRefresh`,
  `WithChecksumVerification`
- Per-call options: `UploadOption` (`WithIgnoreLock`, `WithGroupID`,
  `WithMetadata`, `WithTTL`), `DownloadOption` (`WithLock`)
- Result/error types: `ChunkError`, `FileAttr`, `FileAttrEntry`,
  `ErrNotFound`, `ErrNotFoundGotLock`, `ErrNotFoundAlreadyLocked`,
    `ErrChecksumMismatch`, `IsRecoverableNetErr`

Anything not listed above is implementation detail and may change without
notice.

`WithDNSServer` accepts an IPv4 address or `IPv4:port`; an address without a
port uses port 53. If it is omitted, the system resolver is used. Invalid
values are rejected during `New`. DNS logs identify the
selected resolver and report whether cache server hostnames resolved, including
the selected remote address on a successful connection. Messages use Go's
standard logger, allowing the hosting process to route them to its configured
log destination.

## Regenerating protobufs

Requires `protoc` with `protoc-gen-go`:

```bash
go generate ./proto/
```

## License

MIT — see [LICENSE](LICENSE).
