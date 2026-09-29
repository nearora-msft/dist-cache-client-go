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
- `NewWithContext(ctx context.Context, opts ...Option) (*Client, error)`
- `DiscoverServers(ctx context.Context, opts ...Option) ([]string, error)`
- `Option` constructors: `WithDiscoveryURL`, `WithDNSServer`,
  `WithServerList`, `WithPort`, `WithChunkSize`,
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
values are rejected during `New`. DNS and connection errors include the cache
server and wrap the underlying network error so callers can classify and log
failures using their own logging policy.

`NewWithContext` allows callers to cancel or bound initial server discovery.
`New` remains available for compatibility and bounds discovery with the
configured request timeout.

When a discovery endpoint is configured, cache unavailability at startup is not
fatal: `New` returns a client with no servers and the background discovery
refresh (`WithDiscoveryRefresh`) adds servers once they become available. Until
then, operations return `ErrNoServers`, which `IsRecoverableNetErr` reports as
recoverable so callers can fall back to their storage path. Invalid options and
caller cancellation still fail `New`, as does an empty static server list.

Server discovery supports an authoritative discovery endpoint or a static
server list. Kubernetes service/namespace discovery is intentionally unsupported
because Service endpoints reflect temporary pod availability rather than cache
ring membership.

## Regenerating protobufs

Requires `protoc` with `protoc-gen-go`:

```bash
go generate ./proto/
```

## License

MIT — see [LICENSE](LICENSE).
