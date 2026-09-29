// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package dcache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/nearora-msft/dist-cache-client-go/proto"
)

// discovery manages server list resolution and background refresh.
type discovery struct {
	mu      sync.RWMutex
	servers []string
	ring    *ConsistentHashRing
	cfg     *clientConfig
	connMgr *connManager
	ctx     context.Context
	cancel  context.CancelFunc
}

func newDiscovery(ctx context.Context, cfg *clientConfig, connMgr *connManager, vnodes int) (*discovery, error) {
	refreshCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	d := &discovery{
		cfg:     cfg,
		connMgr: connMgr,
		ring:    NewConsistentHashRing(nil, vnodes),
		ctx:     refreshCtx,
		cancel:  cancel,
	}

	servers, err := d.resolveServers(ctx)
	if err == nil && len(servers) == 0 {
		err = ErrNoServers
	}
	if err != nil {
		// With a discovery endpoint, cache unavailability at startup is not
		// fatal: start with an empty ring and let the refresh loop populate it.
		// Operations return ErrNoServers until then. Caller cancellation and
		// static/env server lists still fail.
		if cfg.discoveryURL == "" || errors.Is(ctx.Err(), context.Canceled) {
			cancel()
			return nil, err
		}
	} else {
		d.servers = servers
		d.ring.UpdateServers(servers)
	}

	// Start background refresh if using dynamic discovery
	if cfg.discoveryURL != "" {
		go d.refreshLoop()
	}

	return d, nil
}

// getServer returns the server responsible for the given cache key.
func (d *discovery) getServer(key string) (string, error) {
	return d.ring.GetServer(key)
}

// getServers returns a copy of the current server list.
func (d *discovery) getServers() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]string, len(d.servers))
	copy(out, d.servers)
	return out
}

// resolveServers determines the server list using the configured discovery method.
// Priority: discovery URL > static list > environment variable.
func (d *discovery) resolveServers(ctx context.Context) ([]string, error) {
	var discoveryErrs []error

	// 1. Discovery endpoint (GetCacheServers RPC)
	if d.cfg.discoveryURL != "" {
		servers, err := d.discoverViaRPC(ctx, d.cfg.discoveryURL)
		if err == nil && len(servers) > 0 {
			return servers, nil
		}
		if err != nil {
			discoveryErrs = append(discoveryErrs, fmt.Errorf("discovery RPC %q: %w", d.cfg.discoveryURL, err))
		}
		// Fall through to next method on error
	}

	// 2. Static server list from config
	if len(d.cfg.servers) > 0 {
		return d.cfg.servers, nil
	}

	// 3. Environment variable
	if envList := os.Getenv("DIST_CACHE_SERVER_LIST"); envList != "" {
		servers := parseServerList(envList)
		if len(servers) > 0 {
			return servers, nil
		}
	}

	if len(discoveryErrs) > 0 {
		return nil, errors.Join(append([]error{ErrNoServers}, discoveryErrs...)...)
	}
	return nil, ErrNoServers
}

// discoverViaRPC connects to the discovery endpoint and calls GetCacheServers.
func (d *discovery) discoverViaRPC(ctx context.Context, endpoint string) ([]string, error) {
	c, err := d.connMgr.getConn(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	var discarded bool
	defer func() {
		if !discarded {
			d.connMgr.putConn(c)
		}
	}()

	deadline := time.Now().Add(d.cfg.requestTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := c.setDeadline(deadline); err != nil {
		d.connMgr.discardConn(c)
		discarded = true
		return nil, err
	}

	req := &pb.Request{
		Payload: &pb.Request_Getcacheserversrequest{
			Getcacheserversrequest: &pb.GetCacheServersRequest{},
		},
	}

	if err := c.sendRequest(req, nil); err != nil {
		d.connMgr.discardConn(c)
		discarded = true
		return nil, fmt.Errorf("send GetCacheServers: %w", err)
	}

	var resp pb.GetCacheServersResponse
	if err := c.recvProto(&resp); err != nil {
		d.connMgr.discardConn(c)
		discarded = true
		return nil, fmt.Errorf("recv GetCacheServers: %w", err)
	}

	if resp.Result != pb.GetCacheServersResponse_SUCCESS {
		return nil, fmt.Errorf("GetCacheServers failed: %v", resp.Result)
	}

	// Normalize server addresses to include port
	servers := make([]string, 0, len(resp.Serveraddresses))
	for _, addr := range resp.Serveraddresses {
		if !strings.Contains(addr, ":") {
			addr = fmt.Sprintf("%s:%d", addr, d.cfg.port)
		}
		servers = append(servers, addr)
	}

	sort.Strings(servers)
	return servers, nil
}

// refreshLoop periodically re-discovers the server list.
func (d *discovery) refreshLoop() {
	ticker := time.NewTicker(d.cfg.discoveryRefresh)
	defer ticker.Stop()

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(d.ctx, d.cfg.requestTimeout)
			d.refresh(ctx)
			cancel()
		}
	}
}

func (d *discovery) refresh(ctx context.Context) {
	servers, err := d.resolveServers(ctx)
	if err != nil || len(servers) == 0 {
		return // keep existing servers on refresh failure
	}

	d.mu.Lock()
	d.servers = servers
	d.mu.Unlock()

	d.ring.UpdateServers(servers)
}

// close stops the background refresh loop.
func (d *discovery) close() {
	d.cancel()
}

// parseServerList splits a comma-separated server list string.
func parseServerList(list string) []string {
	parts := strings.Split(list, ",")
	servers := make([]string, 0, len(parts))
	for _, s := range parts {
		s = strings.TrimSpace(s)
		if s != "" {
			servers = append(servers, s)
		}
	}
	return servers
}

// DiscoverServers discovers servers without creating a full client.
// It is useful for health checks and diagnostics.
func DiscoverServers(ctx context.Context, opts ...Option) ([]string, error) {
	if ctx == nil {
		return nil, fmt.Errorf("dcache: nil context")
	}
	cfg := configWithOptions(opts)
	dnsEndpoint, err := parseDNSServer(cfg.dnsServer)
	if err != nil {
		return nil, err
	}
	cm := newConnManager(2, cfg.dialTimeout, 0, dnsResolver(dnsEndpoint))
	defer cm.closeAll()

	d := &discovery{cfg: cfg, connMgr: cm}
	return d.resolveServers(ctx)
}
