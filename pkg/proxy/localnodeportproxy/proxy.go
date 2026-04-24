/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package localnodeportproxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy"
)

const (
	// dialTimeout is the timeout for connecting to a backend endpoint.
	dialTimeout = 5 * time.Second

	// acceptErrorDelay is the pause after a non-fatal Accept error
	acceptErrorDelay = 100 * time.Millisecond
)

// nodePortSpec describes a single NodePort that needs a localhost proxy.
type nodePortSpec struct {
	servicePortName proxy.ServicePortName
	protocol        v1.Protocol
	port            int
	endpoints       []string
	// sessionAffinityType mirrors Service.spec.sessionAffinity. When set to
	// ClientIP, consecutive connections from localhost are pinned to the same
	// endpoint for stickyMaxAgeSeconds.
	sessionAffinityType v1.ServiceAffinity
	// stickyMaxAgeSeconds is the affinity timeout in seconds, used only when
	// sessionAffinityType is ClientIP.
	stickyMaxAgeSeconds int
}

// affinityTimeout returns the effective affinity window for the spec, or 0
// when affinity is disabled.
func (s *nodePortSpec) affinityTimeout() time.Duration {
	if s.sessionAffinityType != v1.ServiceAffinityClientIP {
		return 0
	}
	return time.Duration(s.stickyMaxAgeSeconds) * time.Second
}

// LocalNodePortProxy manages userspace L4 proxy listeners on localhost
// for NodePort services that cannot be handled by kernel-space proxying
// (e.g. nftables mode, IPv6, IPVS).
type LocalNodePortProxy struct {
	mu       sync.Mutex
	ipFamily v1.IPFamily
	logger   klog.Logger
	listenIP string
	network  string // "tcp4" or "tcp6"
	active   map[string]*nodePortListener
}

// NewLocalNodePortProxy creates a new proxy for the given IP family.
func NewLocalNodePortProxy(ipFamily v1.IPFamily, logger klog.Logger) *LocalNodePortProxy {
	listenIP := "127.0.0.1"
	network := "tcp4"
	if ipFamily == v1.IPv6Protocol {
		listenIP = "::1"
		network = "tcp6"
	}
	return &LocalNodePortProxy{
		ipFamily: ipFamily,
		logger:   logger,
		listenIP: listenIP,
		network:  network,
		active:   make(map[string]*nodePortListener),
	}
}

// SyncNodePorts reconciles the set of active localhost NodePort proxies with
// the desired state. It creates new listeners, removes stale ones, and updates
// endpoint lists for existing ones.
func (p *LocalNodePortProxy) SyncNodePorts(desired map[string]*nodePortSpec) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove stale listeners
	for key, l := range p.active {
		if _, ok := desired[key]; !ok {
			p.logger.V(2).Info("Removing localhost nodeport proxy", "key", key)
			l.shutdown()
			delete(p.active, key)
		}
	}

	// Add or update
	for key, spec := range desired {
		if existing, ok := p.active[key]; ok {
			existing.update(spec)
			continue
		}
		if spec.protocol != v1.ProtocolTCP {
			p.logger.V(2).Info("Skipping non-TCP localhost nodeport proxy: UDP is not currently implemented",
				"service", spec.servicePortName, "protocol", spec.protocol, "nodePort", spec.port)
			continue
		}
		l, err := p.newNodePortListener(key, spec)
		if err != nil {
			p.logger.Error(err, "Failed to create localhost nodeport proxy", "key", key)
			continue
		}
		p.active[key] = l
		p.logger.V(2).Info("Created localhost nodeport proxy", "key", key, "endpoints", len(spec.endpoints))
	}
}

// Shutdown tears down all active listeners and closes any in-flight connections.
// This is primarily used by tests and as a hook for a future graceful-shutdown path.
func (p *LocalNodePortProxy) Shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for key, l := range p.active {
		l.shutdown()
		delete(p.active, key)
	}
}

func (p *LocalNodePortProxy) newNodePortListener(key string, spec *nodePortSpec) (*nodePortListener, error) {
	addr := net.JoinHostPort(p.listenIP, fmt.Sprintf("%d", spec.port))
	listener, err := net.Listen(p.network, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	l := &nodePortListener{
		key:             key,
		port:            spec.port,
		logger:          p.logger,
		endpoints:       spec.endpoints,
		affinityTimeout: spec.affinityTimeout(),
		listener:        listener,
		cancel:          cancel,
		network:         p.network,
	}
	go l.acceptLoop(ctx)
	return l, nil
}

type nodePortListener struct {
	key     string
	port    int
	logger  klog.Logger
	network string // "tcp4" or "tcp6"

	mu        sync.Mutex
	endpoints []string
	nextIndex int
	// affinityTimeout is 0 when SessionAffinity is disabled; otherwise, it is
	// the duration for which a picked endpoint stays pinned for localhost
	// traffic. Since the source IP for all localhost traffic is 127.0.0.1 or
	// ::1, ClientIP affinity effectively pins all traffic through this
	// listener to a single endpoint until the pin goes stale.
	affinityTimeout time.Duration
	pinnedEndpoint  string // "ip:port" of the currently pinned endpoint, empty if none
	pinnedLastUsed  time.Time

	listener net.Listener
	cancel   context.CancelFunc
}

func (l *nodePortListener) acceptLoop(ctx context.Context) {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			l.logger.Error(err, "Accept error on localhost nodeport proxy", "key", l.key)
			t := time.NewTimer(acceptErrorDelay)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			continue
		}
		go l.handleTCPConn(ctx, conn)
	}
}

func (l *nodePortListener) handleTCPConn(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close() //nolint:errcheck

	ep := l.pickEndpoint()
	if ep == "" {
		l.logger.V(4).Info("No endpoints available for localhost nodeport proxy", "key", l.key)
		return
	}

	backendConn, err := net.DialTimeout(l.network, ep, dialTimeout)
	if err != nil {
		l.logger.Error(err, "Failed to connect to backend", "key", l.key, "endpoint", ep)
		return
	}
	defer backendConn.Close() //nolint:errcheck

	// Force both sides closed on listener shutdown so io.Copy returns and
	// the handler goroutine doesn't leak on a long-lived/idle connection.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = clientConn.Close()
			_ = backendConn.Close()
		case <-done:
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(backendConn, clientConn)
		if tc, ok := backendConn.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(clientConn, backendConn)
		if tc, ok := clientConn.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	}()
	wg.Wait()
}

func (l *nodePortListener) pickEndpoint() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.endpoints) == 0 {
		return ""
	}
	now := time.Now()
	if l.affinityTimeout > 0 && l.pinnedEndpoint != "" && now.Sub(l.pinnedLastUsed) <= l.affinityTimeout {
		for _, ep := range l.endpoints {
			if ep == l.pinnedEndpoint {
				l.pinnedLastUsed = now
				return ep
			}
		}
		// Pinned endpoint no longer in the set; fall through and pick a new one.
	}
	ep := l.endpoints[l.nextIndex%len(l.endpoints)]
	l.nextIndex = (l.nextIndex + 1) % len(l.endpoints)
	if l.affinityTimeout > 0 {
		l.pinnedEndpoint = ep
		l.pinnedLastUsed = now
	}
	return ep
}

func (l *nodePortListener) update(spec *nodePortSpec) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.endpoints = spec.endpoints
	if l.nextIndex >= len(spec.endpoints) {
		l.nextIndex = 0
	}
	newTimeout := spec.affinityTimeout()
	if l.affinityTimeout != newTimeout {
		// Affinity config changed; drop any stale pin.
		l.pinnedEndpoint = ""
		l.affinityTimeout = newTimeout
	}
}

func (l *nodePortListener) shutdown() {
	l.cancel()
	_ = l.listener.Close()
}
