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
)

// NodePortSpec describes a single NodePort that needs a localhost proxy.
type NodePortSpec struct {
	ServicePortName proxy.ServicePortName
	Protocol        v1.Protocol
	Port            int
	Endpoints       []proxy.Endpoint
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
func (p *LocalNodePortProxy) SyncNodePorts(desired map[string]*NodePortSpec) {
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
			existing.updateEndpoints(spec.Endpoints)
			continue
		}
		if spec.Protocol != v1.ProtocolTCP {
			p.logger.V(2).Info("Skipping non-TCP localhost nodeport proxy (not yet supported)", "key", key, "protocol", spec.Protocol)
			continue
		}
		l, err := p.newNodePortListener(key, spec)
		if err != nil {
			p.logger.Error(err, "Failed to create localhost nodeport proxy", "key", key)
			continue
		}
		p.active[key] = l
		p.logger.V(2).Info("Created localhost nodeport proxy", "key", key, "endpoints", len(spec.Endpoints))
	}
}

// Shutdown tears down all active listeners.
func (p *LocalNodePortProxy) Shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for key, l := range p.active {
		l.shutdown()
		delete(p.active, key)
	}
}

func (p *LocalNodePortProxy) newNodePortListener(key string, spec *NodePortSpec) (*nodePortListener, error) {
	addr := net.JoinHostPort(p.listenIP, fmt.Sprintf("%d", spec.Port))
	listener, err := net.Listen(p.network, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	l := &nodePortListener{
		key:       key,
		protocol:  spec.Protocol,
		port:      spec.Port,
		logger:    p.logger,
		endpoints: spec.Endpoints,
		listener:  listener,
		cancel:    cancel,
		network:   p.network,
	}
	go l.acceptLoop(ctx)
	return l, nil
}

type nodePortListener struct {
	key      string
	protocol v1.Protocol
	port     int
	logger   klog.Logger
	network  string // "tcp4" or "tcp6"

	mu        sync.RWMutex
	endpoints []proxy.Endpoint
	nextIndex int

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
			continue
		}
		go l.handleTCPConn(ctx, conn)
	}
}

func (l *nodePortListener) handleTCPConn(_ context.Context, clientConn net.Conn) {
	defer clientConn.Close()

	ep := l.pickEndpoint()
	if ep == nil {
		l.logger.V(4).Info("No endpoints available for localhost nodeport proxy", "key", l.key)
		return
	}

	backendConn, err := net.DialTimeout(l.network, ep.String(), dialTimeout)
	if err != nil {
		l.logger.Error(err, "Failed to connect to backend", "key", l.key, "endpoint", ep.String())
		return
	}
	defer backendConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(backendConn, clientConn)
		if tc, ok := backendConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		io.Copy(clientConn, backendConn)
		if tc, ok := clientConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()
	wg.Wait()
}

func (l *nodePortListener) pickEndpoint() proxy.Endpoint {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.endpoints) == 0 {
		return nil
	}
	ep := l.endpoints[l.nextIndex%len(l.endpoints)]
	l.nextIndex = (l.nextIndex + 1) % len(l.endpoints)
	return ep
}

func (l *nodePortListener) updateEndpoints(endpoints []proxy.Endpoint) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.endpoints = endpoints
	if l.nextIndex >= len(endpoints) {
		l.nextIndex = 0
	}
}

func (l *nodePortListener) shutdown() {
	l.cancel()
	l.listener.Close()
}
