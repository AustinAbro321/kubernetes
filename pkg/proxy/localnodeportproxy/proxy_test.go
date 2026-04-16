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
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/proxy"
)

// testEndpoint implements proxy.Endpoint for testing.
type testEndpoint struct {
	ip   string
	port int
}

func (e *testEndpoint) String() string              { return net.JoinHostPort(e.ip, fmt.Sprintf("%d", e.port)) }
func (e *testEndpoint) IP() string                  { return e.ip }
func (e *testEndpoint) Port() int                   { return e.port }
func (e *testEndpoint) IsLocal() bool               { return false }
func (e *testEndpoint) IsReady() bool               { return true }
func (e *testEndpoint) IsServing() bool             { return true }
func (e *testEndpoint) IsTerminating() bool         { return false }
func (e *testEndpoint) ZoneHints() sets.Set[string] { return nil }
func (e *testEndpoint) NodeHints() sets.Set[string] { return nil }

func makeServicePortName(ns, name, port string) proxy.ServicePortName {
	return proxy.ServicePortName{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
		Port:           port,
		Protocol:       v1.ProtocolTCP,
	}
}

// startTCPEchoServer starts a TCP server that echoes back everything it receives.
// Returns the listener and a cleanup function.
func startTCPEchoServer(t *testing.T, network, addr string) net.Listener {
	t.Helper()
	l, err := net.Listen(network, addr)
	if err != nil {
		t.Fatalf("Failed to start echo server: %v", err)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c)
			}(conn)
		}
	}()
	return l
}

func TestSyncNodePorts_AddAndRemove(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(v1.IPv4Protocol, logger)
	defer p.Shutdown()

	svcName := makeServicePortName("default", "test-svc", "http")

	// Start a backend echo server
	backend := startTCPEchoServer(t, "tcp4", "127.0.0.1:0")
	defer backend.Close()
	backendPort := backend.Addr().(*net.TCPAddr).Port

	ep := &testEndpoint{ip: "127.0.0.1", port: backendPort}

	// Use a free port for the nodeport
	freeListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	nodePort := freeListener.Addr().(*net.TCPAddr).Port
	freeListener.Close()

	key := fmt.Sprintf("tcp/%d", nodePort)
	desired := map[string]*NodePortSpec{
		key: {
			ServicePortName: svcName,
			Protocol:        v1.ProtocolTCP,
			Port:            nodePort,
			Endpoints:       []proxy.Endpoint{ep},
		},
	}

	p.SyncNodePorts(desired)

	if len(p.active) != 1 {
		t.Fatalf("Expected 1 active listener, got %d", len(p.active))
	}
	if _, ok := p.active[key]; !ok {
		t.Fatalf("Expected listener for key %s", key)
	}

	// Verify we can connect through the proxy
	conn, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to nodeport proxy: %v", err)
	}
	testMsg := "hello nodeport"
	fmt.Fprint(conn, testMsg)
	conn.(*net.TCPConn).CloseWrite()
	buf, err := io.ReadAll(conn)
	conn.Close()
	if err != nil {
		t.Fatalf("Failed to read from proxy: %v", err)
	}
	if string(buf) != testMsg {
		t.Errorf("Expected %q, got %q", testMsg, string(buf))
	}

	// Remove the NodePort
	p.SyncNodePorts(map[string]*NodePortSpec{})
	if len(p.active) != 0 {
		t.Fatalf("Expected 0 active listeners after removal, got %d", len(p.active))
	}

	// Verify port is closed
	_, err = net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), 500*time.Millisecond)
	if err == nil {
		t.Fatal("Expected connection to fail after listener removal")
	}
}

func TestSyncNodePorts_UpdateEndpoints(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(v1.IPv4Protocol, logger)
	defer p.Shutdown()

	// Start two backend servers
	backend1 := startTCPEchoServer(t, "tcp4", "127.0.0.1:0")
	defer backend1.Close()
	backend2 := startTCPEchoServer(t, "tcp4", "127.0.0.1:0")
	defer backend2.Close()

	ep1 := &testEndpoint{ip: "127.0.0.1", port: backend1.Addr().(*net.TCPAddr).Port}
	ep2 := &testEndpoint{ip: "127.0.0.1", port: backend2.Addr().(*net.TCPAddr).Port}

	svcName := makeServicePortName("default", "test-svc", "http")

	// Get a free port
	freeListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	nodePort := freeListener.Addr().(*net.TCPAddr).Port
	freeListener.Close()

	key := fmt.Sprintf("tcp/%d", nodePort)

	// Start with ep1 only
	p.SyncNodePorts(map[string]*NodePortSpec{
		key: {
			ServicePortName: svcName,
			Protocol:        v1.ProtocolTCP,
			Port:            nodePort,
			Endpoints:       []proxy.Endpoint{ep1},
		},
	})

	if len(p.active) != 1 {
		t.Fatalf("Expected 1 active listener, got %d", len(p.active))
	}

	// Update to ep2
	p.SyncNodePorts(map[string]*NodePortSpec{
		key: {
			ServicePortName: svcName,
			Protocol:        v1.ProtocolTCP,
			Port:            nodePort,
			Endpoints:       []proxy.Endpoint{ep2},
		},
	})

	// Should still have exactly 1 listener (same one, updated endpoints)
	if len(p.active) != 1 {
		t.Fatalf("Expected 1 active listener after update, got %d", len(p.active))
	}

	// Verify connectivity still works
	conn, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect after endpoint update: %v", err)
	}
	testMsg := "after update"
	fmt.Fprint(conn, testMsg)
	conn.(*net.TCPConn).CloseWrite()
	buf, err := io.ReadAll(conn)
	conn.Close()
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if string(buf) != testMsg {
		t.Errorf("Expected %q, got %q", testMsg, string(buf))
	}
}

func TestSyncNodePorts_SkipUDP(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(v1.IPv4Protocol, logger)
	defer p.Shutdown()

	svcName := makeServicePortName("default", "udp-svc", "dns")

	p.SyncNodePorts(map[string]*NodePortSpec{
		"udp/30053": {
			ServicePortName: svcName,
			Protocol:        v1.ProtocolUDP,
			Port:            30053,
			Endpoints:       []proxy.Endpoint{&testEndpoint{ip: "10.0.0.1", port: 53}},
		},
	})

	if len(p.active) != 0 {
		t.Fatalf("Expected 0 active listeners for UDP, got %d", len(p.active))
	}
}

func TestRoundRobinEndpointSelection(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(v1.IPv4Protocol, logger)
	defer p.Shutdown()

	// Start 3 backend servers that respond with their port
	var backends []net.Listener
	var endpoints []proxy.Endpoint
	for range 3 {
		l, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to start backend: %v", err)
		}
		backends = append(backends, l)
		port := l.Addr().(*net.TCPAddr).Port
		endpoints = append(endpoints, &testEndpoint{ip: "127.0.0.1", port: port})
		go func(listener net.Listener, p int) {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				go func(c net.Conn) {
					defer c.Close()
					fmt.Fprintf(c, "port:%d", p)
				}(conn)
			}
		}(l, port)
	}
	defer func() {
		for _, b := range backends {
			b.Close()
		}
	}()

	// Get free port for nodeport
	fl, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nodePort := fl.Addr().(*net.TCPAddr).Port
	fl.Close()

	key := fmt.Sprintf("tcp/%d", nodePort)
	p.SyncNodePorts(map[string]*NodePortSpec{
		key: {
			ServicePortName: makeServicePortName("default", "rr-svc", "http"),
			Protocol:        v1.ProtocolTCP,
			Port:            nodePort,
			Endpoints:       endpoints,
		},
	})

	// Make 6 connections and verify round-robin distribution
	responses := make(map[string]int)
	for range 6 {
		conn, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), 2*time.Second)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		buf, err := io.ReadAll(conn)
		conn.Close()
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}
		responses[string(buf)]++
	}

	// Each of the 3 backends should have been hit exactly twice
	if len(responses) != 3 {
		t.Errorf("Expected 3 different backends, got %d: %v", len(responses), responses)
	}
	for resp, count := range responses {
		if count != 2 {
			t.Errorf("Backend %s was hit %d times, expected 2", resp, count)
		}
	}
}

func TestBackendConnectionFailure(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(v1.IPv4Protocol, logger)
	defer p.Shutdown()

	// Use an endpoint that isn't listening
	ep := &testEndpoint{ip: "127.0.0.1", port: 1} // port 1 is almost certainly not listening

	fl, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nodePort := fl.Addr().(*net.TCPAddr).Port
	fl.Close()

	key := fmt.Sprintf("tcp/%d", nodePort)
	p.SyncNodePorts(map[string]*NodePortSpec{
		key: {
			ServicePortName: makeServicePortName("default", "fail-svc", "http"),
			Protocol:        v1.ProtocolTCP,
			Port:            nodePort,
			Endpoints:       []proxy.Endpoint{ep},
		},
	})

	// Connect — the proxy should accept but then close the connection
	// when the backend dial fails
	conn, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to proxy: %v", err)
	}
	buf, _ := io.ReadAll(conn)
	conn.Close()

	if len(buf) != 0 {
		t.Errorf("Expected empty response on backend failure, got %q", string(buf))
	}
}

func TestShutdown(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(v1.IPv4Protocol, logger)

	// Create multiple listeners
	var ports []int
	desired := make(map[string]*NodePortSpec)
	for range 3 {
		fl, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := fl.Addr().(*net.TCPAddr).Port
		fl.Close()
		ports = append(ports, port)

		key := fmt.Sprintf("tcp/%d", port)
		desired[key] = &NodePortSpec{
			ServicePortName: makeServicePortName("default", fmt.Sprintf("svc-%d", port), "http"),
			Protocol:        v1.ProtocolTCP,
			Port:            port,
			Endpoints:       []proxy.Endpoint{&testEndpoint{ip: "127.0.0.1", port: 1}},
		}
	}

	p.SyncNodePorts(desired)
	if len(p.active) != 3 {
		t.Fatalf("Expected 3 active listeners, got %d", len(p.active))
	}

	p.Shutdown()
	if len(p.active) != 0 {
		t.Fatalf("Expected 0 active listeners after shutdown, got %d", len(p.active))
	}

	// Verify all ports are closed
	for _, port := range ports {
		_, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err == nil {
			t.Errorf("Port %d still accepting connections after shutdown", port)
		}
	}
}

func TestShutdownClosesInFlightConnections(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(v1.IPv4Protocol, logger)

	backend := startTCPEchoServer(t, "tcp4", "127.0.0.1:0")
	defer backend.Close()
	backendPort := backend.Addr().(*net.TCPAddr).Port

	ep := &testEndpoint{ip: "127.0.0.1", port: backendPort}

	fl, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nodePort := fl.Addr().(*net.TCPAddr).Port
	fl.Close()

	key := fmt.Sprintf("tcp/%d", nodePort)
	p.SyncNodePorts(map[string]*NodePortSpec{
		key: {
			ServicePortName: makeServicePortName("default", "inflight-svc", "http"),
			Protocol:        v1.ProtocolTCP,
			Port:            nodePort,
			Endpoints:       []proxy.Endpoint{ep},
		},
	})

	// Open an idle in-flight connection through the proxy.
	conn, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Give handleTCPConn time to dial the backend and enter io.Copy.
	time.Sleep(100 * time.Millisecond)

	p.Shutdown()

	// After shutdown the in-flight connection must be torn down; a blocking
	// Read should return promptly (EOF / use of closed / reset), not block.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("Expected connection to be closed after Shutdown, got nil error")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("Expected connection close after Shutdown, got read timeout: %v", err)
	}
}

func TestIPv6(t *testing.T) {
	// Check if IPv6 loopback is available
	l, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback not available: %v", err)
	}
	l.Close()

	logger, _ := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(v1.IPv6Protocol, logger)
	defer p.Shutdown()

	if p.listenIP != "::1" {
		t.Errorf("Expected listenIP '::1', got %q", p.listenIP)
	}
	if p.network != "tcp6" {
		t.Errorf("Expected network 'tcp6', got %q", p.network)
	}

	// Start an IPv6 backend
	backend := startTCPEchoServer(t, "tcp6", "[::1]:0")
	defer backend.Close()
	backendPort := backend.Addr().(*net.TCPAddr).Port

	ep := &testEndpoint{ip: "::1", port: backendPort}

	fl, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatal(err)
	}
	nodePort := fl.Addr().(*net.TCPAddr).Port
	fl.Close()

	key := fmt.Sprintf("tcp/%d", nodePort)
	p.SyncNodePorts(map[string]*NodePortSpec{
		key: {
			ServicePortName: makeServicePortName("default", "v6-svc", "http"),
			Protocol:        v1.ProtocolTCP,
			Port:            nodePort,
			Endpoints:       []proxy.Endpoint{ep},
		},
	})

	conn, err := net.DialTimeout("tcp6", fmt.Sprintf("[::1]:%d", nodePort), 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to IPv6 nodeport proxy: %v", err)
	}
	testMsg := "hello ipv6"
	fmt.Fprint(conn, testMsg)
	conn.(*net.TCPConn).CloseWrite()
	buf, err := io.ReadAll(conn)
	conn.Close()
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	if string(buf) != testMsg {
		t.Errorf("Expected %q, got %q", testMsg, string(buf))
	}
}

func TestNoEndpoints(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(v1.IPv4Protocol, logger)
	defer p.Shutdown()

	fl, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nodePort := fl.Addr().(*net.TCPAddr).Port
	fl.Close()

	key := fmt.Sprintf("tcp/%d", nodePort)
	p.SyncNodePorts(map[string]*NodePortSpec{
		key: {
			ServicePortName: makeServicePortName("default", "empty-svc", "http"),
			Protocol:        v1.ProtocolTCP,
			Port:            nodePort,
			Endpoints:       []proxy.Endpoint{},
		},
	})

	// Should still create listener, but connections get closed immediately
	conn, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", nodePort), 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	buf, _ := io.ReadAll(conn)
	conn.Close()

	if len(buf) != 0 {
		t.Errorf("Expected empty response with no endpoints, got %q", string(buf))
	}
}

// startPortReportingServer starts a TCP server that sends its own port number
// to each connecting client.
func startPortReportingServer(t *testing.T, network string) (net.Listener, int, proxy.Endpoint) {
	t.Helper()
	addr := "127.0.0.1:0"
	if network == "tcp6" {
		addr = "[::1]:0"
	}
	l, err := net.Listen(network, addr)
	if err != nil {
		t.Fatalf("Failed to start backend: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				fmt.Fprintf(c, "port:%d", port)
			}(conn)
		}
	}()
	ip := "127.0.0.1"
	if network == "tcp6" {
		ip = "::1"
	}
	return l, port, &testEndpoint{ip: ip, port: port}
}

func readBackendID(t *testing.T, network, addr string) string {
	t.Helper()
	conn, err := net.DialTimeout(network, addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	buf, err := io.ReadAll(conn)
	conn.Close()
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}
	return string(buf)
}

func TestSessionAffinity_PinsToSingleEndpoint(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(v1.IPv4Protocol, logger)
	defer p.Shutdown()

	// Start 3 backends; each reports its own port
	var backends []net.Listener
	var endpoints []proxy.Endpoint
	for range 3 {
		b, _, ep := startPortReportingServer(t, "tcp4")
		backends = append(backends, b)
		endpoints = append(endpoints, ep)
	}
	defer func() {
		for _, b := range backends {
			b.Close()
		}
	}()

	fl, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nodePort := fl.Addr().(*net.TCPAddr).Port
	fl.Close()

	key := fmt.Sprintf("tcp/%d", nodePort)
	p.SyncNodePorts(map[string]*NodePortSpec{
		key: {
			ServicePortName:     makeServicePortName("default", "sticky-svc", "http"),
			Protocol:            v1.ProtocolTCP,
			Port:                nodePort,
			Endpoints:           endpoints,
			SessionAffinityType: v1.ServiceAffinityClientIP,
			StickyMaxAgeSeconds: 10800,
		},
	})

	addr := fmt.Sprintf("127.0.0.1:%d", nodePort)
	first := readBackendID(t, "tcp4", addr)
	for range 10 {
		got := readBackendID(t, "tcp4", addr)
		if got != first {
			t.Fatalf("SessionAffinity ClientIP: expected all requests to hit %q, got %q", first, got)
		}
	}
}

func TestSessionAffinity_PinnedEndpointRemoved(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(v1.IPv4Protocol, logger)
	defer p.Shutdown()

	b1, _, ep1 := startPortReportingServer(t, "tcp4")
	defer b1.Close()
	b2, _, ep2 := startPortReportingServer(t, "tcp4")
	defer b2.Close()

	fl, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nodePort := fl.Addr().(*net.TCPAddr).Port
	fl.Close()

	key := fmt.Sprintf("tcp/%d", nodePort)
	svcName := makeServicePortName("default", "sticky-svc", "http")
	p.SyncNodePorts(map[string]*NodePortSpec{
		key: {
			ServicePortName:     svcName,
			Protocol:            v1.ProtocolTCP,
			Port:                nodePort,
			Endpoints:           []proxy.Endpoint{ep1, ep2},
			SessionAffinityType: v1.ServiceAffinityClientIP,
			StickyMaxAgeSeconds: 10800,
		},
	})

	addr := fmt.Sprintf("127.0.0.1:%d", nodePort)
	pinned := readBackendID(t, "tcp4", addr)

	// Drop the pinned endpoint from the set; remaining traffic must flow to
	// the surviving endpoint rather than silently dropping.
	remaining := ep2
	if pinned == fmt.Sprintf("port:%d", ep2.(*testEndpoint).port) {
		remaining = ep1
	}
	p.SyncNodePorts(map[string]*NodePortSpec{
		key: {
			ServicePortName:     svcName,
			Protocol:            v1.ProtocolTCP,
			Port:                nodePort,
			Endpoints:           []proxy.Endpoint{remaining},
			SessionAffinityType: v1.ServiceAffinityClientIP,
			StickyMaxAgeSeconds: 10800,
		},
	})

	got := readBackendID(t, "tcp4", addr)
	want := fmt.Sprintf("port:%d", remaining.(*testEndpoint).port)
	if got != want {
		t.Fatalf("After pinned endpoint removal: expected %q, got %q", want, got)
	}
}

func TestSessionAffinity_Expires(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	p := NewLocalNodePortProxy(v1.IPv4Protocol, logger)
	defer p.Shutdown()

	var backends []net.Listener
	var endpoints []proxy.Endpoint
	for range 3 {
		b, _, ep := startPortReportingServer(t, "tcp4")
		backends = append(backends, b)
		endpoints = append(endpoints, ep)
	}
	defer func() {
		for _, b := range backends {
			b.Close()
		}
	}()

	fl, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nodePort := fl.Addr().(*net.TCPAddr).Port
	fl.Close()

	// StickyMaxAgeSeconds is in seconds; we can't use fractional values via the
	// public spec, so poke the listener directly after construction.
	key := fmt.Sprintf("tcp/%d", nodePort)
	p.SyncNodePorts(map[string]*NodePortSpec{
		key: {
			ServicePortName:     makeServicePortName("default", "sticky-svc", "http"),
			Protocol:            v1.ProtocolTCP,
			Port:                nodePort,
			Endpoints:           endpoints,
			SessionAffinityType: v1.ServiceAffinityClientIP,
			StickyMaxAgeSeconds: 10800,
		},
	})

	p.mu.Lock()
	p.active[key].mu.Lock()
	p.active[key].affinityTimeout = 50 * time.Millisecond
	p.active[key].mu.Unlock()
	p.mu.Unlock()

	addr := fmt.Sprintf("127.0.0.1:%d", nodePort)
	first := readBackendID(t, "tcp4", addr)
	// Within the window, stays pinned.
	if got := readBackendID(t, "tcp4", addr); got != first {
		t.Fatalf("Before expiry: expected %q, got %q", first, got)
	}
	time.Sleep(100 * time.Millisecond)
	// After expiry, round-robin may move us elsewhere. Repeat a few times to
	// make it overwhelmingly likely we observe a different endpoint.
	sawOther := false
	for range 10 {
		if got := readBackendID(t, "tcp4", addr); got != first {
			sawOther = true
			break
		}
	}
	if !sawOther {
		t.Fatalf("After expiry: never saw a different backend than %q", first)
	}
}
