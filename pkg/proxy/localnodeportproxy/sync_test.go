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
	"testing"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/proxy"
	"k8s.io/utils/ptr"
)

const (
	testNS    = "test-ns"
	testNode  = "node-a"
	clusterIP = "10.96.0.1"
)

// buildMaps runs the real ServiceChangeTracker and EndpointsChangeTracker over
// the given objects so the resulting ServicePortMap/EndpointsMap match what a
// live proxier would hand to BuildDesiredNodePorts.
func buildMaps(t *testing.T, svcs []*v1.Service, slices []*discovery.EndpointSlice) (proxy.ServicePortMap, proxy.EndpointsMap) {
	t.Helper()
	sct := proxy.NewServiceChangeTracker(v1.IPv4Protocol, nil, nil)
	for _, svc := range svcs {
		sct.Update(nil, svc)
	}
	svcPortMap := make(proxy.ServicePortMap)
	_ = svcPortMap.Update(sct)

	ect := proxy.NewEndpointsChangeTracker(v1.IPv4Protocol, testNode, nil, nil)
	for _, eps := range slices {
		ect.EndpointSliceUpdate(eps, false)
	}
	endpointsMap := make(proxy.EndpointsMap)
	_ = endpointsMap.Update(ect)
	return svcPortMap, endpointsMap
}

func buildService(name string, mutate func(*v1.Service)) *v1.Service {
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: v1.ServiceSpec{
			ClusterIP: clusterIP,
			Type:      v1.ServiceTypeNodePort,
			Ports: []v1.ServicePort{{
				Name:     "http",
				Port:     80,
				Protocol: v1.ProtocolTCP,
				NodePort: 30080,
			}},
		},
	}
	if mutate != nil {
		mutate(svc)
	}
	return svc
}

func buildEndpointSlice(svcName string, endpoints []discovery.Endpoint, ports []discovery.EndpointPort) *discovery.EndpointSlice {
	return &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName + "-0",
			Namespace: testNS,
			Labels:    map[string]string{discovery.LabelServiceName: svcName},
		},
		AddressType: discovery.AddressTypeIPv4,
		Endpoints:   endpoints,
		Ports:       ports,
	}
}

func readyEndpoint(ip, node string) discovery.Endpoint {
	ep := discovery.Endpoint{
		Addresses:  []string{ip},
		Conditions: discovery.EndpointConditions{Ready: ptr.To(true), Serving: ptr.To(true)},
	}
	if node != "" {
		ep.NodeName = ptr.To(node)
	}
	return ep
}

func notReadyEndpoint(ip, node string) discovery.Endpoint {
	ep := discovery.Endpoint{
		Addresses:  []string{ip},
		Conditions: discovery.EndpointConditions{Ready: ptr.To(false), Serving: ptr.To(false)},
	}
	if node != "" {
		ep.NodeName = ptr.To(node)
	}
	return ep
}

func httpTCPPort() []discovery.EndpointPort {
	return []discovery.EndpointPort{{
		Name:     ptr.To("http"),
		Port:     ptr.To(int32(8080)),
		Protocol: ptr.To(v1.ProtocolTCP),
	}}
}

func TestBuildDesiredNodePorts_ServiceWithoutNodePort(t *testing.T) {
	svc := buildService("no-np", func(s *v1.Service) {
		s.Spec.Type = v1.ServiceTypeClusterIP
		s.Spec.Ports[0].NodePort = 0
	})
	slice := buildEndpointSlice("no-np",
		[]discovery.Endpoint{readyEndpoint("10.0.0.1", "node-b")},
		httpTCPPort())

	svcMap, epMap := buildMaps(t, []*v1.Service{svc}, []*discovery.EndpointSlice{slice})
	got := BuildDesiredNodePorts(svcMap, epMap, testNode, nil)
	if len(got) != 0 {
		t.Fatalf("Expected no entries for service without NodePort, got %+v", got)
	}
}

func TestBuildDesiredNodePorts_ReadyEndpoint(t *testing.T) {
	svc := buildService("basic", nil)
	slice := buildEndpointSlice("basic",
		[]discovery.Endpoint{readyEndpoint("10.0.0.1", "node-b")},
		httpTCPPort())

	svcMap, epMap := buildMaps(t, []*v1.Service{svc}, []*discovery.EndpointSlice{slice})
	got := BuildDesiredNodePorts(svcMap, epMap, testNode, nil)
	spec, ok := got["tcp/30080"]
	if !ok {
		t.Fatalf("Expected key tcp/30080, got %+v", got)
	}
	if spec.port != 30080 {
		t.Errorf("spec.port = %d, want 30080", spec.port)
	}
	if spec.protocol != v1.ProtocolTCP {
		t.Errorf("spec.protocol = %s, want TCP", spec.protocol)
	}
	if len(spec.endpoints) != 1 || spec.endpoints[0] != "10.0.0.1:8080" {
		t.Errorf("unexpected endpoints: %+v", spec.endpoints)
	}
}

func TestBuildDesiredNodePorts_NoEndpoints(t *testing.T) {
	svc := buildService("empty", nil)
	slice := buildEndpointSlice("empty", nil, httpTCPPort())

	svcMap, epMap := buildMaps(t, []*v1.Service{svc}, []*discovery.EndpointSlice{slice})
	got := BuildDesiredNodePorts(svcMap, epMap, testNode, nil)
	if len(got) != 0 {
		t.Fatalf("Expected no entries with no endpoints, got %+v", got)
	}
}

func TestBuildDesiredNodePorts_OnlyNotReadyEndpoints(t *testing.T) {
	svc := buildService("not-ready", nil)
	slice := buildEndpointSlice("not-ready",
		[]discovery.Endpoint{notReadyEndpoint("10.0.0.1", "node-b")},
		httpTCPPort())

	svcMap, epMap := buildMaps(t, []*v1.Service{svc}, []*discovery.EndpointSlice{slice})
	got := BuildDesiredNodePorts(svcMap, epMap, testNode, nil)
	if len(got) != 0 {
		t.Fatalf("Expected no entries when no endpoint is ready/serving, got %+v", got)
	}
}

func TestBuildDesiredNodePorts_ExternalPolicyLocal_WithLocalEndpoint(t *testing.T) {
	svc := buildService("local", func(s *v1.Service) {
		s.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
	})
	slice := buildEndpointSlice("local",
		[]discovery.Endpoint{
			readyEndpoint("10.0.0.1", testNode), // local
			readyEndpoint("10.0.0.2", "node-b"), // remote
		},
		httpTCPPort())

	svcMap, epMap := buildMaps(t, []*v1.Service{svc}, []*discovery.EndpointSlice{slice})
	got := BuildDesiredNodePorts(svcMap, epMap, testNode, nil)
	spec, ok := got["tcp/30080"]
	if !ok {
		t.Fatalf("Expected key tcp/30080, got %+v", got)
	}
	if len(spec.endpoints) != 1 || spec.endpoints[0] != "10.0.0.1:8080" {
		t.Fatalf("ExternalPolicyLocal: expected only the local endpoint 10.0.0.1:8080, got %+v", spec.endpoints)
	}
}

func TestBuildDesiredNodePorts_ExternalPolicyLocal_NoLocalEndpoint(t *testing.T) {
	svc := buildService("local-only-remote", func(s *v1.Service) {
		s.Spec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyLocal
	})
	slice := buildEndpointSlice("local-only-remote",
		[]discovery.Endpoint{readyEndpoint("10.0.0.2", "node-b")},
		httpTCPPort())

	svcMap, epMap := buildMaps(t, []*v1.Service{svc}, []*discovery.EndpointSlice{slice})
	got := BuildDesiredNodePorts(svcMap, epMap, testNode, nil)
	if len(got) != 0 {
		t.Fatalf("ExternalPolicyLocal with no local endpoint should produce no entry, got %+v", got)
	}
}

func TestBuildDesiredNodePorts_MixedTCPAndUDP(t *testing.T) {
	svc := buildService("mixed", func(s *v1.Service) {
		s.Spec.Ports = []v1.ServicePort{
			{Name: "http", Port: 80, Protocol: v1.ProtocolTCP, NodePort: 30080},
			{Name: "dns", Port: 53, Protocol: v1.ProtocolUDP, NodePort: 30053},
		}
	})
	slice := buildEndpointSlice("mixed",
		[]discovery.Endpoint{readyEndpoint("10.0.0.1", "node-b")},
		[]discovery.EndpointPort{
			{Name: ptr.To("http"), Port: ptr.To(int32(8080)), Protocol: ptr.To(v1.ProtocolTCP)},
			{Name: ptr.To("dns"), Port: ptr.To(int32(5353)), Protocol: ptr.To(v1.ProtocolUDP)},
		})

	svcMap, epMap := buildMaps(t, []*v1.Service{svc}, []*discovery.EndpointSlice{slice})
	got := BuildDesiredNodePorts(svcMap, epMap, testNode, nil)

	tcp, ok := got["tcp/30080"]
	if !ok || tcp.protocol != v1.ProtocolTCP {
		t.Errorf("Expected tcp/30080 with TCP protocol, got %+v", got)
	}
	udp, ok := got["udp/30053"]
	if !ok || udp.protocol != v1.ProtocolUDP {
		t.Errorf("Expected udp/30053 with UDP protocol, got %+v", got)
	}
}

func TestBuildDesiredNodePorts_SessionAffinityPropagated(t *testing.T) {
	svc := buildService("sticky", func(s *v1.Service) {
		s.Spec.SessionAffinity = v1.ServiceAffinityClientIP
		s.Spec.SessionAffinityConfig = &v1.SessionAffinityConfig{
			ClientIP: &v1.ClientIPConfig{TimeoutSeconds: ptr.To(int32(7200))},
		}
	})
	slice := buildEndpointSlice("sticky",
		[]discovery.Endpoint{readyEndpoint("10.0.0.1", "node-b")},
		httpTCPPort())

	svcMap, epMap := buildMaps(t, []*v1.Service{svc}, []*discovery.EndpointSlice{slice})
	got := BuildDesiredNodePorts(svcMap, epMap, testNode, nil)
	spec, ok := got["tcp/30080"]
	if !ok {
		t.Fatalf("Expected key tcp/30080, got %+v", got)
	}
	if spec.sessionAffinityType != v1.ServiceAffinityClientIP {
		t.Errorf("sessionAffinityType = %q, want ClientIP", spec.sessionAffinityType)
	}
	if spec.stickyMaxAgeSeconds != 7200 {
		t.Errorf("stickyMaxAgeSeconds = %d, want 7200", spec.stickyMaxAgeSeconds)
	}
}

func TestBuildDesiredNodePorts_NoAffinityYieldsZeroTimeout(t *testing.T) {
	svc := buildService("no-affinity", func(s *v1.Service) {
		s.Spec.SessionAffinity = v1.ServiceAffinityNone
	})
	slice := buildEndpointSlice("no-affinity",
		[]discovery.Endpoint{readyEndpoint("10.0.0.1", "node-b")},
		httpTCPPort())

	svcMap, epMap := buildMaps(t, []*v1.Service{svc}, []*discovery.EndpointSlice{slice})
	got := BuildDesiredNodePorts(svcMap, epMap, testNode, nil)
	spec := got["tcp/30080"]
	if spec == nil {
		t.Fatal("expected entry")
	}
	if spec.sessionAffinityType == v1.ServiceAffinityClientIP {
		t.Errorf("unexpected ClientIP affinity when service has none")
	}
	if spec.stickyMaxAgeSeconds != 0 {
		t.Errorf("stickyMaxAgeSeconds = %d, want 0", spec.stickyMaxAgeSeconds)
	}
	if got := spec.affinityTimeout(); got != 0 {
		t.Errorf("affinityTimeout() = %v, want 0 for non-ClientIP service", got)
	}
}

// TestBuildDesiredNodePorts_NodePortCollision documents that if two services
// claim the same proto/nodeport key, the map collapses them — one overwrites
// the other. API validation prevents this in practice, but we assert the
// function itself stays deterministic (doesn't panic, doesn't duplicate).
func TestBuildDesiredNodePorts_NodePortCollision(t *testing.T) {
	svcA := buildService("svc-a", nil)
	svcB := buildService("svc-b", func(s *v1.Service) {
		s.Spec.ClusterIP = "10.96.0.2"
	})
	sliceA := buildEndpointSlice("svc-a",
		[]discovery.Endpoint{readyEndpoint("10.0.0.1", "node-b")},
		httpTCPPort())
	sliceB := buildEndpointSlice("svc-b",
		[]discovery.Endpoint{readyEndpoint("10.0.0.2", "node-b")},
		httpTCPPort())

	svcMap, epMap := buildMaps(t,
		[]*v1.Service{svcA, svcB},
		[]*discovery.EndpointSlice{sliceA, sliceB})
	got := BuildDesiredNodePorts(svcMap, epMap, testNode, nil)
	if len(got) != 1 {
		t.Fatalf("NodePort collision: expected exactly 1 entry (one wins), got %d: %+v", len(got), got)
	}
	spec := got["tcp/30080"]
	if spec == nil {
		t.Fatalf("expected tcp/30080 entry, got %+v", got)
	}
	name := spec.servicePortName.Name
	if name != "svc-a" && name != "svc-b" {
		t.Errorf("winning entry should be svc-a or svc-b, got %q", name)
	}
}
