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
	"strings"

	"k8s.io/kubernetes/pkg/proxy"
)

// BuildDesiredNodePorts builds the desired set of localhost NodePort proxy
// specs from the current service and endpoint state. This is shared by all
// proxier backends (iptables, nftables, IPVS).
func BuildDesiredNodePorts(
	svcPortMap proxy.ServicePortMap,
	endpointsMap proxy.EndpointsMap,
	nodeName string,
	topologyLabels map[string]string,
) map[string]*nodePortSpec {
	desired := make(map[string]*nodePortSpec)
	for svcName, svcInfo := range svcPortMap {
		if svcInfo.NodePort() == 0 {
			continue
		}
		allEndpoints := endpointsMap[svcName]
		clusterEndpoints, localEndpoints, _, hasEndpoints := proxy.CategorizeEndpoints(
			allEndpoints, svcInfo, nodeName, topologyLabels)
		if !hasEndpoints {
			continue
		}
		endpoints := clusterEndpoints
		if svcInfo.ExternalPolicyLocal() {
			endpoints = localEndpoints
		}
		if len(endpoints) == 0 {
			continue
		}
		protocol := strings.ToLower(string(svcInfo.Protocol()))
		key := fmt.Sprintf("%s/%d", protocol, svcInfo.NodePort())
		desired[key] = &nodePortSpec{
			servicePortName:     svcName,
			protocol:            svcInfo.Protocol(),
			port:                svcInfo.NodePort(),
			endpoints:           endpoints,
			sessionAffinityType: svcInfo.SessionAffinityType(),
			stickyMaxAgeSeconds: svcInfo.StickyMaxAgeSeconds(),
		}
	}
	return desired
}
