package main

import (
	"strings"
	"testing"
)

func TestRenderUnitCapabilities(t *testing.T) {
	hub := renderUnit(true)
	if !strings.Contains(hub, "CapabilityBoundingSet=CAP_NET_BIND_SERVICE CAP_NET_ADMIN") {
		t.Errorf("hub unit missing CAP_NET_ADMIN in bounding set:\n%s", hub)
	}
	if !strings.Contains(hub, "AmbientCapabilities=CAP_NET_BIND_SERVICE CAP_NET_ADMIN") {
		t.Errorf("hub unit missing CAP_NET_ADMIN in ambient set:\n%s", hub)
	}

	foreign := renderUnit(false)
	if !strings.Contains(foreign, "CapabilityBoundingSet=CAP_NET_BIND_SERVICE\n") {
		t.Errorf("foreign bounding set must be CAP_NET_BIND_SERVICE only:\n%s", foreign)
	}
	if !strings.Contains(foreign, "AmbientCapabilities=CAP_NET_BIND_SERVICE\n") {
		t.Errorf("foreign ambient set must be CAP_NET_BIND_SERVICE only:\n%s", foreign)
	}
	if strings.Contains(foreign, "CAP_NET_BIND_SERVICE CAP_NET_ADMIN") {
		t.Errorf("foreign unit must NOT grant CAP_NET_ADMIN on a directive line:\n%s", foreign)
	}
	for _, u := range []string{hub, foreign} {
		if strings.Contains(u, "{{") || strings.Contains(u, "}}") {
			t.Errorf("unrendered template directive left in unit:\n%s", u)
		}
	}
}

func TestNetworkSysctlConfigIsSymmetricHighBDP(t *testing.T) {
	for _, want := range []string{
		"net.core.default_qdisc=fq",
		"net.ipv4.tcp_congestion_control=bbr",
		"net.core.rmem_max=33554432",
		"net.core.wmem_max=33554432",
		"net.ipv4.tcp_rmem=4096 262144 33554432",
		"net.ipv4.tcp_wmem=4096 262144 33554432",
		"net.ipv4.tcp_mtu_probing=1",
	} {
		if !strings.Contains(networkSysctlConfig, want) {
			t.Errorf("network tuning missing %q", want)
		}
	}
}
