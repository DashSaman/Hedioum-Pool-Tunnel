package ingress

import (
	"testing"
	"time"

	"github.com/hedioum/Hedioum-Pool-Tunnel/config"
)

func TestPerformanceEndpointSet(t *testing.T) {
	perf := []config.Endpoint{
		{Target: "1.2.3.4:2083", Mimic: "cpanel"},
		{Target: "1.2.3.4:443", Mimic: "tls"},
		{Target: "1.2.3.4:993", Mimic: "imaps"},
	}
	if !performanceEndpointSet(perf) {
		t.Fatal("implicit-TLS set should be recognized as performance")
	}
	if performanceEndpointSet(append(perf, config.Endpoint{Target: "1.2.3.4:22", Mimic: "ssh"})) {
		t.Fatal("SSH-containing set must not use performance ordering")
	}
}

func TestPerformanceOrderHonorsOptimizedConfigAndCooldown(t *testing.T) {
	n := config.ForeignNode{Endpoints: []config.Endpoint{
		{Target: "1.2.3.4:2083", Mimic: "cpanel"}, // benchmark winner
		{Target: "1.2.3.4:443", Mimic: "tls"},
		{Target: "1.2.3.4:993", Mimic: "imaps"},
	}}
	d := newEndpointDialer(n)
	order := d.attemptOrder()
	if len(order) != 3 || order[0].Mimic != "cpanel" || order[1].Mimic != "tls" {
		t.Fatalf("performance order did not honor config: %+v", order)
	}

	d.health[n.Endpoints[0].Target] = &epHealth{fails: 2, cooldownUntil: time.Now().Add(time.Minute)}
	order = d.attemptOrder()
	if order[0].Mimic == "cpanel" || order[len(order)-1].Mimic != "cpanel" {
		t.Fatalf("cooling endpoint should move to the end: %+v", order)
	}
}
