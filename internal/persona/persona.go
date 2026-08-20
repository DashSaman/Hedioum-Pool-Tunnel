// Package persona groups the mimic library into coherent server identities.
package persona

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

// A Persona is a coherent identity. Backbone is optional. Legacy personas keep
// SSH as a backbone, while the performance persona intentionally has no SSH
// dependency at all. Core mimics define the identity and are always present; Pool
// is deterministically filled until Size non-backbone mimics are selected.
type Persona struct {
	Name     string
	Backbone string
	Core     []string
	Pool     []string
	Size     int
}

// Performance is the production-oriented default for the DashSaman fork. It uses
// only implicit-TLS transports, so OpenSSH is never touched and the hot path uses
// a single TLS crypto layer rather than the SSH-mimic securestream framing path.
// The set deliberately keeps multiple conventional TLS ports for failover/racing.
var Registry = map[string]Persona{
	"performance": {
		Name:     "performance",
		Backbone: "",
		Core: []string{
			"tls", "https-alt", "smtps", "imaps", "docker",
			"grafana", "prometheus", "cpanel", "whm", "webmail",
		},
		Size: 10,
	},
	"cpanel": {
		Name:     "cpanel",
		Backbone: "ssh",
		Core:     []string{"tls", "cpanel", "whm", "webmail"},
		Pool:     []string{"https-alt", "smtp", "smtps", "imaps", "imap", "postgres", "mysql", "docker"},
		Size:     9,
	},
	"directadmin": {
		Name:     "directadmin",
		Backbone: "ssh",
		Core:     []string{"tls", "directadmin"},
		Pool:     []string{"https-alt", "smtp", "smtps", "imaps", "imap", "postgres", "mysql", "docker", "grafana"},
		Size:     9,
	},
	"devops": {
		Name:     "devops",
		Backbone: "ssh",
		Core:     []string{"tls", "https-alt", "docker", "grafana", "prometheus"},
		Pool:     []string{"postgres", "mysql", "smtp", "smtps", "imap", "imaps"},
		Size:     9,
	},
}

var order = []string{"performance", "cpanel", "directadmin", "devops"}

func Names() []string {
	out := make([]string, len(order))
	copy(out, order)
	return out
}

func Known(name string) bool {
	_, ok := Registry[name]
	return ok
}

// Auto intentionally selects the SSH-free performance persona in this fork.
// Operators who explicitly want a legacy camouflage persona can still select it
// by name, but an unattended/default setup can never require moving OpenSSH.
func Auto(seed string) string {
	_ = seed
	return "performance"
}

// Resolve returns the ordered mimic set. The optional backbone is first, followed
// by Core and deterministic Pool fill. Size counts non-backbone mimics so legacy
// personas remain shape-compatible (ssh + 9), while performance resolves to ten
// TLS-family mimics and no SSH endpoint.
func Resolve(name, seed string) ([]string, error) {
	p, ok := Registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown persona %q (want one of %v)", name, order)
	}
	set := make([]string, 0, p.Size+1)
	seen := make(map[string]bool, p.Size+1)
	backboneCount := 0
	if p.Backbone != "" {
		set = append(set, p.Backbone)
		seen[p.Backbone] = true
		backboneCount = 1
	}
	for _, m := range p.Core {
		if !seen[m] {
			set = append(set, m)
			seen[m] = true
		}
	}
	for _, m := range seededOrder(p.Pool, seed) {
		if len(set)-backboneCount >= p.Size {
			break
		}
		if !seen[m] {
			set = append(set, m)
			seen[m] = true
		}
	}
	if len(set)-backboneCount != p.Size {
		return nil, fmt.Errorf("persona %q: pool too small to reach size %d", name, p.Size)
	}
	return set, nil
}

// CheckCoherence rejects an incoherent mimic set. A real server never runs both
// cPanel-family and DirectAdmin identities on the same host.
func CheckCoherence(mimics []string) error {
	has := make(map[string]bool, len(mimics))
	for _, m := range mimics {
		has[m] = true
	}
	if (has["cpanel"] || has["whm"] || has["webmail"]) && has["directadmin"] {
		return fmt.Errorf("incoherent mimic set: cPanel family and DirectAdmin must not run on the same host")
	}
	return nil
}

func seededOrder(items []string, seed string) []string {
	type kv struct {
		k uint64
		v string
	}
	arr := make([]kv, len(items))
	for i, it := range items {
		h := sha256.Sum256([]byte("hedioum-fill\x00" + seed + "\x00" + it))
		arr[i] = kv{binary.BigEndian.Uint64(h[:8]), it}
	}
	sort.Slice(arr, func(i, j int) bool {
		if arr[i].k != arr[j].k {
			return arr[i].k < arr[j].k
		}
		return arr[i].v < arr[j].v
	})
	out := make([]string, len(items))
	for i, e := range arr {
		out[i] = e.v
	}
	return out
}
