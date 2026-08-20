package persona

import (
	"strings"
	"testing"
)

func TestResolveShapeAndCoherence(t *testing.T) {
	for _, name := range Names() {
		set, err := Resolve(name, "seed-"+name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(set) != 10 {
			t.Fatalf("%s: got %d mimics, want 10: %v", name, len(set), set)
		}
		seen := map[string]bool{}
		for _, m := range set {
			if seen[m] {
				t.Fatalf("%s: duplicate %q in %v", name, m, set)
			}
			seen[m] = true
		}
		if !seen["tls"] {
			t.Fatalf("%s: every persona must include tls (:443): %v", name, set)
		}
		for _, c := range Registry[name].Core {
			if !seen[c] {
				t.Fatalf("%s: core mimic %q missing: %v", name, c, set)
			}
		}
		if err := CheckCoherence(set); err != nil {
			t.Fatalf("%s: resolved set is incoherent: %v", name, err)
		}
		if Registry[name].Backbone != "" && set[0] != Registry[name].Backbone {
			t.Fatalf("%s: backbone must be first, got %q", name, set[0])
		}
	}
}

func TestPerformancePersonaIsSSHFreeAndImplicitTLSOnly(t *testing.T) {
	set, err := Resolve("performance", "seed")
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"tls": true, "https-alt": true, "smtps": true, "imaps": true,
		"docker": true, "grafana": true, "prometheus": true,
		"cpanel": true, "whm": true, "webmail": true,
	}
	for _, m := range set {
		if m == "ssh" {
			t.Fatal("performance persona must never include ssh")
		}
		if !allowed[m] {
			t.Fatalf("performance persona contains non implicit-TLS mimic %q", m)
		}
	}
}

func TestDeterministic(t *testing.T) {
	a, _ := Resolve("cpanel", "token-A")
	b, _ := Resolve("cpanel", "token-A")
	if strings.Join(a, ",") != strings.Join(b, ",") {
		t.Fatalf("not deterministic:\n%v\n%v", a, b)
	}
}

func TestSeedVariation(t *testing.T) {
	seen := map[string]int{}
	for _, s := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		set, _ := Resolve("cpanel", s)
		seen[strings.Join(set, ",")]++
	}
	if len(seen) < 2 {
		t.Fatalf("expected fill variation across seeds, got only %d distinct sets", len(seen))
	}
}

func TestAutoIsPerformanceAndDeterministic(t *testing.T) {
	for _, seed := range []string{"a", "b", "different-token", "x"} {
		if got := Auto(seed); got != "performance" {
			t.Fatalf("Auto(%q)=%q, want performance", seed, got)
		}
	}
}

func TestCoherenceValidator(t *testing.T) {
	if err := CheckCoherence([]string{"ssh", "tls", "cpanel", "directadmin"}); err == nil {
		t.Fatal("cpanel + directadmin should be rejected as incoherent")
	}
	if err := CheckCoherence([]string{"tls", "whm", "docker"}); err != nil {
		t.Fatalf("coherent performance/cPanel set wrongly rejected: %v", err)
	}
}

func TestUnknownPersona(t *testing.T) {
	if _, err := Resolve("nope", "s"); err == nil {
		t.Fatal("unknown persona should error")
	}
}
