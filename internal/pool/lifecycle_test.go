package pool

import (
	"testing"
	"time"
)

func TestPolicyRollBounds(t *testing.T) {
	p := NewLifecyclePolicy("some-node-auth-token")
	for i := 0; i < 5000; i++ {
		// Implicit TLS: long lifetime and NO byte budget.
		tlsLife, tlsBudget := p.roll("tls")
		if tlsLife < tlsMinLifetime || tlsLife > tlsMaxLifetime {
			t.Fatalf("tls lifetime %v out of [%v,%v]", tlsLife, tlsMinLifetime, tlsMaxLifetime)
		}
		if tlsBudget != 0 {
			t.Fatalf("stable TLS must have no byte budget, got %d", tlsBudget)
		}

		// STARTTLS/legacy auxiliary: short lifetime plus byte budget.
		auxLife, auxBudget := p.roll("smtp")
		if auxLife < auxMinLifetime || auxLife > auxMaxLifetime {
			t.Fatalf("aux lifetime %v out of [%v,%v]", auxLife, auxMinLifetime, auxMaxLifetime)
		}
		if auxBudget < auxMinBudget || auxBudget > auxMaxBudget {
			t.Fatalf("aux budget %d out of [%d,%d]", auxBudget, uint64(auxMinBudget), uint64(auxMaxBudget))
		}

		sshLife, sshBudget := p.roll("ssh")
		if sshLife < sshMinLifetime || sshLife > sshMaxLifetime {
			t.Fatalf("ssh lifetime %v out of [%v,%v]", sshLife, sshMinLifetime, sshMaxLifetime)
		}
		if sshBudget != 0 {
			t.Fatalf("ssh must have no byte budget, got %d", sshBudget)
		}
	}
}

func TestStableTLSMimics(t *testing.T) {
	stable := []string{"tls", "smtps", "imaps", "https-alt", "directadmin", "docker", "grafana", "prometheus", "cpanel", "whm", "webmail"}
	for _, m := range stable {
		if !isStableTLSMimic(m) {
			t.Fatalf("%s should use stable TLS lifecycle", m)
		}
	}
	for _, m := range []string{"ssh", "smtp", "imap", "postgres", "mysql"} {
		if isStableTLSMimic(m) {
			t.Fatalf("%s must not be classified as implicit stable TLS", m)
		}
	}
}

func TestPolicyPerServerPersonality(t *testing.T) {
	a := NewLifecyclePolicy("token-server-A")
	b := NewLifecyclePolicy("token-server-B")
	if a.auxLifetimeBase == b.auxLifetimeBase &&
		a.auxBudgetBase == b.auxBudgetBase &&
		a.tlsLifetimeBase == b.tlsLifetimeBase &&
		a.sshLifetimeBase == b.sshLifetimeBase {
		t.Fatal("two different tokens produced identical personalities")
	}
	a2 := NewLifecyclePolicy("token-server-A")
	if a != a2 {
		t.Fatalf("same token produced different personalities: %+v vs %+v", a, a2)
	}
}

func TestPolicyPerConnectionJitter(t *testing.T) {
	p := NewLifecyclePolicy("jitter-token")
	seen := map[time.Duration]int{}
	for i := 0; i < 200; i++ {
		life, _ := p.roll("tls")
		seen[life]++
	}
	if len(seen) < 50 {
		t.Fatalf("tls lifetimes not varied enough: only %d distinct values", len(seen))
	}
}

func TestShouldRetireByAge(t *testing.T) {
	fresh := &YamuxSession{mimicType: "tls", bornAt: time.Now(), retireAfter: 4 * time.Hour, byteBudget: 0}
	if fresh.ShouldRetire() {
		t.Fatal("a fresh pipe must not retire")
	}
	old := &YamuxSession{mimicType: "tls", bornAt: time.Now().Add(-5 * time.Hour), retireAfter: 4 * time.Hour, byteBudget: 0}
	if !old.ShouldRetire() {
		t.Fatal("a pipe past its lifetime must retire")
	}
}

func TestStableTLSNeverRetiresByBytes(t *testing.T) {
	s := &YamuxSession{mimicType: "tls", bornAt: time.Now(), retireAfter: 8 * time.Hour, byteBudget: 0}
	s.cumulativeBytes = 500 * gib
	if s.ShouldRetire() {
		t.Fatal("stable TLS must not retire on transfer volume")
	}
}

func TestAuxCanRetireByBytes(t *testing.T) {
	s := &YamuxSession{mimicType: "smtp", bornAt: time.Now(), retireAfter: time.Hour, byteBudget: 1 * gib}
	s.cumulativeBytes = 2 * gib
	if !s.ShouldRetire() {
		t.Fatal("auxiliary STARTTLS pipe over byte budget must retire")
	}
}

func TestShouldRetireSSHNoByteBudget(t *testing.T) {
	s := &YamuxSession{mimicType: "ssh", bornAt: time.Now(), retireAfter: 12 * time.Hour, byteBudget: 0}
	s.cumulativeBytes = 500 * gib
	if s.ShouldRetire() {
		t.Fatal("SSH must not retire on volume")
	}
	oldSSH := &YamuxSession{mimicType: "ssh", bornAt: time.Now().Add(-13 * time.Hour), retireAfter: 12 * time.Hour}
	if !oldSSH.ShouldRetire() {
		t.Fatal("SSH must still retire once past its long lifetime")
	}
}

func TestEvaluateHealthRetiresExpiredPipe(t *testing.T) {
	sess, _, err := fakeDialer()
	if err != nil {
		t.Fatalf("fakeDialer: %v", err)
	}
	ys := NewYamuxSession(sess, 10, 2, "tls", NewLifecyclePolicy("tok"))
	ys.bornAt = time.Now().Add(-5 * time.Hour)
	ys.retireAfter = 4 * time.Hour
	np := &NodePool{
		Alias: "n", label: "tcp", minConnections: 0, maxConnections: 5,
		sessions: []*YamuxSession{ys}, shutdown: make(chan struct{}),
	}
	np.evaluateHealthAndScale()
	if !ys.IsDraining() {
		t.Fatal("an expired pipe must shift to Draining")
	}
}

func TestNewYamuxSessionRollsStableTLSPolicy(t *testing.T) {
	p := NewLifecyclePolicy("ctor-token")
	tls := NewYamuxSession(nil, 10, 2, "tls", p)
	if tls.retireAfter < tlsMinLifetime || tls.retireAfter > tlsMaxLifetime {
		t.Fatalf("tls retireAfter %v out of stable TLS bounds", tls.retireAfter)
	}
	if tls.byteBudget != 0 {
		t.Fatal("stable TLS session must have no transfer budget")
	}
	if tls.bornAt.IsZero() {
		t.Fatal("bornAt must be set")
	}
	ssh := NewYamuxSession(nil, 10, 2, "ssh", p)
	if ssh.byteBudget != 0 {
		t.Fatal("ssh session must have no transfer budget")
	}
	if ssh.retireAfter < sshMinLifetime || ssh.retireAfter > sshMaxLifetime {
		t.Fatalf("ssh retireAfter %v out of ssh bounds", ssh.retireAfter)
	}
}
