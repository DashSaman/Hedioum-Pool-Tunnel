package pool

import (
	"crypto/sha256"
	"encoding/binary"
	mrand "math/rand/v2"
	"time"
)

const (
	gib = 1 << 30

	// STARTTLS/legacy auxiliary transports may still churn relatively quickly.
	auxMinLifetime = 5 * time.Minute
	auxMaxLifetime = 60 * time.Minute
	auxMinBudget   = 1 * gib
	auxMaxBudget   = 5 * gib

	// Implicit-TLS transports are the high-throughput production path in the
	// performance persona. They must never retire because of byte volume: at
	// hundreds of Mbps a 1-5 GiB budget is consumed in seconds/minutes and causes
	// needless reconnect churn. Rotate only on a long randomized lifetime.
	tlsMinLifetime = 2 * time.Hour
	tlsMaxLifetime = 12 * time.Hour

	sshMinLifetime = 6 * time.Hour
	sshMaxLifetime = 24 * time.Hour
)

type LifecyclePolicy struct {
	auxLifetimeBase time.Duration
	auxBudgetBase   uint64
	tlsLifetimeBase time.Duration
	sshLifetimeBase time.Duration
}

func NewLifecyclePolicy(seed string) LifecyclePolicy {
	h := sha256.Sum256([]byte(seed))
	frac := func(i int) float64 {
		return float64(binary.BigEndian.Uint64(h[i:i+8])) / (1 << 64)
	}
	auxLife := lerpDur(frac(0), auxMinLifetime+10*time.Minute, auxMaxLifetime-15*time.Minute)
	auxBudget := lerpU64(frac(8), 2*gib, 4*gib)
	sshLife := lerpDur(frac(16), sshMinLifetime+4*time.Hour, sshMaxLifetime-6*time.Hour)
	tlsLife := lerpDur(frac(24), tlsMinLifetime+time.Hour, tlsMaxLifetime-2*time.Hour)
	return LifecyclePolicy{
		auxLifetimeBase: auxLife,
		auxBudgetBase:   auxBudget,
		tlsLifetimeBase: tlsLife,
		sshLifetimeBase: sshLife,
	}
}

// isStableTLSMimic is deliberately limited to transports that use the direct
// implicit-TLS path. STARTTLS variants keep the shorter auxiliary policy.
func isStableTLSMimic(m string) bool {
	switch m {
	case "tls", "smtps", "imaps", "https-alt", "directadmin", "docker", "grafana", "prometheus", "cpanel", "whm", "webmail":
		return true
	default:
		return false
	}
}

// roll returns the concrete retirement policy. A byteBudget of 0 means volume is
// never a retirement trigger; only age can rotate the physical connection.
func (p LifecyclePolicy) roll(mimicType string) (retireAfter time.Duration, byteBudget uint64) {
	if mimicType == "ssh" {
		d := scaleDur(p.sshLifetimeBase, 0.5, 1.5)
		return clampDur(d, sshMinLifetime, sshMaxLifetime), 0
	}
	if isStableTLSMimic(mimicType) {
		d := scaleDur(p.tlsLifetimeBase, 0.65, 1.35)
		return clampDur(d, tlsMinLifetime, tlsMaxLifetime), 0
	}
	d := scaleDur(p.auxLifetimeBase, 0.4, 1.6)
	b := scaleU64(p.auxBudgetBase, 0.5, 1.5)
	return clampDur(d, auxMinLifetime, auxMaxLifetime), clampU64(b, auxMinBudget, auxMaxBudget)
}

func jitter(lo, hi float64) float64 { return lo + mrand.Float64()*(hi-lo) }

func scaleDur(base time.Duration, lo, hi float64) time.Duration {
	return time.Duration(float64(base) * jitter(lo, hi))
}
func scaleU64(base uint64, lo, hi float64) uint64 {
	return uint64(float64(base) * jitter(lo, hi))
}

func lerpDur(f float64, lo, hi time.Duration) time.Duration {
	return lo + time.Duration(f*float64(hi-lo))
}
func lerpU64(f float64, lo, hi uint64) uint64 {
	return lo + uint64(f*float64(hi-lo))
}

func clampDur(v, lo, hi time.Duration) time.Duration {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
func clampU64(v, lo, hi uint64) uint64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
