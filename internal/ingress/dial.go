package ingress

import (
	"context"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/hedioum/Hedioum-Pool-Tunnel/config"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/mimic"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/muxcfg"
)

const dialRaceStagger = 125 * time.Millisecond

func hubYamuxConfig() *yamux.Config { return muxcfg.WAN() }

func clientMimicFor(ep config.Endpoint, token string) mimic.ClientMimic {
	switch ep.Mimic {
	case "tls", "smtps", "imaps", "directadmin", "https-alt", "docker", "grafana", "prometheus",
		"cpanel", "whm", "webmail":
		return &mimic.TLSClient{Token: token, ServerName: ep.ServerName}
	case "smtp", "imap", "postgres", "mysql":
		return &mimic.StartTLSClient{Proto: ep.Mimic, TLS: &mimic.TLSClient{Token: token, ServerName: ep.ServerName}}
	default:
		return &mimic.SSHClient{Token: token}
	}
}

// DialEndpoint is the public non-cancellable convenience wrapper used by CLI
// probes/speedtests. Pool races use DialEndpointContext so losing handshakes can be
// aborted as soon as another endpoint wins.
func DialEndpoint(ep config.Endpoint, token string, cfg *yamux.Config) (*yamux.Session, error) {
	return DialEndpointContext(context.Background(), ep, token, cfg)
}

func DialEndpointContext(ctx context.Context, ep config.Endpoint, token string, cfg *yamux.Config) (*yamux.Session, error) {
	dialer := net.Dialer{Timeout: 8 * time.Second, KeepAlive: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", ep.Target)
	if err != nil {
		return nil, err
	}

	// Mimic handshakes do not take a context directly. Close the underlying socket
	// on cancellation so a losing TLS/STARTTLS/SSH race wakes immediately rather
	// than holding an FD and CPU until its independent handshake deadline expires.
	stopCancelWatch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopCancelWatch:
		}
	}()
	defer close(stopCancelWatch)

	secureConn, err := clientMimicFor(ep, token).Dial(conn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("%s mimic handshake failed: %w", ep.Mimic, err)
	}
	if err := ctx.Err(); err != nil {
		_ = secureConn.Close()
		return nil, err
	}
	session, err := yamux.Client(secureConn, cfg)
	if err != nil {
		_ = secureConn.Close()
		return nil, err
	}
	return session, nil
}

func ProbeEndpoint(ep config.Endpoint, token string) (time.Duration, error) {
	sess, err := DialEndpoint(ep, token, hubYamuxConfig())
	if err != nil {
		return 0, err
	}
	defer sess.Close()
	return sess.Ping()
}

type endpointDialer struct {
	node    config.ForeignNode
	cfg     *yamux.Config
	weights []float64

	mu     sync.Mutex
	health map[string]*epHealth
}

type epHealth struct {
	fails         int
	cooldownUntil time.Time
}

const (
	dialMaxAttempts = 4
	epFailThreshold = 2
	epCooldownBase  = 60 * time.Second
	epCooldownMax   = 10 * time.Minute
)

var dialPriority = map[string]int{
	"tls": 0, "https-alt": 1,
	"smtps": 2, "imaps": 2, "directadmin": 2, "cpanel": 2, "whm": 2, "webmail": 2,
	"grafana": 2, "prometheus": 2, "docker": 2,
	"smtp": 3, "imap": 3, "postgres": 4, "mysql": 4, "ssh": 5,
}

func dialRank(m string) int {
	if r, ok := dialPriority[m]; ok {
		return r
	}
	return 3
}

func newEndpointDialer(node config.ForeignNode) *endpointDialer {
	w := make([]float64, len(node.Endpoints))
	for i := range w {
		w[i] = 0.3 + mrand.Float64()
	}
	return &endpointDialer{node: node, cfg: hubYamuxConfig(), weights: w, health: map[string]*epHealth{}}
}

func (d *endpointDialer) attemptOrder() []config.Endpoint {
	eps := d.node.Endpoints
	if len(eps) <= 1 {
		return eps
	}
	now := time.Now()
	d.mu.Lock()
	var reachable, cooling []int
	for i, ep := range eps {
		if h := d.health[ep.Target]; h != nil && h.cooldownUntil.After(now) {
			cooling = append(cooling, i)
		} else {
			reachable = append(reachable, i)
		}
	}
	d.mu.Unlock()

	pool := reachable
	if len(pool) == 0 {
		pool, cooling = cooling, nil
	}
	if len(pool) == 0 {
		return nil
	}

	primary := d.weightedPick(pool)
	seen := map[int]bool{primary: true}
	order := []int{primary}

	rest := append([]int(nil), pool...)
	sort.SliceStable(rest, func(a, b int) bool {
		return dialRank(eps[rest[a]].Mimic) < dialRank(eps[rest[b]].Mimic)
	})
	for _, i := range rest {
		if !seen[i] {
			order = append(order, i)
			seen[i] = true
		}
	}
	order = append(order, cooling...)
	if len(order) > dialMaxAttempts {
		order = order[:dialMaxAttempts]
	}
	out := make([]config.Endpoint, len(order))
	for i, idx := range order {
		out[i] = eps[idx]
	}
	return out
}

func (d *endpointDialer) weightedPick(cand []int) int {
	if len(cand) == 1 {
		return cand[0]
	}
	total := 0.0
	for _, i := range cand {
		total += d.weights[i]
	}
	r := mrand.Float64() * total
	for _, i := range cand {
		if r -= d.weights[i]; r <= 0 {
			return i
		}
	}
	return cand[len(cand)-1]
}

func (d *endpointDialer) recordSuccess(target string) {
	d.mu.Lock()
	if h := d.health[target]; h != nil {
		h.fails, h.cooldownUntil = 0, time.Time{}
	}
	d.mu.Unlock()
}

func (d *endpointDialer) recordFailure(target string) {
	d.mu.Lock()
	h := d.health[target]
	if h == nil {
		h = &epHealth{}
		d.health[target] = h
	}
	h.fails++
	if h.fails >= epFailThreshold {
		back := epCooldownBase << uint(h.fails-epFailThreshold)
		if back <= 0 || back > epCooldownMax {
			back = epCooldownMax
		}
		h.cooldownUntil = time.Now().Add(back)
	}
	d.mu.Unlock()
}

type dialResult struct {
	session *yamux.Session
	mimic   string
	target  string
	err     error
}

// dial performs a staggered parallel race. A shared context is cancelled when the
// first endpoint wins, which aborts in-flight loser TCP/mimic handshakes instead of
// allowing them to consume descriptors for their full timeout.
func (d *endpointDialer) dial() (*yamux.Session, string, error) {
	attempts := d.attemptOrder()
	if len(attempts) == 0 {
		return nil, "", fmt.Errorf("node %q has no endpoints to dial", d.node.Alias)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan dialResult)
	done := make(chan struct{})

	for i, ep := range attempts {
		delay := time.Duration(i) * dialRaceStagger
		go func(ep config.Endpoint, delay time.Duration) {
			if delay > 0 {
				t := time.NewTimer(delay)
				defer t.Stop()
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
			}

			session, err := DialEndpointContext(ctx, ep, d.node.AuthToken, d.cfg)
			if err != nil {
				// Cancellation means another endpoint already won; do not poison this
				// endpoint's health score for a race it intentionally lost.
				if ctx.Err() != nil {
					return
				}
				d.recordFailure(ep.Target)
				select {
				case results <- dialResult{mimic: ep.Mimic, target: ep.Target, err: err}:
				case <-ctx.Done():
				}
				return
			}

			d.recordSuccess(ep.Target)
			select {
			case results <- dialResult{session: session, mimic: ep.Mimic, target: ep.Target}:
			case <-done:
				_ = session.Close()
			case <-ctx.Done():
				_ = session.Close()
			}
		}(ep, delay)
	}

	var lastErr error
	for range attempts {
		r := <-results
		if r.err != nil {
			slog.Warn("pipe dial failed", "node", d.node.Alias, "mimic", r.mimic, "target", r.target, "err", r.err)
			lastErr = r.err
			continue
		}

		close(done)
		cancel()
		slog.Info("pipe established", "node", d.node.Alias, "mimic", r.mimic, "target", r.target)
		go keepAlive(r.session)
		return r.session, r.mimic, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("node %q: all endpoint attempts failed", d.node.Alias)
	}
	return nil, "", lastErr
}

func keepAlive(s *yamux.Session) {
	for {
		if s.IsClosed() {
			return
		}
		time.Sleep(time.Duration(mrand.IntN(26)+20) * time.Second)
		if _, err := s.Ping(); err != nil {
			_ = s.Close()
			return
		}
	}
}
