package ingress

import (
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
)

const (
	yamuxStreamWindow = 16 << 20
	yamuxOpenTimeout  = 20 * time.Second
	yamuxWriteTimeout = 30 * time.Second
	yamuxCloseTimeout = 5 * time.Minute
	yamuxBacklog      = 1024
	dialRaceStagger   = 125 * time.Millisecond
)

// hubYamuxConfig tunes Yamux for high-latency, high-throughput WAN links. The
// egress uses the same window/timeout values so either transfer direction gets
// identical bandwidth-delay-product headroom.
func hubYamuxConfig() *yamux.Config {
	c := yamux.DefaultConfig()
	c.EnableKeepAlive = false // we run a custom randomized heartbeat
	c.AcceptBacklog = yamuxBacklog
	c.ConnectionWriteTimeout = yamuxWriteTimeout
	c.MaxStreamWindowSize = yamuxStreamWindow
	c.StreamOpenTimeout = yamuxOpenTimeout
	c.StreamCloseTimeout = yamuxCloseTimeout
	return c
}

// clientMimicFor builds the client camouflage for an endpoint's mimic type.
func clientMimicFor(ep config.Endpoint, token string) mimic.ClientMimic {
	switch ep.Mimic {
	case "tls", "smtps", "imaps", "directadmin", "https-alt", "docker", "grafana", "prometheus",
		"cpanel", "whm", "webmail":
		return &mimic.TLSClient{Token: token, ServerName: ep.ServerName}
	case "smtp", "imap", "postgres", "mysql":
		return &mimic.StartTLSClient{Proto: ep.Mimic, TLS: &mimic.TLSClient{Token: token, ServerName: ep.ServerName}}
	default: // "ssh"
		return &mimic.SSHClient{Token: token}
	}
}

// DialEndpoint dials one endpoint: TCP connect, mimic handshake, Yamux client.
// Shared by the pool dialer and the speedtest CLI.
func DialEndpoint(ep config.Endpoint, token string, cfg *yamux.Config) (*yamux.Session, error) {
	dialer := net.Dialer{Timeout: 8 * time.Second, KeepAlive: 30 * time.Second}
	conn, err := dialer.Dial("tcp", ep.Target)
	if err != nil {
		return nil, err
	}
	secureConn, err := clientMimicFor(ep, token).Dial(conn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("%s mimic handshake failed: %w", ep.Mimic, err)
	}
	session, err := yamux.Client(secureConn, cfg)
	if err != nil {
		_ = secureConn.Close()
		return nil, err
	}
	return session, nil
}

// ProbeEndpoint dials one endpoint end-to-end (TCP + mimic handshake + yamux) and
// pings it through the tunnel to confirm the egress is actually alive, returning
// the round-trip latency.
func ProbeEndpoint(ep config.Endpoint, token string) (time.Duration, error) {
	sess, err := DialEndpoint(ep, token, hubYamuxConfig())
	if err != nil {
		return 0, err
	}
	defer sess.Close()
	return sess.Ping()
}

// endpointDialer spreads new physical pipes across a node's endpoints with random
// per-node weights and reachability memory. One pool dial uses a staggered parallel
// race, so a black-holed port does not serialize several 8-second timeouts before a
// healthy 443/8443 path is attempted.
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

// attemptOrder returns endpoints best-first: a weighted-random primary for mimic
// diversity, then reachable innocuous ports, then cooling endpoints as last resort.
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

// dial performs a REAL staggered race across candidate endpoints. The first fully
// authenticated Yamux session wins. Losing successful sessions are closed, and
// failed endpoints update cooldown memory.
func (d *endpointDialer) dial() (*yamux.Session, string, error) {
	attempts := d.attemptOrder()
	if len(attempts) == 0 {
		return nil, "", fmt.Errorf("node %q has no endpoints to dial", d.node.Alias)
	}

	results := make(chan dialResult, len(attempts))
	done := make(chan struct{})

	for i, ep := range attempts {
		delay := time.Duration(i) * dialRaceStagger
		go func(ep config.Endpoint, delay time.Duration) {
			if delay > 0 {
				t := time.NewTimer(delay)
				defer t.Stop()
				select {
				case <-done:
					return
				case <-t.C:
				}
			}

			session, err := DialEndpoint(ep, d.node.AuthToken, d.cfg)
			if err != nil {
				d.recordFailure(ep.Target)
				select {
				case results <- dialResult{mimic: ep.Mimic, target: ep.Target, err: err}:
				case <-done:
				}
				return
			}

			d.recordSuccess(ep.Target)
			select {
			case results <- dialResult{session: session, mimic: ep.Mimic, target: ep.Target}:
			case <-done:
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
		slog.Info("pipe established", "node", d.node.Alias, "mimic", r.mimic, "target", r.target)
		go keepAlive(r.session)
		return r.session, r.mimic, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("node %q: all endpoint attempts failed", d.node.Alias)
	}
	return nil, "", lastErr
}

// keepAlive sends a randomized-interval Yamux ping. A failed ping actively closes
// the session so pool health sees it as dead immediately instead of retaining a
// zombie transport until a later user stream happens to fail.
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
