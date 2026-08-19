package pool

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/hedioum/Hedioum-Pool-Tunnel/config"
)

const (
	defaultMaxConns = 15
	defaultMinConns = 10
	staggerDelay    = 500 * time.Millisecond
	healthCheckFreq = 10 * time.Second

	// A draining pipe with no streams closes immediately. A pipe that still owns a
	// logical stream is closed only after it has had no real payload activity for
	// the grace period. There is deliberately no wall-clock hard kill for an active
	// transfer: long downloads, calls and WebSockets must be allowed to finish.
	drainSoftGrace = 90 * time.Second

	// The lifecycle code keeps normal operation below 2x max (active + draining).
	// 3x is only a safety ceiling for legacy/edge states and lets us recover active
	// capacity without force-closing a genuinely live drainer.
	maxTotalFactor = 3
)

// shouldCloseDraining decides whether a draining pipe may be closed now. Live
// traffic is never killed just because a wall-clock deadline elapsed.
func shouldCloseDraining(streams int, drainedFor time.Duration, idle bool) bool {
	if streams == 0 {
		return true
	}
	return idle && drainedFor > drainSoftGrace
}

// DialFunc creates a new authenticated physical connection and reports which mimic
// type it used, so the pool can apply the protocol-aware retirement policy.
type DialFunc func() (*yamux.Session, string, error)

// PoolStats holds real-time telemetry data for the interactive dashboard.
type PoolStats struct {
	ActiveConns   int
	DrainingConns int
	TotalMbps     int
}

// NodePool manages an auto-scaling pool of Yamux sessions to a single foreign server.
type NodePool struct {
	Alias           string
	label           string // "tcp" or "udp" — which sub-pool, for observability in logs
	TargetIP        string
	minConnections  int
	maxConnections  int
	baseLimitMbps   int
	jitterMbps      int
	dialer          DialFunc
	lifecycle       LifecyclePolicy
	sessions        []*YamuxSession
	mu              sync.RWMutex
	currentMbps     int32 // Atomic total bandwidth of this pool for dashboard monitoring
	replenishing    int32 // CAS guard: at most one dial/replenish worker per sub-pool
	replenishWanted int32 // max outstanding number of fresh pipes requested
	shutdown        chan struct{}
	stopOnce        sync.Once
}

// UDP sub-pool sizing. UDP rides a SEPARATE set of physical connections so a bulk
// TCP download cannot head-of-line-block a real-time UDP call.
const (
	udpMinConns = 2
	udpMaxConns = 6
)

// nodePools holds the two isolated sub-pools for one foreign node.
type nodePools struct {
	tcp *NodePool
	udp *NodePool
}

// HubManager oversees all active foreign node pools in the Iran Hub.
type HubManager struct {
	pools map[string]*nodePools
	mu    sync.RWMutex
}

// NewHubManager initializes the global pool manager.
func NewHubManager() *HubManager {
	return &HubManager{pools: make(map[string]*nodePools)}
}

// newNodePool builds and starts a monitored connection pool.
func newNodePool(cfg config.ForeignNode, label string, minConns, maxConns int, dialer DialFunc, lifecycle LifecyclePolicy) *NodePool {
	pool := &NodePool{
		Alias:          cfg.Alias,
		label:          label,
		TargetIP:       cfg.TargetIP,
		minConnections: minConns,
		maxConnections: maxConns,
		baseLimitMbps:  cfg.BandwidthLimitMbps,
		jitterMbps:     cfg.BandwidthJitterMbps,
		dialer:         dialer,
		lifecycle:      lifecycle,
		sessions:       make([]*YamuxSession, 0, maxConns),
		shutdown:       make(chan struct{}),
	}
	go pool.monitorAndScale()
	return pool
}

// RegisterNode provisions isolated TCP and UDP sub-pools. Re-registering an alias
// cleanly stops the old pools instead of leaking their watchdog goroutines/sockets.
func (hm *HubManager) RegisterNode(cfg config.ForeignNode, dialer DialFunc) {
	minConns := cfg.MinConnections
	if minConns < 1 {
		minConns = defaultMinConns
	}
	maxConns := cfg.MaxConnections
	if maxConns < minConns {
		maxConns = minConns + 5
	}

	lifecycle := NewLifecyclePolicy(cfg.AuthToken)
	fresh := &nodePools{
		tcp: newNodePool(cfg, "tcp", minConns, maxConns, dialer, lifecycle),
		udp: newNodePool(cfg, "udp", udpMinConns, udpMaxConns, dialer, lifecycle),
	}

	hm.mu.Lock()
	old := hm.pools[cfg.Alias]
	hm.pools[cfg.Alias] = fresh
	hm.mu.Unlock()

	if old != nil {
		old.tcp.stop()
		old.udp.stop()
	}
}

// GetStreamTCP returns a least-loaded stream on the node's TCP sub-pool.
func (hm *HubManager) GetStreamTCP(nodeAlias string) (net.Conn, error) {
	np, err := hm.lookup(nodeAlias)
	if err != nil {
		return nil, err
	}
	return np.tcp.getStreamLeastLoaded()
}

// GetStreamUDP returns a least-loaded stream on the node's dedicated UDP sub-pool.
func (hm *HubManager) GetStreamUDP(nodeAlias string) (net.Conn, error) {
	np, err := hm.lookup(nodeAlias)
	if err != nil {
		return nil, err
	}
	return np.udp.getStreamLeastLoaded()
}

func (hm *HubManager) lookup(nodeAlias string) (*nodePools, error) {
	hm.mu.RLock()
	np, exists := hm.pools[nodeAlias]
	hm.mu.RUnlock()
	if !exists {
		return nil, errors.New("foreign node pool not found")
	}
	return np, nil
}

type streamCandidate struct {
	s       *YamuxSession
	streams int
}

// getStreamLeastLoaded snapshots candidates under the pool lock, RELEASES the
// lock, then calls Yamux OpenStream. OpenStream can block while waiting for a peer
// ACK, so holding the pool lock across it can freeze the watchdog/replenisher.
// If the best session races with a disconnect, the next candidate is tried before
// failing the user connection.
func (np *NodePool) getStreamLeastLoaded() (net.Conn, error) {
	np.mu.RLock()
	candidates := make([]streamCandidate, 0, len(np.sessions))
	for _, s := range np.sessions {
		if s.IsClosed() || !s.IsActive() {
			continue
		}
		candidates = append(candidates, streamCandidate{s: s, streams: s.ActiveStreams()})
	}
	np.mu.RUnlock()

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].streams < candidates[j].streams
	})

	var lastErr error
	for _, c := range candidates {
		stream, err := c.s.OpenStream()
		if err == nil {
			return stream, nil
		}
		lastErr = err
		// A stream-open failure means this physical session is no longer a safe
		// candidate. Close it immediately so subsequent requests do not stall on
		// the same zombie until the next watchdog tick.
		_ = c.s.Close()
		slog.Debug("yamux stream open failed; closed pipe and trying another", "node", np.Alias, "pool", np.label, "err", err)
	}

	// Do not make a client request responsible for rebuilding the whole pool. Kick
	// recovery in the background; Xray/SOCKS callers can retry immediately.
	np.replenishAsync(1)
	if lastErr != nil {
		return nil, fmt.Errorf("no usable active connection in pool: %w", lastErr)
	}
	return nil, errors.New("no active connections available in the pool")
}

// monitorAndScale is the core watchdog. Replenishment is asynchronous so failed or
// filtered dials cannot stall health evaluation for tens of seconds.
func (np *NodePool) monitorAndScale() {
	ticker := time.NewTicker(healthCheckFreq)
	defer ticker.Stop()

	np.replenishAsync(np.minConnections)

	for {
		select {
		case <-np.shutdown:
			np.cleanup()
			return
		case <-ticker.C:
			np.evaluateHealthAndScale()
		}
	}
}

// evaluateHealthAndScale calculates throughput, lifecycle state, and scale dynamics.
func (np *NodePool) evaluateHealthAndScale() {
	np.mu.Lock()
	retainedSessions := make([]*YamuxSession, 0, len(np.sessions))

	dynamicIdleLimit := time.Duration(rand.Intn(61)+60) * time.Second
	needsScaleUp := false
	activeCount := 0
	drainingCount := np.drainingCountLocked()
	totalPoolMbps := 0

	for _, s := range np.sessions {
		if s.IsClosed() {
			slog.Info("purged dead physical connection", "node", np.Alias, "pool", np.label)
			continue
		}

		bytesLastInterval := s.GetAndResetBytes()
		intervalSeconds := uint64(healthCheckFreq.Seconds())
		mbps := int((bytesLastInterval * 8) / (1024 * 1024 * intervalSeconds))
		totalPoolMbps += mbps

		// This is a scale-out target only; payload is not hard-throttled.
		s.UpdateChaosLimit()
		cap := s.CurrentCap()

		if s.IsActive() {
			activeCount++

			// Never let lifecycle churn create an unbounded wall of drainers. If the
			// drainer budget is full, defer retirement until existing live streams
			// naturally finish instead of killing one to make room.
			if s.ShouldRetire() && drainingCount < np.maxConnections {
				s.SetDraining()
				activeCount--
				drainingCount++
				slog.Info("retired: lifecycle budget reached", "node", np.Alias, "pool", np.label,
					"mimic", s.mimicType, "age", s.Age().Round(time.Second), "mb", s.CumulativeBytes()/(1024*1024))
			} else {
				if mbps >= int(float64(cap)*0.8) {
					needsScaleUp = true
				}

				// Scale down only truly unused pipes. A low-bandwidth live stream (voice,
				// WebSocket, keepalive traffic) is not an idle connection.
				if activeCount > np.minConnections && drainingCount < np.maxConnections &&
					s.ActiveStreams() == 0 && s.IdleTime() > dynamicIdleLimit {
					s.SetDraining()
					activeCount--
					drainingCount++
					slog.Info("scaled down: unused connection draining", "node", np.Alias, "pool", np.label)
				}
			}
		} else if s.IsDraining() {
			streams := s.ActiveStreams()
			drainedFor := s.DrainingFor()
			idle := s.IdleTime() > drainSoftGrace
			if shouldCloseDraining(streams, drainedFor, idle) {
				s.Close()
				drainingCount--
				slog.Info("draining complete: connection closed", "node", np.Alias, "pool", np.label,
					"streams", streams, "drained_for", drainedFor.Round(time.Second), "idle", idle)
				continue
			}
		}

		retainedSessions = append(retainedSessions, s)
	}

	np.sessions = retainedSessions
	atomic.StoreInt32(&np.currentMbps, int32(totalPoolMbps))
	np.mu.Unlock()

	if needsScaleUp {
		np.executeScaleUp()
	}

	np.mu.RLock()
	currentActive := np.activeCountLocked()
	np.mu.RUnlock()

	if currentActive == 0 {
		slog.Warn("watchdog: pool has no active connections — re-warming", "node", np.Alias, "pool", np.label)
	}
	if currentActive < np.minConnections {
		np.replenishAsync(np.minConnections - currentActive)
	}
}

// activeCountLocked counts live Active sessions. Caller holds np.mu.
func (np *NodePool) activeCountLocked() int {
	n := 0
	for _, s := range np.sessions {
		if s.IsActive() && !s.IsClosed() {
			n++
		}
	}
	return n
}

// drainingCountLocked counts live draining sessions. Caller holds np.mu.
func (np *NodePool) drainingCountLocked() int {
	n := 0
	for _, s := range np.sessions {
		if s.IsDraining() && !s.IsClosed() {
			n++
		}
	}
	return n
}

// evictOldestSafeDrainingLocked frees a slot only from a drainer that is already
// empty or genuinely idle. It never sacrifices a live transfer to replenish the
// pool. Caller holds np.mu.
func (np *NodePool) evictOldestSafeDrainingLocked() bool {
	idx := -1
	var oldest time.Duration
	for i, s := range np.sessions {
		if !s.IsDraining() {
			continue
		}
		if s.ActiveStreams() > 0 && s.IdleTime() <= drainSoftGrace {
			continue
		}
		if d := s.DrainingFor(); idx == -1 || d > oldest {
			idx, oldest = i, d
		}
	}
	if idx == -1 {
		return false
	}
	np.sessions[idx].Close()
	slog.Info("evicted safe draining connection", "node", np.Alias, "pool", np.label,
		"drained_for", oldest.Round(time.Second))
	np.sessions = append(np.sessions[:idx], np.sessions[idx+1:]...)
	return true
}

// executeScaleUp requests a fresh physical connection under load.
func (np *NodePool) executeScaleUp() {
	np.mu.RLock()
	activeConns := np.activeCountLocked()
	np.mu.RUnlock()

	if activeConns < np.maxConnections {
		np.replenishAsync(1)
	}
}

// replenishAsync keeps the health loop non-blocking and coalesces duplicate
// recovery requests without LOSING a larger request that arrives while a dial batch
// is already running. replenishWanted stores the maximum outstanding batch size.
func (np *NodePool) replenishAsync(needed int) {
	if needed <= 0 {
		return
	}
	atomicMaxInt32(&np.replenishWanted, int32(needed))
	if !atomic.CompareAndSwapInt32(&np.replenishing, 0, 1) {
		return
	}
	go np.replenishWorker()
}

func atomicMaxInt32(dst *int32, v int32) {
	for {
		old := atomic.LoadInt32(dst)
		if old >= v {
			return
		}
		if atomic.CompareAndSwapInt32(dst, old, v) {
			return
		}
	}
}

func (np *NodePool) replenishWorker() {
	for {
		wanted := int(atomic.SwapInt32(&np.replenishWanted, 0))
		if wanted > 0 {
			np.replenishPool(wanted)
			continue
		}

		// Publish idle only after the pending counter is empty, then close the race
		// with a request that may have arrived while replenishing was still 1.
		atomic.StoreInt32(&np.replenishing, 0)
		if atomic.LoadInt32(&np.replenishWanted) == 0 {
			return
		}
		if !atomic.CompareAndSwapInt32(&np.replenishing, 0, 1) {
			return // another caller started the replacement worker
		}
	}
}

func (np *NodePool) compactClosedLocked() {
	kept := np.sessions[:0]
	for _, s := range np.sessions {
		if !s.IsClosed() {
			kept = append(kept, s)
		}
	}
	np.sessions = kept
}

func (np *NodePool) replenishPool(needed int) {
	for i := 0; i < needed; i++ {
		select {
		case <-np.shutdown:
			return
		case <-time.After(staggerDelay):
		}

		rawYamuxSession, mimicType, err := np.dialer()
		if err != nil || rawYamuxSession == nil {
			slog.Warn("failed to dial new connection", "node", np.Alias, "pool", np.label, "err", err)
			continue
		}

		select {
		case <-np.shutdown:
			rawYamuxSession.Close()
			return
		default:
		}

		wrappedSession := NewYamuxSession(rawYamuxSession, np.baseLimitMbps, np.jitterMbps, mimicType, np.lifecycle)

		np.mu.Lock()
		np.compactClosedLocked()
		switch {
		case np.activeCountLocked() >= np.maxConnections:
			wrappedSession.Close()
		case len(np.sessions) >= maxTotalFactor*np.maxConnections:
			if np.evictOldestSafeDrainingLocked() {
				np.sessions = append(np.sessions, wrappedSession)
			} else {
				// Safety ceiling reached with only genuinely live drainers. Do not cut
				// user traffic; reject this extra pipe and retry after streams finish.
				wrappedSession.Close()
				slog.Warn("session safety ceiling reached; preserving live drainers",
					"node", np.Alias, "pool", np.label, "total", len(np.sessions))
			}
		default:
			np.sessions = append(np.sessions, wrappedSession)
			slog.Info("scaled up: dialed new connection", "node", np.Alias, "pool", np.label,
				"active", np.activeCountLocked(), "total", len(np.sessions), "max", np.maxConnections)
		}
		np.mu.Unlock()
	}
}

// GetStats returns aggregated telemetry (TCP + UDP sub-pools) for the dashboard.
func (hm *HubManager) GetStats(nodeAlias string) PoolStats {
	np, err := hm.lookup(nodeAlias)
	if err != nil {
		return PoolStats{}
	}
	tcp := np.tcp.stats()
	udp := np.udp.stats()
	return PoolStats{
		ActiveConns:   tcp.ActiveConns + udp.ActiveConns,
		DrainingConns: tcp.DrainingConns + udp.DrainingConns,
		TotalMbps:     tcp.TotalMbps + udp.TotalMbps,
	}
}

// stats snapshots one pool's connection counts and bandwidth.
func (np *NodePool) stats() PoolStats {
	np.mu.RLock()
	defer np.mu.RUnlock()

	var active, draining int
	for _, s := range np.sessions {
		if s.IsClosed() {
			continue
		}
		if s.IsActive() {
			active++
		} else if s.IsDraining() {
			draining++
		}
	}
	return PoolStats{
		ActiveConns:   active,
		DrainingConns: draining,
		TotalMbps:     int(atomic.LoadInt32(&np.currentMbps)),
	}
}

func (np *NodePool) stop() {
	np.stopOnce.Do(func() { close(np.shutdown) })
}

// Close cleanly stops every pool. It is idempotent and safe during shutdown.
func (hm *HubManager) Close() {
	hm.mu.Lock()
	all := make([]*nodePools, 0, len(hm.pools))
	for _, np := range hm.pools {
		all = append(all, np)
	}
	hm.pools = make(map[string]*nodePools)
	hm.mu.Unlock()

	for _, np := range all {
		np.tcp.stop()
		np.udp.stop()
	}
}

func (np *NodePool) cleanup() {
	np.mu.Lock()
	defer np.mu.Unlock()
	for _, session := range np.sessions {
		session.Close()
	}
	np.sessions = nil
}
