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
	maxTotalFactor  = 3
)

func shouldCloseDraining(streams int, _ time.Duration, _ bool) bool {
	return streams == 0
}

type DialFunc func() (*yamux.Session, string, error)

type PoolStats struct {
	ActiveConns   int
	DrainingConns int
	TotalMbps     int
}

type NodePool struct {
	Alias           string
	label           string
	TargetIP        string
	minConnections  int
	maxConnections  int
	baseLimitMbps   int
	jitterMbps      int
	dialer          DialFunc
	lifecycle       LifecyclePolicy
	sessions        []*YamuxSession
	mu              sync.RWMutex
	currentMbps     int32
	replenishing    int32
	replenishWanted int32
	shutdown        chan struct{}
	stopOnce        sync.Once
}

const (
	udpMinConns = 2
	udpMaxConns = 6
)

type nodePools struct {
	tcp *NodePool
	udp *NodePool
}

type HubManager struct {
	pools map[string]*nodePools
	mu    sync.RWMutex
}

func NewHubManager() *HubManager {
	return &HubManager{pools: make(map[string]*nodePools)}
}

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

func (hm *HubManager) GetStreamTCP(nodeAlias string) (net.Conn, error) {
	np, err := hm.lookup(nodeAlias)
	if err != nil {
		return nil, err
	}
	return np.tcp.getStreamLeastLoaded()
}

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
	s           *YamuxSession
	streams     int
	recentBytes uint64
}

// getStreamLeastLoaded never holds the pool lock while Yamux OpenStream blocks.
// If one or more candidates die during selection, the surviving user request is
// still served by the next candidate and all observed losses are replenished in a
// single coalesced background request.
func (np *NodePool) getStreamLeastLoaded() (net.Conn, error) {
	np.mu.RLock()
	candidates := make([]streamCandidate, 0, len(np.sessions))
	for _, s := range np.sessions {
		if s.IsClosed() || !s.IsActive() {
			continue
		}
		candidates = append(candidates, streamCandidate{
			s:           s,
			streams:     s.ActiveStreams(),
			recentBytes: s.RecentBytes(),
		})
	}
	np.mu.RUnlock()

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].streams != candidates[j].streams {
			return candidates[i].streams < candidates[j].streams
		}
		return candidates[i].recentBytes < candidates[j].recentBytes
	})

	var lastErr error
	failed := 0
	for _, c := range candidates {
		stream, err := c.s.OpenStream()
		if err == nil {
			if failed > 0 {
				np.replenishAsync(failed)
			}
			return stream, nil
		}
		lastErr = err
		failed++
		_ = c.s.Close()
		slog.Debug("yamux stream open failed; closed pipe and trying another", "node", np.Alias, "pool", np.label, "err", err)
	}

	np.mu.RLock()
	currentActive := np.activeCountLocked()
	np.mu.RUnlock()
	needed := np.minConnections - currentActive
	if needed < failed {
		needed = failed
	}
	if needed < 1 {
		needed = 1
	}
	np.replenishAsync(needed)
	if lastErr != nil {
		return nil, fmt.Errorf("no usable active connection in pool: %w", lastErr)
	}
	return nil, errors.New("no active connections available in the pool")
}

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

// Retirement is make-before-break with respect to minConnections. Open logical
// streams on draining pipes are never force-cut merely because they are quiet.
func (np *NodePool) evaluateHealthAndScale() {
	np.mu.Lock()
	retainedSessions := make([]*YamuxSession, 0, len(np.sessions))
	dynamicIdleLimit := time.Duration(rand.Intn(61)+60) * time.Second
	needsScaleUp := false
	activeRemaining := np.activeCountLocked()
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
		s.UpdateChaosLimit()
		cap := s.CurrentCap()

		if s.IsActive() {
			retireRequested := s.ShouldRetire()
			if retireRequested && drainingCount < np.maxConnections && activeRemaining > np.minConnections {
				s.SetDraining()
				activeRemaining--
				drainingCount++
				slog.Info("retired: lifecycle budget reached", "node", np.Alias, "pool", np.label,
					"mimic", s.mimicType, "age", s.Age().Round(time.Second), "mb", s.CumulativeBytes()/(1024*1024))
			} else {
				if retireRequested && activeRemaining <= np.minConnections && activeRemaining < np.maxConnections {
					needsScaleUp = true
				}
				if mbps >= int(float64(cap)*0.8) {
					needsScaleUp = true
				}
				if !retireRequested && activeRemaining > np.minConnections && drainingCount < np.maxConnections &&
					s.ActiveStreams() == 0 && s.IdleTime() > dynamicIdleLimit {
					s.SetDraining()
					activeRemaining--
					drainingCount++
					slog.Info("scaled down: unused connection draining", "node", np.Alias, "pool", np.label)
				}
			}
		} else if s.IsDraining() {
			streams := s.ActiveStreams()
			drainedFor := s.DrainingFor()
			if shouldCloseDraining(streams, drainedFor, false) {
				_ = s.Close()
				drainingCount--
				slog.Info("draining complete: connection closed", "node", np.Alias, "pool", np.label,
					"streams", streams, "drained_for", drainedFor.Round(time.Second))
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

func (np *NodePool) activeCountLocked() int {
	n := 0
	for _, s := range np.sessions {
		if s.IsActive() && !s.IsClosed() {
			n++
		}
	}
	return n
}

func (np *NodePool) drainingCountLocked() int {
	n := 0
	for _, s := range np.sessions {
		if s.IsDraining() && !s.IsClosed() {
			n++
		}
	}
	return n
}

func (np *NodePool) evictOldestSafeDrainingLocked() bool {
	idx := -1
	var oldest time.Duration
	for i, s := range np.sessions {
		if !s.IsDraining() || s.ActiveStreams() > 0 {
			continue
		}
		if d := s.DrainingFor(); idx == -1 || d > oldest {
			idx, oldest = i, d
		}
	}
	if idx == -1 {
		return false
	}
	_ = np.sessions[idx].Close()
	slog.Info("evicted empty draining connection to free a slot", "node", np.Alias, "pool", np.label,
		"drained_for", oldest.Round(time.Second))
	np.sessions = append(np.sessions[:idx], np.sessions[idx+1:]...)
	return true
}

func (np *NodePool) executeScaleUp() {
	np.mu.RLock()
	activeConns := np.activeCountLocked()
	np.mu.RUnlock()
	if activeConns < np.maxConnections {
		np.replenishAsync(1)
	}
}

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

// replenishWorker is self-healing: after every coalesced batch it re-checks the
// real live active count before going idle. Concurrent losses therefore cannot be
// hidden by max-coalescing. Shutdown is checked explicitly so a stopped, deficient
// pool cannot spin forever trying to restore a floor it is no longer allowed to dial.
func (np *NodePool) replenishWorker() {
	defer atomic.StoreInt32(&np.replenishing, 0)
	for {
		select {
		case <-np.shutdown:
			return
		default:
		}

		wanted := int(atomic.SwapInt32(&np.replenishWanted, 0))
		if wanted > 0 {
			np.replenishPool(wanted)
			continue
		}

		np.mu.RLock()
		active := np.activeCountLocked()
		np.mu.RUnlock()
		if active < np.minConnections {
			atomicMaxInt32(&np.replenishWanted, int32(np.minConnections-active))
			continue
		}

		// Publish idle, then close the race with a request that arrived while this
		// worker still owned the guard. If a request exists, reclaim the guard and
		// continue; otherwise another caller is free to start the next worker.
		atomic.StoreInt32(&np.replenishing, 0)
		if atomic.LoadInt32(&np.replenishWanted) == 0 {
			return
		}
		if !atomic.CompareAndSwapInt32(&np.replenishing, 0, 1) {
			return
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
		np.mu.RLock()
		activeBeforeDial := np.activeCountLocked()
		np.mu.RUnlock()
		if i > 0 || activeBeforeDial > 0 {
			select {
			case <-np.shutdown:
				return
			case <-time.After(staggerDelay):
			}
		}

		rawYamuxSession, mimicType, err := np.dialer()
		if err != nil || rawYamuxSession == nil {
			slog.Warn("failed to dial new connection", "node", np.Alias, "pool", np.label, "err", err)
			continue
		}
		select {
		case <-np.shutdown:
			_ = rawYamuxSession.Close()
			return
		default:
		}

		wrappedSession := NewYamuxSession(rawYamuxSession, np.baseLimitMbps, np.jitterMbps, mimicType, np.lifecycle)
		np.mu.Lock()
		np.compactClosedLocked()
		switch {
		case np.activeCountLocked() >= np.maxConnections:
			_ = wrappedSession.Close()
		case len(np.sessions) >= maxTotalFactor*np.maxConnections:
			if np.evictOldestSafeDrainingLocked() {
				np.sessions = append(np.sessions, wrappedSession)
			} else {
				_ = wrappedSession.Close()
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
		_ = session.Close()
	}
	np.sessions = nil
}
