package pool

import (
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"
)

const (
	StateActive   int32 = 0
	StateDraining int32 = 1
)

type YamuxSession struct {
	session *yamux.Session
	state   int32

	bytesTransferred uint64
	cumulativeBytes  uint64
	pendingOpens     int32 // reservations made before blocking Yamux OpenStream

	mimicType     string
	bornAt        time.Time
	retireAfter   time.Duration
	byteBudget    uint64
	drainingSince time.Time

	baseLimitMbps  int
	jitterMbps     int
	currentCapMbps int

	lastActivityUnixNano int64
	mu                   sync.RWMutex
}

func NewYamuxSession(ys *yamux.Session, baseLimit, jitter int, mimicType string, policy LifecyclePolicy) *YamuxSession {
	retireAfter, byteBudget := policy.roll(mimicType)
	s := &YamuxSession{
		session: ys, state: StateActive, mimicType: mimicType, bornAt: time.Now(),
		retireAfter: retireAfter, byteBudget: byteBudget,
		baseLimitMbps: baseLimit, jitterMbps: jitter,
	}
	s.touchActivity()
	s.UpdateChaosLimit()
	return s
}

func (ys *YamuxSession) ShouldRetire() bool {
	if ys.retireAfter > 0 && time.Since(ys.bornAt) >= ys.retireAfter { return true }
	return ys.byteBudget > 0 && atomic.LoadUint64(&ys.cumulativeBytes) >= ys.byteBudget
}

type monitoredStream struct { net.Conn; parent *YamuxSession }
func (m *monitoredStream) Close() error { return m.Conn.Close() }
func (m *monitoredStream) CloseWrite() error {
	if cw, ok := m.Conn.(interface{ CloseWrite() error }); ok { return cw.CloseWrite() }
	return m.Conn.Close()
}
func (m *monitoredStream) Read(b []byte) (int, error) {
	n, err := m.Conn.Read(b)
	if n > 0 {
		atomic.AddUint64(&m.parent.bytesTransferred, uint64(n))
		atomic.AddUint64(&m.parent.cumulativeBytes, uint64(n))
		m.parent.touchActivity()
	}
	return n, err
}
func (m *monitoredStream) Write(b []byte) (int, error) {
	n, err := m.Conn.Write(b)
	if n > 0 {
		atomic.AddUint64(&m.parent.bytesTransferred, uint64(n))
		atomic.AddUint64(&m.parent.cumulativeBytes, uint64(n))
		m.parent.touchActivity()
	}
	return n, err
}

func (ys *YamuxSession) OpenStream() (net.Conn, error) {
	stream, err := ys.session.OpenStream()
	if err != nil { return nil, err }
	ys.touchActivity()
	return &monitoredStream{Conn: stream, parent: ys}, nil
}

func (ys *YamuxSession) ReserveOpen() { atomic.AddInt32(&ys.pendingOpens, 1) }
func (ys *YamuxSession) ReleaseOpen() {
	// Every reservation has exactly one release in getStreamLeastLoaded.
	atomic.AddInt32(&ys.pendingOpens, -1)
}
func (ys *YamuxSession) PendingOpens() int { return int(atomic.LoadInt32(&ys.pendingOpens)) }
func (ys *YamuxSession) LoadScore() int { return ys.ActiveStreams() + ys.PendingOpens() }

func (ys *YamuxSession) touchActivity() { atomic.StoreInt64(&ys.lastActivityUnixNano, time.Now().UnixNano()) }
func (ys *YamuxSession) GetAndResetBytes() uint64 { return atomic.SwapUint64(&ys.bytesTransferred, 0) }
func (ys *YamuxSession) RecentBytes() uint64 { return atomic.LoadUint64(&ys.bytesTransferred) }

func (ys *YamuxSession) UpdateChaosLimit() {
	ys.mu.Lock(); defer ys.mu.Unlock()
	if ys.jitterMbps <= 0 {
		ys.currentCapMbps = ys.baseLimitMbps
	} else {
		variance := rand.Intn((ys.jitterMbps*2)+1) - ys.jitterMbps
		ys.currentCapMbps = ys.baseLimitMbps + variance
	}
	if ys.currentCapMbps < 1 { ys.currentCapMbps = 1 }
}
func (ys *YamuxSession) CurrentCap() int { ys.mu.RLock(); defer ys.mu.RUnlock(); return ys.currentCapMbps }

func (ys *YamuxSession) SetDraining() {
	ys.mu.Lock(); if ys.drainingSince.IsZero() { ys.drainingSince = time.Now() }; ys.mu.Unlock()
	atomic.StoreInt32(&ys.state, StateDraining)
}
func (ys *YamuxSession) Revive() {
	ys.mu.Lock(); ys.drainingSince = time.Time{}; ys.mu.Unlock(); atomic.StoreInt32(&ys.state, StateActive)
}
func (ys *YamuxSession) DrainingFor() time.Duration {
	ys.mu.RLock(); defer ys.mu.RUnlock(); if ys.drainingSince.IsZero() { return 0 }; return time.Since(ys.drainingSince)
}
func (ys *YamuxSession) IsDraining() bool { return atomic.LoadInt32(&ys.state) == StateDraining }
func (ys *YamuxSession) IsActive() bool { return atomic.LoadInt32(&ys.state) == StateActive }
func (ys *YamuxSession) ActiveStreams() int {
	if ys.session == nil || ys.session.IsClosed() { return 0 }; return ys.session.NumStreams()
}
func (ys *YamuxSession) IsClosed() bool { return ys.session == nil || ys.session.IsClosed() }
func (ys *YamuxSession) Close() error { if ys.session == nil { return nil }; return ys.session.Close() }
func (ys *YamuxSession) Age() time.Duration { return time.Since(ys.bornAt) }
func (ys *YamuxSession) CumulativeBytes() uint64 { return atomic.LoadUint64(&ys.cumulativeBytes) }
func (ys *YamuxSession) IdleTime() time.Duration {
	ns := atomic.LoadInt64(&ys.lastActivityUnixNano); if ns == 0 { return 0 }; return time.Since(time.Unix(0, ns))
}
