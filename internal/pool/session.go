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
	StateDraining int32 = 1 // Connection is waiting for active logical streams to finish
)

// YamuxSession wraps the HashiCorp Yamux multiplexer with pool/lifecycle metadata.
// BandwidthLimitMbps is intentionally NOT enforced as a hard token-bucket here:
// the value is used by the pool as a scale-out target only. Hard per-pipe shaping
// caused single-flow throughput to collapse below the real path capacity.
type YamuxSession struct {
	session *yamux.Session
	state   int32 // Atomic state: Active vs Draining

	// Atomic counters for real-time bandwidth calculation (Bytes). bytesTransferred
	// is reset every health interval for the Mbps gauge; cumulativeBytes is never
	// reset and drives the lifecycle transfer budget.
	bytesTransferred uint64
	cumulativeBytes  uint64

	// Protocol-aware lifecycle (see lifecycle.go). A non-SSH pipe retires after
	// retireAfter OR byteBudget, whichever comes first; SSH retires on the long
	// retireAfter only (byteBudget == 0 means "no budget").
	mimicType     string
	bornAt        time.Time
	retireAfter   time.Duration
	byteBudget    uint64
	drainingSince time.Time // when SetDraining was called (guarded by mu)

	// Pool scale-out target / DPI traffic-shape metadata. This no longer throttles
	// user payload in the hot Read/Write path.
	baseLimitMbps  int
	jitterMbps     int
	currentCapMbps int

	// lastActivityUnixNano is updated on every successful payload Read/Write, not
	// merely when a logical stream is opened. That distinction is critical for
	// long-lived low-bandwidth streams (voice, WebSocket, messaging): they must not
	// be classified idle while data is still flowing.
	lastActivityUnixNano int64
	mu                   sync.RWMutex
}

// NewYamuxSession initializes a monitored physical connection with a randomized
// scale-out target and a protocol-aware retirement budget rolled from the node's
// lifecycle policy.
func NewYamuxSession(ys *yamux.Session, baseLimit, jitter int, mimicType string, policy LifecyclePolicy) *YamuxSession {
	retireAfter, byteBudget := policy.roll(mimicType)
	s := &YamuxSession{
		session:       ys,
		state:         StateActive,
		mimicType:     mimicType,
		bornAt:        time.Now(),
		retireAfter:   retireAfter,
		byteBudget:    byteBudget,
		baseLimitMbps: baseLimit,
		jitterMbps:    jitter,
	}
	s.touchActivity()
	s.UpdateChaosLimit()
	return s
}

// ShouldRetire reports whether this pipe has reached its randomized lifetime or
// transfer budget and should be drained so the pool churns to a fresh pipe. SSH
// (byteBudget == 0) retires on its long lifetime only.
func (ys *YamuxSession) ShouldRetire() bool {
	if ys.retireAfter > 0 && time.Since(ys.bornAt) >= ys.retireAfter {
		return true
	}
	if ys.byteBudget > 0 && atomic.LoadUint64(&ys.cumulativeBytes) >= ys.byteBudget {
		return true
	}
	return false
}

// monitoredStream is a Decorator for net.Conn that counts traffic and tracks real
// data activity. It deliberately does not rate-limit payload: the network path,
// congestion control, and application are allowed to use the available capacity.
type monitoredStream struct {
	net.Conn
	parent *YamuxSession
}

func (m *monitoredStream) Close() error { return m.Conn.Close() }

// CloseWrite provides half-close semantics to the bidirectional proxy. Hashicorp
// yamux.Stream has no CloseWrite method; its Close sends a FIN for the local write
// side while reads remain valid until the peer also closes, so falling back to
// Close is the correct half-close operation for the wrapped yamux stream.
func (m *monitoredStream) CloseWrite() error {
	if cw, ok := m.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
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

// OpenStream opens a new logical stream and wraps it for accounting/activity.
func (ys *YamuxSession) OpenStream() (net.Conn, error) {
	stream, err := ys.session.OpenStream()
	if err != nil {
		return nil, err
	}
	ys.touchActivity()
	return &monitoredStream{Conn: stream, parent: ys}, nil
}

func (ys *YamuxSession) touchActivity() {
	atomic.StoreInt64(&ys.lastActivityUnixNano, time.Now().UnixNano())
}

// GetAndResetBytes atomically fetches the total transferred bytes since the last check, and resets the counter to 0.
func (ys *YamuxSession) GetAndResetBytes() uint64 {
	return atomic.SwapUint64(&ys.bytesTransferred, 0)
}

// UpdateChaosLimit shifts the per-pipe scale-out target randomly. Unlike upstream,
// it does not install a hard rate limiter; this preserves the distribution signal
// used by the pool without artificially capping a user's flow. Negative jitter is
// treated as disabled so a malformed legacy/manual config can never panic rand.Intn.
func (ys *YamuxSession) UpdateChaosLimit() {
	ys.mu.Lock()
	defer ys.mu.Unlock()

	if ys.jitterMbps <= 0 {
		ys.currentCapMbps = ys.baseLimitMbps
	} else {
		variance := rand.Intn((ys.jitterMbps*2)+1) - ys.jitterMbps
		ys.currentCapMbps = ys.baseLimitMbps + variance
	}

	if ys.currentCapMbps < 1 {
		ys.currentCapMbps = 1
	}
}

// CurrentCap returns the active fluctuating scale-out target for this connection.
func (ys *YamuxSession) CurrentCap() int {
	ys.mu.RLock()
	defer ys.mu.RUnlock()
	return ys.currentCapMbps
}

// --- State Management (Active / Draining) ---

func (ys *YamuxSession) SetDraining() {
	ys.mu.Lock()
	if ys.drainingSince.IsZero() {
		ys.drainingSince = time.Now()
	}
	ys.mu.Unlock()
	atomic.StoreInt32(&ys.state, StateDraining)
}

func (ys *YamuxSession) Revive() {
	ys.mu.Lock()
	ys.drainingSince = time.Time{}
	ys.mu.Unlock()
	atomic.StoreInt32(&ys.state, StateActive)
}

// DrainingFor reports how long this session has been in the Draining state (0 if it
// is not draining), used to bound the drain grace period.
func (ys *YamuxSession) DrainingFor() time.Duration {
	ys.mu.RLock()
	defer ys.mu.RUnlock()
	if ys.drainingSince.IsZero() {
		return 0
	}
	return time.Since(ys.drainingSince)
}

func (ys *YamuxSession) IsDraining() bool {
	return atomic.LoadInt32(&ys.state) == StateDraining
}

func (ys *YamuxSession) IsActive() bool {
	return atomic.LoadInt32(&ys.state) == StateActive
}

// --- Core Wrapper Functions ---

func (ys *YamuxSession) ActiveStreams() int {
	if ys.session == nil || ys.session.IsClosed() {
		return 0
	}
	return ys.session.NumStreams()
}

func (ys *YamuxSession) IsClosed() bool {
	return ys.session == nil || ys.session.IsClosed()
}

func (ys *YamuxSession) Close() error {
	if ys.session == nil {
		return nil
	}
	return ys.session.Close()
}

// Age reports how long this physical connection has been alive.
func (ys *YamuxSession) Age() time.Duration {
	return time.Since(ys.bornAt)
}

// CumulativeBytes reports total bytes moved over this connection's whole lifetime
// (never reset), used for the transfer-budget retirement.
func (ys *YamuxSession) CumulativeBytes() uint64 {
	return atomic.LoadUint64(&ys.cumulativeBytes)
}

// IdleTime is based on the last successful payload I/O, not the stream-open time.
func (ys *YamuxSession) IdleTime() time.Duration {
	ns := atomic.LoadInt64(&ys.lastActivityUnixNano)
	if ns == 0 {
		return 0
	}
	return time.Since(time.Unix(0, ns))
}
