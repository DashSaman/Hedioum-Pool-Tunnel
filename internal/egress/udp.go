package egress

import (
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/tunproto"
)

const (
	udpIdleTimeout       = 60 * time.Second // close a UDP flow after this much silence
	udpMaxFlows          = 256              // per-stream NAT table cap (FD/memory guard)
	udpReadBufSize       = 64 * 1024
	udpSocketBufferBytes = 4 << 20 // best-effort kernel burst buffer per active flow
)

// dialUDP is the seam used to open a vetted UDP socket to a target; overridable
// in tests to reach a loopback echo server without tripping the SSRF gate.
var dialUDP = safeDialUDP

// udpFlow is one connected UDP socket to a single target. Timer operations,
// activity and close state are synchronized because request, response and timeout
// goroutines all touch the same flow.
type udpFlow struct {
	conn   *net.UDPConn
	target tunproto.Addr

	timerMu      sync.Mutex
	timer        *time.Timer
	lastActivity time.Time
	closed       bool
}

// arm installs an idle timer that re-checks the actual last-activity timestamp
// when it fires. This prevents a stale timer callback from closing a flow that was
// refreshed at almost exactly the timeout boundary.
func (f *udpFlow) arm(onIdle func()) {
	f.timerMu.Lock()
	defer f.timerMu.Unlock()
	if f.closed {
		return
	}
	f.lastActivity = time.Now()

	var expire func()
	expire = func() {
		f.timerMu.Lock()
		if f.closed {
			f.timerMu.Unlock()
			return
		}
		idleFor := time.Since(f.lastActivity)
		if idleFor < udpIdleTimeout {
			remaining := udpIdleTimeout - idleFor
			f.timer.Reset(remaining)
			f.timerMu.Unlock()
			return
		}

		// Win the timeout race while holding timerMu: after closed=true, a
		// concurrent touch cannot revive this expired socket. Close the fd here,
		// then let onIdle remove this exact identity from the flow map.
		f.closed = true
		conn := f.conn
		f.timerMu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		onIdle()
	}

	f.timer = time.AfterFunc(udpIdleTimeout, expire)
}

func (f *udpFlow) touch() {
	f.timerMu.Lock()
	defer f.timerMu.Unlock()
	if f.closed {
		return
	}
	f.lastActivity = time.Now()
	if f.timer != nil {
		f.timer.Reset(udpIdleTimeout)
	}
}

func (f *udpFlow) close() {
	f.timerMu.Lock()
	if f.closed {
		f.timerMu.Unlock()
		return
	}
	f.closed = true
	if f.timer != nil {
		f.timer.Stop()
	}
	conn := f.conn
	f.timerMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// handleUDPStream relays a UDP association's datagrams to the internet. The flow
// table is identity-safe: an old timer/reader may close only the exact udpFlow it
// belongs to, never a newer flow that reused the same host:port key.
func handleUDPStream(stream net.Conn) {
	var mu sync.Mutex
	flows := make(map[string]*udpFlow)
	var streamWriteMu sync.Mutex

	closeFlow := func(key string, expected *udpFlow) {
		var victim *udpFlow
		mu.Lock()
		if current, ok := flows[key]; ok && current == expected {
			delete(flows, key)
			victim = current
		}
		mu.Unlock()
		if victim != nil {
			victim.close()
		}
	}

	defer func() {
		mu.Lock()
		victims := make([]*udpFlow, 0, len(flows))
		for key, f := range flows {
			delete(flows, key)
			victims = append(victims, f)
		}
		mu.Unlock()
		for _, f := range victims {
			f.close()
		}
	}()

	for {
		addr, payload, err := tunproto.ReadDatagram(stream)
		if err != nil {
			return
		}
		key := addr.HostPort()

		mu.Lock()
		f, ok := flows[key]
		if !ok {
			if len(flows) >= udpMaxFlows {
				mu.Unlock()
				continue
			}
			uconn, derr := dialUDP(key)
			if derr != nil {
				mu.Unlock()
				if errors.Is(derr, errBlockedTarget) {
					slog.Warn("blocked SSRF attempt (UDP)", "target", key)
				}
				continue
			}
			// Outer-TCP/Yamux scheduling stalls should not immediately overflow the
			// connected UDP socket and lose QUIC/voice responses. Linux may clamp these
			// requests to net.core.{r,w}mem_max; bare-metal installation raises those
			// ceilings symmetrically on both tunnel endpoints.
			_ = uconn.SetReadBuffer(udpSocketBufferBytes)
			_ = uconn.SetWriteBuffer(udpSocketBufferBytes)
			f = &udpFlow{conn: uconn, target: addr}
			flows[key] = f

			// Capture explicit immutable copies for the delayed timer callback. This
			// guarantees an old callback can target only the flow instance that made it.
			flowKey, flowPtr := key, f
			f.arm(func() { closeFlow(flowKey, flowPtr) })
			go udpResponseReader(f, key, stream, &streamWriteMu, closeFlow)
		}
		mu.Unlock()

		f.touch()
		if _, werr := f.conn.Write(payload); werr != nil {
			closeFlow(key, f)
		}
	}
}

// udpResponseReader reads datagrams coming back from the target and writes them to
// the tunnel stream, until the exact flow socket closes.
func udpResponseReader(
	f *udpFlow,
	key string,
	stream net.Conn,
	streamWriteMu *sync.Mutex,
	closeFlow func(string, *udpFlow),
) {
	defer closeFlow(key, f)
	buf := make([]byte, udpReadBufSize)
	for {
		n, err := f.conn.Read(buf)
		if err != nil {
			return
		}
		f.touch()
		streamWriteMu.Lock()
		werr := tunproto.WriteDatagram(stream, f.target, buf[:n])
		streamWriteMu.Unlock()
		if werr != nil {
			return
		}
	}
}
