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
	udpIdleTimeout = 60 * time.Second // close a UDP flow after this much silence
	udpMaxFlows    = 256              // per-stream NAT table cap (FD/memory guard)
	udpReadBufSize = 64 * 1024
)

// dialUDP is the seam used to open a vetted UDP socket to a target; overridable
// in tests to reach a loopback echo server without tripping the SSRF gate.
var dialUDP = safeDialUDP

// udpFlow is one connected UDP socket to a single target. Timer operations and
// close state are synchronized because request and response goroutines both touch
// the idle deadline.
type udpFlow struct {
	conn   *net.UDPConn
	target tunproto.Addr

	timerMu sync.Mutex
	timer   *time.Timer
	closed  bool
}

func (f *udpFlow) arm(onIdle func()) {
	f.timerMu.Lock()
	defer f.timerMu.Unlock()
	if f.closed {
		return
	}
	if f.timer == nil {
		f.timer = time.AfterFunc(udpIdleTimeout, onIdle)
		return
	}
	f.timer.Reset(udpIdleTimeout)
}

func (f *udpFlow) touch() {
	f.timerMu.Lock()
	defer f.timerMu.Unlock()
	if !f.closed && f.timer != nil {
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
			f = &udpFlow{conn: uconn, target: addr}
			flows[key] = f

			// Capture explicit immutable copies for the delayed timer callback. This
			// avoids any dependence on loop-variable capture semantics and guarantees
			// an old timer can target only the flow instance that created it.
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
