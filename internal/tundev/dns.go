//go:build linux

package tundev

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"golang.org/x/net/proxy"
)

const (
	dnsMaxInflight = 256
	dnsDialTimeout = 5 * time.Second
)

// dnsForwarder is a tiny UDP+TCP resolver bound to the TUN gateway IP:53 that
// forwards every query over the node's SOCKS proxy as DNS-over-TCP to a public
// resolver. Inflight forwarding is explicitly bounded so an upstream outage or
// query flood cannot create an unbounded goroutine/FD storm that starves user data.
type dnsForwarder struct {
	udp      net.PacketConn
	tcp      net.Listener
	dialer   proxy.Dialer
	upstream string
	sem      chan struct{}
}

var dnsUpstreams = []string{"1.1.1.1:53", "8.8.8.8:53"}

func startDNSForwarder(gatewayIP, socksAddr string) (*dnsForwarder, error) {
	baseDialer := &net.Dialer{Timeout: dnsDialTimeout, KeepAlive: 30 * time.Second}
	d, err := proxy.SOCKS5("tcp", socksAddr, nil, baseDialer)
	if err != nil {
		return nil, fmt.Errorf("socks dialer: %w", err)
	}
	f := &dnsForwarder{
		dialer:   d,
		upstream: dnsUpstreams[0],
		sem:      make(chan struct{}, dnsMaxInflight),
	}

	uc, err := net.ListenPacket("udp", net.JoinHostPort(gatewayIP, "53"))
	if err != nil {
		return nil, fmt.Errorf("listen udp %s:53: %w", gatewayIP, err)
	}
	f.udp = uc
	go f.serveUDP()

	if tl, err := net.Listen("tcp", net.JoinHostPort(gatewayIP, "53")); err == nil {
		f.tcp = tl
		go f.serveTCP()
	}
	return f, nil
}

func (f *dnsForwarder) Close() {
	if f == nil {
		return
	}
	if f.udp != nil {
		_ = f.udp.Close()
	}
	if f.tcp != nil {
		_ = f.tcp.Close()
	}
}

func (f *dnsForwarder) tryAcquire() bool {
	select {
	case f.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (f *dnsForwarder) release() { <-f.sem }

// serveUDP drops excess DNS work when the bounded forwarding pool is saturated.
// DNS clients already retry; dropping here is substantially safer than allowing an
// outage to consume all descriptors/goroutines and degrade unrelated tunnel flows.
func (f *dnsForwarder) serveUDP() {
	buf := make([]byte, 4096)
	for {
		n, addr, err := f.udp.ReadFrom(buf)
		if err != nil {
			return
		}
		if !f.tryAcquire() {
			continue
		}
		query := make([]byte, n)
		copy(query, buf[:n])
		go func(q []byte, a net.Addr) {
			defer f.release()
			resp, err := f.forward(q)
			if err != nil {
				return
			}
			// Do not mutate a shared PacketConn deadline from many goroutines: packet
			// deadlines are connection-wide and one slow query could expire another's
			// write. A local UDP WriteTo is non-blocking under normal kernel operation.
			_, _ = f.udp.WriteTo(resp, a)
		}(query, addr)
	}
}

func (f *dnsForwarder) serveTCP() {
	for {
		conn, err := f.tcp.Accept()
		if err != nil {
			return
		}
		if !f.tryAcquire() {
			_ = conn.Close()
			continue
		}
		go func(c net.Conn) {
			defer f.release()
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(10 * time.Second))
			query, err := readDNSMessage(c)
			if err != nil {
				return
			}
			resp, err := f.forward(query)
			if err != nil {
				return
			}
			_ = writeDNSMessage(c, resp)
		}(conn)
	}
}

func (f *dnsForwarder) forward(query []byte) ([]byte, error) {
	var lastErr error
	for _, up := range dnsUpstreams {
		conn, err := f.dialer.Dial("tcp", up)
		if err != nil {
			lastErr = err
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
		if err := writeDNSMessage(conn, query); err != nil {
			_ = conn.Close()
			lastErr = err
			continue
		}
		resp, err := readDNSMessage(conn)
		_ = conn.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return resp, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no upstream resolver reachable")
	}
	return nil, lastErr
}

func readDNSMessage(r io.Reader) ([]byte, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint16(lenBuf[:])
	if n == 0 {
		return nil, fmt.Errorf("empty dns message")
	}
	msg := make([]byte, n)
	if _, err := io.ReadFull(r, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func writeDNSMessage(w io.Writer, msg []byte) error {
	if len(msg) > 0xFFFF {
		return fmt.Errorf("dns message too large: %d", len(msg))
	}
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(msg)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(msg)
	return err
}
