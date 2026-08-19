package ingress

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/pool"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/tunproto"
)

const (
	udpBufSize    = 64 * 1024
	udpQueueDepth = 256 // bounded outbound queue; UDP drops instead of growing RAM
)

// handleUDPAssociate implements SOCKS5 UDP ASSOCIATE. The tunnel stream is opened
// BEFORE REP=success, so a dead UDP pool is reported to the client instead of
// creating a relay socket that can never forward anything.
func handleUDPAssociate(ctrlConn net.Conn, nodeAlias string, hubManager *pool.HubManager) {
	relay, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		_ = sendSocksReply(ctrlConn, repGeneralFailure, net.IPv4zero, 0)
		return
	}
	defer relay.Close()

	stream, err := hubManager.GetStreamUDP(nodeAlias)
	if err != nil {
		_ = sendSocksReply(ctrlConn, repGeneralFailure, net.IPv4zero, 0)
		return
	}
	defer stream.Close()
	if err := tunproto.WriteUDPHeader(stream); err != nil {
		_ = sendSocksReply(ctrlConn, repGeneralFailure, net.IPv4zero, 0)
		return
	}

	localPort := uint16(relay.LocalAddr().(*net.UDPAddr).Port)
	if err := sendSocksReply(ctrlConn, repSuccess, net.IPv4(127, 0, 0, 1), localPort); err != nil {
		return
	}
	_ = ctrlConn.SetDeadline(time.Time{})

	relayUDP(ctrlConn, relay, stream)
}

// relayUDP pumps datagrams between the local relay UDP socket and the UDP tunnel
// stream, tearing down when the control conn or the stream closes.
func relayUDP(ctrlConn net.Conn, relay *net.UDPConn, stream net.Conn) {
	var clientAddr atomic.Pointer[net.UDPAddr]
	done := make(chan struct{})
	var once sync.Once
	closeAll := func() { once.Do(func() { close(done) }) }

	type dgram struct {
		addr tunproto.Addr
		data []byte
	}
	sendCh := make(chan dgram, udpQueueDepth)

	// Writer: single writer to the multiplexed stream.
	go func() {
		defer closeAll()
		for {
			select {
			case <-done:
				return
			case d := <-sendCh:
				if err := tunproto.WriteDatagram(stream, d.addr, d.data); err != nil {
					return
				}
			}
		}
	}()

	// A. Client -> tunnel, drop-on-congestion.
	go func() {
		defer closeAll()
		buf := make([]byte, udpBufSize)
		for {
			n, src, err := relay.ReadFromUDP(buf)
			if err != nil {
				return
			}
			clientAddr.Store(src)
			addr, off, err := tunproto.ParseSocksUDPHeader(buf[:n])
			if err != nil {
				continue
			}
			data := make([]byte, n-off)
			copy(data, buf[off:n])
			select {
			case sendCh <- dgram{addr: addr, data: data}:
			default:
				// Tunnel backed up: preserve bounded memory and UDP latency.
			}
		}
	}()

	// B. Tunnel -> client.
	go func() {
		defer closeAll()
		for {
			addr, payload, err := tunproto.ReadDatagram(stream)
			if err != nil {
				return
			}
			ca := clientAddr.Load()
			if ca == nil {
				continue
			}
			if _, err := relay.WriteToUDP(tunproto.BuildSocksUDPHeader(addr, payload), ca); err != nil {
				return
			}
		}
	}()

	// C. RFC 1928 lifetime: association ends with the control TCP connection.
	go func() {
		defer closeAll()
		_, _ = io.Copy(io.Discard, ctrlConn)
	}()

	<-done
}
