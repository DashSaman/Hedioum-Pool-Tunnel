package egress

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/tunproto"
)

func TestSpeedtestDownload(t *testing.T) {
	c, s := net.Pipe()
	defer c.Close()
	done := make(chan struct{})
	go func() { handleSpeedtestStream(s); s.Close(); close(done) }()

	if _, err := c.Write([]byte{tunproto.SpeedDown, 0x00, 0x01}); err != nil {
		t.Fatal(err)
	}
	c.SetReadDeadline(time.Now().Add(4 * time.Second))
	buf := make([]byte, speedtestChunk)
	total := 0
	for {
		n, err := c.Read(buf)
		total += n
		if err != nil {
			break
		}
	}
	if total == 0 {
		t.Fatal("download produced no data")
	}
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("handler did not stop near the deadline")
	}
}

func TestSpeedtestUploadReceiverReport(t *testing.T) {
	c, s := net.Pipe()
	defer c.Close()
	done := make(chan struct{})
	go func() { handleSpeedtestStream(s); s.Close(); close(done) }()

	if _, err := c.Write([]byte{tunproto.SpeedUp, 0x00, 0x01}); err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 256*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	if _, err := c.Write(payload); err != nil {
		t.Fatalf("upload write: %v", err)
	}
	// net.Pipe has no TCP-style CloseWrite, so for this unit test we signal EOF by
	// closing the writer-side pipe and read the result through a second in-memory
	// path would be impossible. Instead use a real loopback TCP pair below.
	_ = c.Close()
	<-done
}

func TestSpeedtestUploadReceiverReportTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverDone := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			handleSpeedtestStream(conn)
			_ = conn.Close()
		}
		close(serverDone)
	}()

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn := raw.(*net.TCPConn)
	defer conn.Close()
	if _, err := conn.Write([]byte{tunproto.SpeedUp, 0x00, 0x01}); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 384*1024)
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := conn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	result, err := tunproto.ReadSpeedtestResult(conn)
	if err != nil {
		t.Fatalf("read receiver result: %v", err)
	}
	if result.Bytes != uint64(len(payload)) {
		t.Fatalf("foreign counted %d bytes, want %d", result.Bytes, len(payload))
	}
	if result.Elapsed <= 0 {
		t.Fatalf("invalid elapsed: %v", result.Elapsed)
	}
	if _, err := io.Copy(io.Discard, conn); err != nil {
		t.Fatalf("drain: %v", err)
	}
	<-serverDone
}
