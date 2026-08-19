// Package tunproto defines the framing that rides inside each authenticated
// yamux stream between the Iran hub and the foreign egress. Keeping it in one
// place stops the ingress and egress sides from drifting.
package tunproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	StreamTCP       byte = 0x01
	StreamUDP       byte = 0x03
	StreamSpeedtest byte = 0x05
)

const (
	SpeedDown byte = 0x01
	SpeedUp   byte = 0x02
)

func WriteSpeedtestHeader(w io.Writer, dir byte, seconds uint16) error {
	var b [4]byte
	b[0] = StreamSpeedtest
	b[1] = dir
	binary.BigEndian.PutUint16(b[2:4], seconds)
	_, err := w.Write(b[:])
	return err
}

func ReadSpeedtestHeader(r io.Reader) (dir byte, seconds uint16, err error) {
	var b [3]byte
	if _, err = io.ReadFull(r, b[:]); err != nil {
		return 0, 0, err
	}
	return b[0], binary.BigEndian.Uint16(b[1:3]), nil
}

type SpeedtestResult struct {
	Bytes   uint64
	Elapsed time.Duration
}

func WriteSpeedtestResult(w io.Writer, result SpeedtestResult) error {
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], result.Bytes)
	ns := result.Elapsed.Nanoseconds()
	if ns < 0 {
		ns = 0
	}
	binary.BigEndian.PutUint64(b[8:16], uint64(ns))
	_, err := w.Write(b[:])
	return err
}

func ReadSpeedtestResult(r io.Reader) (SpeedtestResult, error) {
	var b [16]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return SpeedtestResult{}, err
	}
	return SpeedtestResult{
		Bytes:   binary.BigEndian.Uint64(b[0:8]),
		Elapsed: time.Duration(binary.BigEndian.Uint64(b[8:16])),
	}, nil
}

const (
	atypIPv4   byte = 0x01
	atypDomain byte = 0x03
	atypIPv6   byte = 0x04
)

const (
	maxTargetLen = 2048
	maxRecord    = 0xFFFF
)

var (
	errTargetLen  = errors.New("tunproto: target length out of range")
	errRecordLen  = errors.New("tunproto: datagram record too large")
	errShortAddr  = errors.New("tunproto: truncated address")
	errBadAtyp    = errors.New("tunproto: unknown address type")
	errEmptyChunk = errors.New("tunproto: empty datagram record")

	// Outbound UDP framing is extremely hot for QUIC. Reusing one maximum-sized
	// scratch buffer per active P avoids a heap allocation for every datagram while
	// preserving the single-Write record atomicity expected by callers.
	datagramWritePool = sync.Pool{New: func() any {
		return make([]byte, maxRecord+2)
	}}
)

func ReadStreamType(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

func WriteTCPHeader(w io.Writer, target string) error {
	if len(target) == 0 || len(target) > maxTargetLen {
		return errTargetLen
	}
	buf := make([]byte, 1+2+len(target))
	buf[0] = StreamTCP
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(target)))
	copy(buf[3:], target)
	_, err := w.Write(buf)
	return err
}

func ReadTCPTarget(r io.Reader) (string, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", err
	}
	n := int(binary.BigEndian.Uint16(lenBuf[:]))
	if n == 0 || n > maxTargetLen {
		return "", errTargetLen
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func WriteUDPHeader(w io.Writer) error {
	_, err := w.Write([]byte{StreamUDP})
	return err
}

type Addr struct {
	IP     net.IP
	Domain string
	Port   uint16
}

func (a Addr) HostPort() string {
	host := a.Domain
	if host == "" && a.IP != nil {
		host = a.IP.String()
	}
	return net.JoinHostPort(host, strconv.Itoa(int(a.Port)))
}

func (a Addr) encodedLen() int {
	if a.Domain != "" {
		return 1 + 1 + len(a.Domain) + 2
	}
	if ip4 := a.IP.To4(); ip4 != nil {
		return 1 + 4 + 2
	}
	return 1 + 16 + 2
}

func (a Addr) encode(dst []byte) int {
	n := 0
	switch {
	case a.Domain != "":
		dst[0] = atypDomain
		dst[1] = byte(len(a.Domain))
		n = 2 + copy(dst[2:], a.Domain)
	case a.IP.To4() != nil:
		dst[0] = atypIPv4
		n = 1 + copy(dst[1:], a.IP.To4())
	default:
		dst[0] = atypIPv6
		n = 1 + copy(dst[1:], a.IP.To16())
	}
	binary.BigEndian.PutUint16(dst[n:], a.Port)
	return n + 2
}

func decodeAddr(b []byte) (Addr, int, error) {
	if len(b) < 1 {
		return Addr{}, 0, errShortAddr
	}
	switch b[0] {
	case atypIPv4:
		if len(b) < 7 {
			return Addr{}, 0, errShortAddr
		}
		ip := make(net.IP, 4)
		copy(ip, b[1:5])
		return Addr{IP: ip, Port: binary.BigEndian.Uint16(b[5:7])}, 7, nil
	case atypIPv6:
		if len(b) < 19 {
			return Addr{}, 0, errShortAddr
		}
		ip := make(net.IP, 16)
		copy(ip, b[1:17])
		return Addr{IP: ip, Port: binary.BigEndian.Uint16(b[17:19])}, 19, nil
	case atypDomain:
		if len(b) < 2 {
			return Addr{}, 0, errShortAddr
		}
		dlen := int(b[1])
		end := 2 + dlen + 2
		if len(b) < end {
			return Addr{}, 0, errShortAddr
		}
		domain := string(b[2 : 2+dlen])
		return Addr{Domain: domain, Port: binary.BigEndian.Uint16(b[2+dlen : end])}, end, nil
	default:
		return Addr{}, 0, errBadAtyp
	}
}

func WriteDatagram(w io.Writer, addr Addr, payload []byte) error {
	recordLen := addr.encodedLen() + len(payload)
	if recordLen == 0 {
		return errEmptyChunk
	}
	if recordLen > maxRecord {
		return errRecordLen
	}
	storage := datagramWritePool.Get().([]byte)
	buf := storage[:2+recordLen]
	binary.BigEndian.PutUint16(buf[0:2], uint16(recordLen))
	n := addr.encode(buf[2:])
	copy(buf[2+n:], payload)
	_, err := w.Write(buf)
	datagramWritePool.Put(storage)
	return err
}

// ReadDatagram performs one allocation for the owned record. Returning record[n:]
// avoids the second payload allocation/copy that used to amplify GC at high pps.
func ReadDatagram(r io.Reader) (Addr, []byte, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return Addr{}, nil, err
	}
	recordLen := int(binary.BigEndian.Uint16(lenBuf[:]))
	if recordLen == 0 {
		return Addr{}, nil, errEmptyChunk
	}
	record := make([]byte, recordLen)
	if _, err := io.ReadFull(r, record); err != nil {
		return Addr{}, nil, err
	}
	addr, n, err := decodeAddr(record)
	if err != nil {
		return Addr{}, nil, err
	}
	return addr, record[n:], nil
}

func AddrFromUDP(u *net.UDPAddr) Addr {
	return Addr{IP: u.IP, Port: uint16(u.Port)}
}

func ParseSocksUDPHeader(b []byte) (Addr, int, error) {
	if len(b) < 3 {
		return Addr{}, 0, errShortAddr
	}
	if b[2] != 0 {
		return Addr{}, 0, fmt.Errorf("tunproto: fragmented UDP not supported (FRAG=%d)", b[2])
	}
	addr, n, err := decodeAddr(b[3:])
	if err != nil {
		return Addr{}, 0, err
	}
	return addr, 3 + n, nil
}

func BuildSocksUDPHeader(addr Addr, payload []byte) []byte {
	out := make([]byte, 3+addr.encodedLen()+len(payload))
	n := addr.encode(out[3:])
	copy(out[3+n:], payload)
	return out
}
