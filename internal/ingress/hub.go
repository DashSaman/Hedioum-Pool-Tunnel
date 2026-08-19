package ingress

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/hedioum/Hedioum-Pool-Tunnel/config"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/pipe"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/pool"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/sysutil"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/tundev"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/tunproto"
)

func StartIranHub(cfg *config.AppConfig) {
	hubManager := pool.NewHubManager()

	for _, node := range cfg.ForeignNodes {
		nodeCopy := node
		dialer := newEndpointDialer(nodeCopy)
		hubManager.RegisterNode(nodeCopy, dialer.dial)
		go startLocalSocksListener(nodeCopy, hubManager)
	}

	tunInstances := startTunInterfaces(cfg.ForeignNodes)

	if len(cfg.ForeignNodes) == 0 {
		slog.Warn("iran hub started with no foreign nodes; add a node and restart")
	}

	sysutil.WaitForTerminationSignal()
	hubManager.Close()
	for _, inst := range tunInstances {
		_ = inst.Close()
	}
}

func startTunInterfaces(nodes []config.ForeignNode) []*tundev.Instance {
	var instances []*tundev.Instance
	for _, node := range nodes {
		if !node.TunEnabled && !node.GatewayEnabled {
			continue
		}
		if node.TunName == "" || node.TunAddr == "" {
			slog.Warn("TUN/gateway enabled but interface/address missing; skipping", "node", node.Alias)
			continue
		}
		inst, err := tundev.Start(tundev.Node{
			Name:         node.TunName,
			Addr:         node.TunAddr,
			SocksAddr:    fmt.Sprintf("127.0.0.1:%d", node.LocalSocksPort),
			EnableDNS:    node.DNSEnabled,
			Gateway:      node.GatewayEnabled,
			GatewayIface: node.GatewayIface,
			GatewayLAN:   node.GatewayLAN,
		})
		if err != nil {
			slog.Warn("TUN not started for node (SOCKS still active)",
				"node", node.Alias, "iface", node.TunName, "err", err)
			continue
		}
		slog.Info("TUN egress active",
			"node", node.Alias, "iface", node.TunName, "addr", node.TunAddr,
			"dns", node.DNSEnabled, "gateway", node.GatewayEnabled)
		instances = append(instances, inst)
	}
	return instances
}

func startLocalSocksListener(node config.ForeignNode, hubManager *pool.HubManager) {
	bind := node.SocksBind
	if bind == "" {
		bind = "127.0.0.1"
	}
	listenAddr := net.JoinHostPort(bind, fmt.Sprintf("%d", node.LocalSocksPort))
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		slog.Error("failed to bind SOCKS5 listener", "node", node.Alias, "addr", listenAddr, "err", err)
		return
	}
	defer listener.Close()

	if bind != "127.0.0.1" && bind != "localhost" && bind != "::1" {
		slog.Warn("SOCKS5 bound to a non-loopback address — ensure this network is trusted (open proxy risk)",
			"node", node.Alias, "addr", listenAddr)
	}
	slog.Info("SOCKS5 ingress active", "node", node.Alias, "addr", listenAddr)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Warn("SOCKS5 accept failed; retrying", "node", node.Alias, "err", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		go handleClientTraffic(clientConn, node.Alias, hubManager)
	}
}

func handleClientTraffic(localConn net.Conn, nodeAlias string, hubManager *pool.HubManager) {
	defer localConn.Close()

	cmd, dst, err := readSocksRequest(localConn)
	if err != nil {
		slog.Debug("socks handshake failed", "node", nodeAlias, "err", err)
		return
	}
	// The 3-second deadline belongs ONLY to parsing the local SOCKS greeting/request.
	// Pool recovery/OpenStream may legitimately take longer on a lossy WAN. Leaving
	// this deadline armed caused a later, successful tunnel acquisition to be thrown
	// away because the final SOCKS reply hit the already-expired local deadline.
	_ = localConn.SetDeadline(time.Time{})

	switch cmd {
	case cmdConnect:
		handleTCPConnect(localConn, dst, nodeAlias, hubManager)
	case cmdUDPAssociate:
		handleUDPAssociate(localConn, nodeAlias, hubManager)
	default:
		slog.Debug("unsupported SOCKS command", "node", nodeAlias, "cmd", cmd)
		_ = sendSocksReply(localConn, repCmdNotSupported, net.IPv4zero, 0)
	}
}

func handleTCPConnect(localConn net.Conn, targetDest, nodeAlias string, hubManager *pool.HubManager) {
	stream, err := hubManager.GetStreamTCP(nodeAlias)
	if err != nil {
		_ = sendSocksReply(localConn, repGeneralFailure, net.IPv4zero, 0)
		slog.Debug("tcp pool unavailable", "node", nodeAlias, "err", err)
		return
	}
	defer stream.Close()

	if err := tunproto.WriteTCPHeader(stream, targetDest); err != nil {
		_ = sendSocksReply(localConn, repGeneralFailure, net.IPv4zero, 0)
		return
	}
	if err := sendSocksReply(localConn, repSuccess, net.IPv4zero, 0); err != nil {
		return
	}

	_ = pipe.Bidirectional(localConn, stream)
}
