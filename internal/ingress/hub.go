package ingress

import (
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

// StartIranHub initializes the SOCKS5 listeners and dynamically scaling connection
// pools for all configured foreign egress nodes.
func StartIranHub(cfg *config.AppConfig) {
	hubManager := pool.NewHubManager()

	for _, node := range cfg.ForeignNodes {
		nodeCopy := node // local copy for the closure

		// The dialer spreads new physical pipes across this node's endpoints with a
		// fluctuating per-server mimic distribution (see dial.go).
		dialer := newEndpointDialer(nodeCopy)
		hubManager.RegisterNode(nodeCopy, dialer.dial)

		go startLocalSocksListener(nodeCopy, hubManager)
	}

	// Bring up an OS-level TUN interface for every node that opted in.
	tunInstances := startTunInterfaces(cfg.ForeignNodes)

	if len(cfg.ForeignNodes) == 0 {
		slog.Warn("iran hub started with no foreign nodes; add a node and restart")
	}

	sysutil.WaitForTerminationSignal()

	// Stop the pool watchdogs/dials first so shutdown cannot create fresh sockets
	// while TUN interfaces are being removed.
	hubManager.Close()
	for _, inst := range tunInstances {
		_ = inst.Close()
	}
}

// startTunInterfaces opens a TUN interface for each TUN-enabled node and returns
// the running instances (to close on shutdown). Nodes without TUN are skipped;
// a per-node failure is logged and does not stop the others or the SOCKS path.
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

// startLocalSocksListener boots up a TCP server for the node's SOCKS5 port. It
// binds to node.SocksBind (default 127.0.0.1, private to the host, for X-UI/Xray
// colocated on the box); set to the container veth IP / 0.0.0.0 so LAN/router
// clients can reach it when the hub runs in a container.
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

	if bind != "127.0.0.1" && bind != "localhost" && bind != "::1" {
		slog.Warn("SOCKS5 bound to a non-loopback address — ensure this network is trusted (open proxy risk)",
			"node", node.Alias, "addr", listenAddr)
	}
	slog.Info("SOCKS5 ingress active", "node", node.Alias, "addr", listenAddr)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleClientTraffic(clientConn, node.Alias, hubManager)
	}
}

// handleClientTraffic processes the local SOCKS5 handshake, extracts the target metadata,
// and multiplexes the payload over a Yamux stream.
func handleClientTraffic(localConn net.Conn, nodeAlias string, hubManager *pool.HubManager) {
	defer localConn.Close()

	cmd, dst, err := readSocksRequest(localConn)
	if err != nil {
		slog.Debug("socks handshake failed", "node", nodeAlias, "err", err)
		return
	}

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

// handleTCPConnect multiplexes a SOCKS5 CONNECT over a Yamux stream to the egress.
func handleTCPConnect(localConn net.Conn, targetDest, nodeAlias string, hubManager *pool.HubManager) {
	stream, err := hubManager.GetStreamTCP(nodeAlias)
	if err != nil {
		_ = sendSocksReply(localConn, repGeneralFailure, net.IPv4zero, 0)
		slog.Debug("tcp pool unavailable", "node", nodeAlias, "err", err)
		return
	}
	defer stream.Close()

	// Announce the stream before telling the SOCKS client that the tunnel path is
	// ready. This avoids returning REP=success when the local pool itself is dead.
	if err := tunproto.WriteTCPHeader(stream, targetDest); err != nil {
		_ = sendSocksReply(localConn, repGeneralFailure, net.IPv4zero, 0)
		return
	}
	if err := sendSocksReply(localConn, repSuccess, net.IPv4zero, 0); err != nil {
		return
	}
	_ = localConn.SetDeadline(time.Time{})

	// Wait for both directions and propagate FIN independently. Returning after the
	// first io.Copy used to truncate the still-active half of some connections.
	_ = pipe.Bidirectional(localConn, stream)
}
