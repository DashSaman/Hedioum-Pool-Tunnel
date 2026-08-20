package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/fatih/color"
)

func runSubcommand(name string, args []string) {
	switch name {
	case "version":
		printVersion()
	case "setup-foreign":
		cmdSetupForeign(args)
	case "setup-iran":
		cmdSetupIran(args)
	case "add-node":
		cmdAddNode(args)
	case "edit-node":
		cmdEditNode(args)
	case "remove-node":
		cmdRemoveNode(args)
	case "edit-foreign":
		cmdEditForeign(args)
	case "install":
		cmdInstall(args)
	case "uninstall":
		cmdUninstall(args)
	case "update":
		cmdUpdate(args)
	case "speedtest":
		cmdSpeedtest(args)
	case "probe":
		cmdProbe(args)
	case "test":
		cmdTestConnection(args)
	case "optimize":
		cmdOptimize(args)
	case "check-ip":
		cmdCheckIP(args)
	case "help", "-h", "--help":
		printUsage()
	default:
		color.Red("[x] Unknown command %q", name)
		printUsage()
		os.Exit(2)
	}
}

func printUsage() {
	fmt.Print(`Hedioum Dynamic Pool Tunnel

Usage:
  hedioum-tunnel                         Interactive dashboard (TTY) or daemon (systemd)
  hedioum-tunnel version                 Print version and build info
  hedioum-tunnel install                 Install/enable the systemd service (self-copy)
  hedioum-tunnel uninstall [--yes]       Stop, disable, and remove everything
  hedioum-tunnel update [--file PATH]    Update from GitHub, or from a local binary

  hedioum-tunnel setup-foreign [flags]   Write the foreign (egress) config
      --persona performance              Recommended: SSH-free single-layer TLS set
      --egress-mode ipv4|ipv6|dual       --egress-bind-ip IP

  hedioum-tunnel edit-foreign [flags]    Edit the foreign config in place
  hedioum-tunnel setup-iran   [flags]    Write the Iran (hub) config with one node
  hedioum-tunnel add-node     [flags]    Append a foreign node to the hub config
      --alias NAME  --socks-port N  --token PAIRING_TOKEN
      [--min N --max N --bw N --jitter N]
      [--tun [--tun-name hedioumN] [--tun-addr 10.200.N.1/24] [--dns]]
  hedioum-tunnel edit-node --alias NAME [flags]
  hedioum-tunnel remove-node --alias NAME

  hedioum-tunnel probe     [--node NAME]
  hedioum-tunnel test      [--node NAME]
  hedioum-tunnel speedtest [--node NAME] [--mimic TYPE] [--seconds N] [--dir down|up|both]
  hedioum-tunnel optimize  [--node NAME] [--seconds 3] [--apply=true]
      Receiver-measure every endpoint in both directions, rank by worst-direction
      throughput + symmetry + RTT, save best-first order, and restart the daemon.
  hedioum-tunnel check-ip

Flags for the default mode:
  --reset            Wipe the config and re-run the setup wizard
  --open-firewall    Open configured foreign listen ports and exit
`)
}

func validPort(p int) error {
	if p < 1 || p > 65535 {
		return fmt.Errorf("port %d out of range (1..65535)", p)
	}
	return nil
}

func validIP(s string) error {
	if net.ParseIP(s) == nil {
		return fmt.Errorf("invalid IP address %q", s)
	}
	return nil
}

func validTarget(s string) error {
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		return fmt.Errorf("target must be HOST:PORT: %w", err)
	}
	if host == "" {
		return fmt.Errorf("target host is empty")
	}
	if net.ParseIP(host) == nil && strings.ContainsAny(host, " \t/\\") {
		return fmt.Errorf("invalid target host %q", host)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid target port %q", portStr)
	}
	return validPort(port)
}

func validToken(s string) error {
	if len(s) < 8 {
		return fmt.Errorf("token too short (need >= 8 chars)")
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return fmt.Errorf("token must be hexadecimal")
		}
	}
	return nil
}

func fail(format string, a ...interface{}) {
	color.Red("[x] "+format, a...)
	os.Exit(1)
}
