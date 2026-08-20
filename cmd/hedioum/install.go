package main

import (
	"bytes"
	_ "embed"
	"flag"
	"io"
	"os"
	"os/exec"
	"text/template"

	"github.com/fatih/color"
	"github.com/hedioum/Hedioum-Pool-Tunnel/config"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/sysutil"
)

//go:embed hedioum.service.tmpl
var systemdUnit string

func renderUnit(tunCapable bool) string {
	t, err := template.New("unit").Parse(systemdUnit)
	if err != nil {
		return systemdUnit
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, struct{ TunCapable bool }{tunCapable}); err != nil {
		return systemdUnit
	}
	return buf.String()
}

func reconcileUnit(role string) {
	if _, err := os.Stat(installUnitPath); err != nil {
		return
	}
	if err := os.WriteFile(installUnitPath, []byte(renderUnit(role == "iran")), 0644); err != nil {
		return
	}
	_ = exec.Command("systemctl", "daemon-reload").Run()
}

const (
	installBinPath  = "/usr/local/bin/hedioum-tunnel"
	installUnitPath = "/etc/systemd/system/hedioum.service"
)

// Symmetric high-BDP tuning for both Iran and foreign hosts. These values are
// autotuning CEILINGS, not preallocated memory. 64 MiB leaves comfortable headroom
// above the 32 MiB Yamux stream window and covers several-hundred-Mbps paths at
// very high RTT without silently making one direction receive-window limited.
const networkSysctlConfig = `net.core.default_qdisc=fq
net.ipv4.tcp_congestion_control=bbr
net.core.rmem_max=67108864
net.core.wmem_max=67108864
net.ipv4.tcp_rmem=4096 262144 67108864
net.ipv4.tcp_wmem=4096 262144 67108864
net.core.netdev_max_backlog=32768
net.core.somaxconn=8192
net.ipv4.tcp_mtu_probing=1
net.ipv4.tcp_slow_start_after_idle=0
net.ipv4.tcp_keepalive_time=120
net.ipv4.tcp_keepalive_intvl=30
net.ipv4.tcp_keepalive_probes=4
net.ipv4.udp_rmem_min=16384
net.ipv4.udp_wmem_min=16384
`

func cmdInstall(args []string) {
	if os.Geteuid() != 0 {
		fail("install must run as root")
	}
	if err := os.MkdirAll("/etc/hedioum", 0755); err != nil {
		fail("cannot create /etc/hedioum: %v", err)
	}
	self, err := os.Executable()
	if err != nil {
		fail("cannot locate the running binary: %v", err)
	}
	if err := copyExecutable(self, installBinPath); err != nil {
		fail("failed to install binary: %v", err)
	}
	color.Green("[✓] Binary installed to %s", installBinPath)

	tunCapable := true
	if cfg, err := config.LoadConfig(); err == nil && cfg.Role == "foreign" {
		tunCapable = false
	}
	if err := os.WriteFile(installUnitPath, []byte(renderUnit(tunCapable)), 0644); err != nil {
		fail("failed to write systemd unit: %v", err)
	}
	_ = exec.Command("systemctl", "daemon-reload").Run()
	_ = exec.Command("systemctl", "enable", "hedioum.service").Run()
	color.Green("[✓] systemd service installed and enabled.")

	enableBBR()

	color.HiWhite("\nNext:")
	color.HiWhite("  Foreign: hedioum-tunnel setup-foreign --persona performance")
	color.HiWhite("  Iran:    hedioum-tunnel setup-iran --alias ... --token <PAIRING_TOKEN> --socks-port N --min 10 --max 32 --bw 80 --jitter 0")
	color.HiWhite("  Then:    systemctl start hedioum.service")
}

// enableBBR applies congestion control plus symmetric receive/send ceilings.
// Best-effort: unsupported sysctls do not abort install.
func enableBBR() {
	if err := os.WriteFile("/etc/sysctl.d/99-hedioum-bbr.conf", []byte(networkSysctlConfig), 0644); err != nil {
		return
	}
	if err := exec.Command("sysctl", "-p", "/etc/sysctl.d/99-hedioum-bbr.conf").Run(); err == nil {
		color.Green("[✓] Enabled BBR/fq and symmetric high-BDP network buffers.")
	}
}

func copyExecutable(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".new"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

func cmdUpdate(args []string) {
	fs := flag.NewFlagSet("update", flag.ExitOnError)
	file := fs.String("file", "", "install from a local binary instead of downloading")
	_ = fs.Parse(args)
	if *file != "" {
		sysutil.UpdateFromFile(*file)
		return
	}
	sysutil.SelfUpdate(AppVersion)
}

func cmdUninstall(args []string) {
	fs := flag.NewFlagSet("uninstall", flag.ExitOnError)
	yes := fs.Bool("yes", false, "confirm removal of the daemon, config, and binary")
	_ = fs.Parse(args)
	if !*yes {
		fail("refusing without --yes (this removes the daemon, config, and binary)")
	}
	sysutil.Uninstall()
}
