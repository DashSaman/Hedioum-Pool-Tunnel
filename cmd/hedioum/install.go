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

// Conservative high-BDP tuning applied identically on Iran and foreign hosts.
// These are CEILINGS, not preallocated buffers: TCP autotuning grows only busy
// sockets as required. 32 MiB comfortably covers ~400 Mbps at ~500 ms BDP while
// avoiding the extreme global memory settings seen in many "speed tweak" scripts.
const networkSysctlConfig = `net.core.default_qdisc=fq
net.ipv4.tcp_congestion_control=bbr
net.core.rmem_max=33554432
net.core.wmem_max=33554432
net.ipv4.tcp_rmem=4096 262144 33554432
net.ipv4.tcp_wmem=4096 262144 33554432
net.core.netdev_max_backlog=16384
net.core.somaxconn=4096
net.ipv4.tcp_mtu_probing=1
`

// cmdInstall self-installs the running binary + the systemd unit, with no network
// access required — deploy by copying the binary to the server and running this.
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
	color.HiWhite("  Foreign: hedioum-tunnel setup-foreign [--move-ssh]")
	color.HiWhite("  Iran:    hedioum-tunnel setup-iran --alias ... --target IP:PORT --socks-port N --token HEX")
	color.HiWhite("  Then:    systemctl start hedioum.service")
}

// enableBBR applies congestion control plus symmetric receive/send ceilings. The
// name is kept for compatibility with the existing installer flow, but this now
// also prevents one endpoint's smaller TCP receive window from becoming a hidden
// one-way throughput cap. Best-effort: unsupported sysctls do not abort install.
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
