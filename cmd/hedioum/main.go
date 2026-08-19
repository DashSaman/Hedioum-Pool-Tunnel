package main

import (
	"flag"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/hedioum/Hedioum-Pool-Tunnel/config"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/egress"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/firewall"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/ingress"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/logging"
	"golang.org/x/term"
)

// AppVersion defines the current build version for the self-updater.
// It matches the fork release tag produced by .github/workflows/fork-release.yml.
//
// v0.11.1-pv1 is the DashSaman stability/throughput hardening fork based on
// upstream v0.11.0: unthrottled hot data path, symmetric high-RTT Yamux windows,
// non-blocking pool recovery, lossless TCP half-close, real endpoint racing, and
// hardened UDP flow lifecycle/timer handling. Wire framing remains compatible.
const AppVersion = "v0.11.1-pv1"

func main() {
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		runSubcommand(os.Args[1], os.Args[2:])
		return
	}

	resetCfg := flag.Bool("reset", false, "Wipe the current configuration database and restart the setup wizard")
	openFW := flag.Bool("open-firewall", false, "Open the tunnel's listen port on the host firewall and exit (run privileged, e.g. from systemd ExecStartPre=+)")
	networkTune := flag.Bool("network-tune", false, "Apply Hedioum's symmetric BBR/high-BDP sysctls and exit (privileged maintenance mode)")
	foreground := flag.Bool("foreground", false, "Run the daemon in the foreground with logs on stdout (used by the dashboard's Debug mode)")
	flag.Parse()

	if *networkTune {
		if os.Geteuid() != 0 {
			fail("--network-tune must run as root")
		}
		enableBBR()
		return
	}
	if *openFW {
		handleOpenFirewall()
		return
	}

	if *foreground {
		cfg, err := config.LoadConfig()
		if err != nil {
			fail("no configuration to run: %v", err)
		}
		runDaemon(cfg)
		return
	}

	if *resetCfg {
		handleReset()
	}

	isFirstLaunch := false
	cfg, err := config.LoadConfig()
	if err != nil {
		isFirstLaunch = true
		if !isTerminal(os.Stdin) {
			printHeader()
			color.Yellow("[!] No configuration found and no interactive terminal detected.")
			printSetupHint()
			return
		}
		printHeader()
		color.Yellow("[!] Initializing Setup Wizard for fresh installation...\n")
		cfg = runSetupWizard()
	}

	isInteractive := isTerminal(os.Stdout)
	if isInteractive {
		if isFirstLaunch {
			color.HiBlue("\n[*] Bootstrapping background daemon...")
			err := exec.Command("systemctl", "start", "hedioum.service").Run()
			if err != nil {
				color.Yellow("[-] Note: Could not auto-start systemd service. If not using systemd, start daemon manually.")
			}
			time.Sleep(1 * time.Second)
		}
		runInteractiveDashboard(cfg)
	} else {
		runDaemon(cfg)
	}
}

func runDaemon(cfg *config.AppConfig) {
	logging.Init(false)
	slog.Info("hedioum daemon starting", "version", AppVersion, "role", cfg.Role)
	switch cfg.Role {
	case "foreign":
		egress.StartForeignDaemon(cfg)
	case "iran":
		ingress.StartIranHub(cfg)
	default:
		slog.Error("undefined role in config; refusing to start", "role", cfg.Role)
		os.Exit(1)
	}
}

func handleOpenFirewall() {
	cfg, err := config.LoadConfig()
	if err != nil {
		color.Yellow("[-] open-firewall: no configuration yet (%v); nothing to do.", err)
		return
	}
	if cfg.Role != "foreign" {
		return
	}

	for _, port := range firewallPorts(cfg) {
		backend, err := firewall.EnsurePortOpen(port)
		switch {
		case err != nil:
			color.Yellow("[-] Firewall (%s): could not open tcp/%d automatically: %v", backend, port, err)
			color.Yellow("    Please allow tcp/%d manually if remote clients cannot connect.", port)
		case backend == "none":
			color.Cyan("[i] No active host firewall detected; tcp/%d needs no rule.", port)
		default:
			color.Green("[✓] Ensured tcp/%d is open via %s.", port, backend)
		}
	}
}

func firewallPorts(cfg *config.AppConfig) []int {
	ports := make([]int, 0, len(cfg.Mimics))
	for _, ml := range cfg.Mimics {
		if ml.Port != 0 {
			ports = append(ports, ml.Port)
		}
	}
	if len(ports) == 0 {
		port := cfg.ForeignListenPort
		if port == 0 {
			port = 22
		}
		ports = append(ports, port)
	}
	if cfg.HTTPDecoyPort > 0 {
		ports = append(ports, cfg.HTTPDecoyPort)
	}
	return ports
}

func isTerminal(f *os.File) bool {
	if f == nil {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}

func printSetupHint() {
	color.HiWhite("\nConfigure non-interactively, then start the service:")
	color.HiWhite("  Foreign: hedioum-tunnel setup-foreign            (auto-picks a persona; prints a pairing token)")
	color.HiWhite("  Iran:    hedioum-tunnel setup-iran --alias NAME --token <PAIRING_TOKEN> --socks-port N")
	color.HiWhite("  Then:    systemctl start hedioum.service")
	color.HiBlack("  (The pairing token carries the IP, ports and persona — no --target-ip/--mimics needed.)")
	color.HiBlack("  (Or run 'hedioum-tunnel' on an interactive terminal for the guided wizard.)")
}

func printHeader() {
	color.Cyan("=========================================================")
	color.HiCyan("   Hedioum Dynamic Pool Tunnel - Management Dashboard")
	color.HiWhite("   Version: %s | Core: Chaos Mesh Routing", AppVersion)
	color.Cyan("=========================================================\n")
}
