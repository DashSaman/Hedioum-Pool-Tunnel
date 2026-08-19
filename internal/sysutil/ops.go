package sysutil

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/fatih/color"
)

const (
	binaryPath = "/usr/local/bin/hedioum-tunnel"
	backupPath = "/usr/local/bin/hedioum-tunnel.bak"
	stagePath  = "/usr/local/bin/hedioum-tunnel.new"
	repoAPI    = "https://api.github.com/repos/DashSaman/Hedioum-Pool-Tunnel/releases/latest"

	minBinarySize    = 1024 * 1024
	downloadAttempts = 3
)

type GitHubRelease struct {
	TagName string `json:"tag_name"`
	Assets  []struct {
		Name               string `json:"name"`
		BrowserDownloadURL string `json:"browser_download_url"`
	} `json:"assets"`
}

func SelfUpdate(currentVersion string) {
	color.Cyan("[*] Checking for updates...")

	release, err := fetchLatestRelease()
	if err != nil {
		color.Red("[x] Failed to query GitHub: %v", err)
		manualHint()
		return
	}
	if release.TagName == "" || !IsNewer(release.TagName, currentVersion) {
		color.Green("[✓] You are already running the latest version (%s).", currentVersion)
		return
	}

	asset := targetAsset()
	url := assetURL(release, asset)
	if url == "" {
		color.Red("[x] Release %s has no '%s' binary.", release.TagName, asset)
		return
	}

	color.Yellow("[*] New version %s found. Downloading (%d attempts)...", release.TagName, downloadAttempts)
	defer os.Remove(stagePath)
	if err := downloadWithRetry(url, stagePath, downloadAttempts); err != nil {
		color.Red("[x] Download failed: %v", err)
		manualHint()
		return
	}
	installStaged(release.TagName)
}

func UpdateFromFile(path string) {
	color.Cyan("[*] Installing from %s ...", path)
	if err := copyFile(path, stagePath); err != nil {
		color.Red("[x] Could not read %s: %v", path, err)
		return
	}
	defer os.Remove(stagePath)
	installStaged("manual:" + path)
}

func fetchLatestRelease() (*GitHubRelease, error) {
	client := http.Client{Timeout: 8 * time.Second}
	resp, err := client.Get(repoAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("HTTP 403 (likely API rate limit); try later")
		}
		return nil, fmt.Errorf("HTTP %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	var release GitHubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, err
	}
	return &release, nil
}

func targetAsset() string {
	if runtime.GOARCH == "arm64" {
		return "hedioum-tunnel-arm64"
	}
	return "hedioum-tunnel"
}

func assetURL(release *GitHubRelease, name string) string {
	for _, a := range release.Assets {
		if a.Name == name {
			return a.BrowserDownloadURL
		}
	}
	return ""
}

func downloadWithRetry(url, dst string, attempts int) error {
	var err error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			color.Yellow("[-] Attempt %d failed; retrying...", i)
			time.Sleep(2 * time.Second)
		}
		if err = exec.Command("curl", "-f", "-L", "-s", "-o", dst, url).Run(); err == nil {
			return nil
		}
	}
	return err
}

func manualHint() {
	color.Yellow("    GitHub may be blocked. Download '%s' manually from DashSaman/Hedioum-Pool-Tunnel Releases and run:", targetAsset())
	color.HiWhite("      hedioum-tunnel update --file /path/to/%s", targetAsset())
}

// installStaged swaps stagePath into place with backup + restart + health-check +
// rollback. Before the new daemon starts, invoke the NEW binary's privileged
// network-tune mode. That makes self-update apply the release's current symmetric
// BBR/buffer policy too; otherwise an old host could keep stale one-way window
// ceilings indefinitely even after its binary was upgraded.
func installStaged(label string) {
	if st, err := os.Stat(stagePath); err != nil || st.Size() < minBinarySize {
		color.Red("[x] Staged binary missing or too small; aborting update.")
		return
	}
	_ = os.Chmod(stagePath, 0755)

	color.Cyan("[*] Backing up the current binary...")
	if err := os.Rename(binaryPath, backupPath); err != nil {
		color.Red("[x] Failed to create backup: %v", err)
		return
	}
	if err := os.Rename(stagePath, binaryPath); err != nil {
		color.Red("[x] Failed to deploy new binary; rolling back...")
		rollback()
		return
	}
	_ = os.Chmod(binaryPath, 0755)

	// Best effort because kernels/containers may intentionally disallow some
	// sysctls. The daemon itself is still valid if tuning is unavailable.
	if err := exec.Command(binaryPath, "--network-tune").Run(); err != nil {
		color.Yellow("[-] Network tuning could not be fully applied; continuing update.")
	}

	color.Cyan("[*] Restarting daemon...")
	_ = exec.Command("systemctl", "restart", "hedioum.service").Run()
	time.Sleep(2 * time.Second)
	if err := exec.Command("systemctl", "is-active", "--quiet", "hedioum.service").Run(); err != nil {
		color.HiRed("[!] New version failed to start; rolling back!")
		rollback()
		return
	}

	_ = os.Remove(backupPath)
	color.Green("\n[✓] Update successful (%s).", label)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(dst)
		return err
	}
	return nil
}

func rollback() {
	if err := os.Rename(backupPath, binaryPath); err != nil {
		color.Red("[x] FATAL: Rollback failed! Manual intervention required.")
		return
	}
	_ = exec.Command("systemctl", "restart", "hedioum.service").Run()
	color.Yellow("[-] System has been successfully rolled back to the previous version.")
}

func Uninstall() {
	color.Yellow("[*] Stopping and disabling Hedioum service...")
	_ = exec.Command("systemctl", "stop", "hedioum.service").Run()
	_ = exec.Command("systemctl", "disable", "hedioum.service").Run()

	color.Yellow("[*] Removing Systemd service file...")
	_ = os.Remove("/etc/systemd/system/hedioum.service")
	_ = exec.Command("systemctl", "daemon-reload").Run()

	color.Yellow("[*] Removing binaries and configuration files...")
	_ = os.RemoveAll("/etc/hedioum")
	_ = os.Remove(binaryPath)
	_ = os.Remove(backupPath)

	if isUFWActive() {
		color.Yellow("[*] Removing UFW firewall rule for port 2022...")
		_ = exec.Command("ufw", "delete", "allow", "2022/tcp").Run()
	}

	color.Green("[✓] Hedioum has been completely removed from this system.")
	color.HiRed("IMPORTANT: Remember to manually change your SSH port back to 22 in '/etc/ssh/sshd_config' if you moved it during installation!")
	os.Exit(0)
}
