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
	// stagePath is deliberately in the SAME directory as binaryPath so the final
	// swap is an atomic same-filesystem rename (a /tmp staging area risks EXDEV).
	stagePath = "/usr/local/bin/hedioum-tunnel.new"
	repoAPI   = "https://api.github.com/repos/DashSaman/Hedioum-Pool-Tunnel/releases/latest"

	minBinarySize    = 1024 * 1024 // sanity floor for a real binary
	downloadAttempts = 3
)

// GitHubRelease represents the structure of the GitHub API response
type GitHubRelease struct {
	TagName string `json:"tag_name"`
	Assets  []struct {
		Name               string `json:"name"`
		BrowserDownloadURL string `json:"browser_download_url"`
	} `json:"assets"`
}

// SelfUpdate downloads and installs a newer release from GitHub, with retries,
// a semver "is-newer" check, and automatic rollback. When GitHub is unreachable
// (e.g. filtered) it prints the manual path: download the binary and run
// `hedioum-tunnel update --file /path`.
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

// UpdateFromFile installs a locally-provided binary (the manual fallback when
// GitHub is blocked), using the same backup/rollback flow.
func UpdateFromFile(path string) {
	color.Cyan("[*] Installing from %s ...", path)
	if err := copyFile(path, stagePath); err != nil {
		color.Red("[x] Could not read %s: %v", path, err)
		return
	}
	defer os.Remove(stagePath)
	installStaged("manual:" + path)
}

// fetchLatestRelease queries this fork's GitHub releases API.
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

// downloadWithRetry downloads url to dst directly from GitHub, retrying transient
// failures. No third-party proxy is used.
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
// rollback. stagePath and binaryPath share a directory, so the swap is atomic.
func installStaged(label string) {
	if st, err := os.Stat(stagePath); err != nil || st.Size() < minBinarySize {
		color.Red("[x] Staged binary missing or too small; aborting update.")
		return
	}
	os.Chmod(stagePath, 0755)

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
	os.Chmod(binaryPath, 0755)

	color.Cyan("[*] Restarting daemon...")
	exec.Command("systemctl", "restart", "hedioum.service").Run()
	time.Sleep(2 * time.Second)
	if err := exec.Command("systemctl", "is-active", "--quiet", "hedioum.service").Run(); err != nil {
		color.HiRed("[!] New version failed to start; rolling back!")
		rollback()
		return
	}

	os.Remove(backupPath)
	color.Green("\n[✓] Update successful (%s).", label)
}

// copyFile copies src to dst (used for the manual --file path; handles src on a
// different filesystem than dst).
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
	return out.Close()
}

// rollback restores the previous binary and restarts the service
func rollback() {
	if err := os.Rename(backupPath, binaryPath); err != nil {
		color.Red("[x] FATAL: Rollback failed! Manual intervention required.")
		return
	}
	exec.Command("systemctl", "restart", "hedioum.service").Run()
	color.Yellow("[-] System has been successfully rolled back to the previous version.")
}

// Uninstall safely purges all Hedioum components from the server
func Uninstall() {
	color.Yellow("[*] Stopping and disabling Hedioum service...")
	exec.Command("systemctl", "stop", "hedioum.service").Run()
	exec.Command("systemctl", "disable", "hedioum.service").Run()

	color.Yellow("[*] Removing Systemd service file...")
	os.Remove("/etc/systemd/system/hedioum.service")
	exec.Command("systemctl", "daemon-reload").Run()

	color.Yellow("[*] Removing binaries and configuration files...")
	os.RemoveAll("/etc/hedioum")
	os.Remove(binaryPath)
	os.Remove(backupPath)

	if isUFWActive() {
		color.Yellow("[*] Removing UFW firewall rule for port 2022...")
		exec.Command("ufw", "delete", "allow", "2022/tcp").Run()
	}

	color.Green("[✓] Hedioum has been completely removed from this system.")
	color.HiRed("IMPORTANT: Remember to manually change your SSH port back to 22 in '/etc/ssh/sshd_config' if you moved it during installation!")
	os.Exit(0)
}
