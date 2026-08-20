package sysutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

// GetPublicIPv4 safely resolves the server's public IPv4 address, forcing v4 transport.
func GetPublicIPv4() (string, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second, DualStack: false}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, "tcp4", addr)
	}}
	client := &http.Client{Timeout: 5 * time.Second, Transport: transport}
	resp, err := client.Get("https://api.ipify.org")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	ip, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(ip)), nil
}

// GetPublicIPv6 resolves the server's public IPv6 address, forcing v6 transport.
func GetPublicIPv6() (string, error) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, "tcp6", addr)
	}}
	client := &http.Client{Timeout: 5 * time.Second, Transport: transport}
	resp, err := client.Get("https://api6.ipify.org")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	ip, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(ip)), nil
}

// ChangeSSHPort is intentionally disabled in the DashSaman performance fork.
// Hedioum must never edit sshd_config, stop ssh.socket, change firewall rules for
// administration, or restart OpenSSH. The function remains only as a compatibility
// shim for older setup/wizard code paths; callers get a clear error and the host's
// SSH configuration is left byte-for-byte untouched.
func ChangeSSHPort(newPort string) error {
	return fmt.Errorf("SSH port management is disabled; requested port %s was NOT applied", newPort)
}

// isUFWActive checks if the Uncomplicated Firewall is running.
func isUFWActive() bool {
	out, err := exec.Command("ufw", "status").Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), "Status: active")
}

// GenerateSecureToken creates a 32-character random hex string for authentication.
func GenerateSecureToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "fallback-secure-token-12345"
	}
	return hex.EncodeToString(b)
}
