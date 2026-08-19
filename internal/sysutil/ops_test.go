package sysutil

import (
	"runtime"
	"strings"
	"testing"
)

// TestForkUpdateSource prevents the fork from silently reverting to upstream
// release binaries. Installer and self-updater must remain on DashSaman's repo.
func TestForkUpdateSource(t *testing.T) {
	if !strings.Contains(repoAPI, "DashSaman/Hedioum-Pool-Tunnel") {
		t.Fatalf("self-updater points outside the fork: %q", repoAPI)
	}
}

// TestTargetAsset verifies the self-updater picks the binary matching the host
// architecture — the release-asset names install.sh and the updater rely on.
func TestTargetAsset(t *testing.T) {
	got := targetAsset()
	want := "hedioum-tunnel"
	if runtime.GOARCH == "arm64" {
		want = "hedioum-tunnel-arm64"
	}
	if got != want {
		t.Fatalf("targetAsset() = %q, want %q for GOARCH=%s", got, want, runtime.GOARCH)
	}
}

// TestAssetURL verifies asset lookup by name (and the empty result when the
// release lacks the arch-specific binary, so the updater fails closed instead of
// downloading the wrong file).
func TestAssetURL(t *testing.T) {
	rel := &GitHubRelease{TagName: "v9.9.9"}
	rel.Assets = append(rel.Assets, struct {
		Name               string `json:"name"`
		BrowserDownloadURL string `json:"browser_download_url"`
	}{Name: "hedioum-tunnel", BrowserDownloadURL: "https://example.com/amd64"})
	rel.Assets = append(rel.Assets, struct {
		Name               string `json:"name"`
		BrowserDownloadURL string `json:"browser_download_url"`
	}{Name: "hedioum-tunnel-arm64", BrowserDownloadURL: "https://example.com/arm64"})

	if got := assetURL(rel, "hedioum-tunnel"); got != "https://example.com/amd64" {
		t.Fatalf("amd64 url = %q", got)
	}
	if got := assetURL(rel, "hedioum-tunnel-arm64"); got != "https://example.com/arm64" {
		t.Fatalf("arm64 url = %q", got)
	}
	if got := assetURL(rel, "nonexistent"); got != "" {
		t.Fatalf("missing asset should return empty, got %q", got)
	}
}
