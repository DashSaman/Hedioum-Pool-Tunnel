#!/bin/bash
# ==========================================================
# Hedioum Dynamic Pool Tunnel - DashSaman performance/stability fork
#
# This script only: (1) checks the CPU architecture, (2) downloads the matching
# release binary from THIS fork, (3) runs it. The binary itself handles self-copy,
# systemd, firewall, configuration and future updates.
# ==========================================================
set -euo pipefail

if [ "${EUID:-$(id -u)}" -ne 0 ]; then
  echo "[x] Please run as root (e.g. sudo bash install.sh)"
  exit 1
fi

case "$(uname -m)" in
  aarch64 | arm64) ASSET="hedioum-tunnel-arm64" ;;
  *) ASSET="hedioum-tunnel" ;;
esac
echo "[*] Architecture asset: $ASSET"

REPO="DashSaman/Hedioum-Pool-Tunnel"
URL="https://github.com/${REPO}/releases/latest/download/${ASSET}"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

echo "[*] Downloading the latest DashSaman fork release from GitHub..."
ok=""
for attempt in 1 2 3; do
  if curl -fL --connect-timeout 15 -o "$TMP" "$URL"; then ok=1; break; fi
  echo "[-] Attempt $attempt failed; retrying..."
  sleep 2
done

if [ -z "$ok" ]; then
  cat <<EOF
[x] Download failed. GitHub may be blocked on this network, or the fork release is not available yet.
    Download '${ASSET}' manually from ${REPO} Releases, copy it to this server, then:
        chmod +x ${ASSET} && ./${ASSET} install
    and configure with 'hedioum-tunnel' (wizard) or the setup-* subcommands.
EOF
  exit 1
fi

# Refuse an obviously incomplete/error-page download before executing it.
SIZE="$(wc -c < "$TMP" 2>/dev/null || echo 0)"
if [ "$SIZE" -lt 1048576 ]; then
  echo "[x] Downloaded file is unexpectedly small (${SIZE} bytes); refusing to execute it."
  exit 1
fi

chmod +x "$TMP"

echo "[*] Installing (the binary sets up the service, firewall, and config paths)..."
"$TMP" install

echo "[*] Launching interactive setup..."
exec hedioum-tunnel
