#!/usr/bin/env sh
# One-line installer for agent-relay: detects OS/arch, downloads the
# matching release asset from GitHub Releases, verifies its checksum, and
# installs the binary to ~/.local/bin.
#
#   curl -fsSL https://raw.githubusercontent.com/invokr-agent/agent-relay/main/install.sh | sh
#
# Env overrides:
#   AGENT_RELAY_INSTALL_REPO      owner/repo to pull releases from (required if
#                              the script isn't fetched from the real repo URL)
#   AGENT_RELAY_INSTALL_VERSION   tag to install, default: latest
#   AGENT_RELAY_INSTALL_DIR       install directory, default: $HOME/.local/bin
set -eu

REPO="${AGENT_RELAY_INSTALL_REPO:-invokr-agent/agent-relay}"
VERSION="${AGENT_RELAY_INSTALL_VERSION:-latest}"
INSTALL_DIR="${AGENT_RELAY_INSTALL_DIR:-$HOME/.local/bin}"
BIN_NAME="agent-relay"

log() { printf '%s\n' "$*" >&2; }
die() {
	log "error: $*"
	exit 1
}

detect_platform() {
	os="$(uname -s)"
	arch="$(uname -m)"
	case "$os" in
	Linux) goos=linux ;;
	Darwin) goos=darwin ;;
	*)
		die "unsupported OS: $os. agent-relay ships prebuilt binaries for Linux and macOS only; on Windows, use WSL."
		;;
	esac
	case "$arch" in
	x86_64 | amd64) goarch=amd64 ;;
	arm64 | aarch64) goarch=arm64 ;;
	*) die "unsupported architecture: $arch" ;;
	esac
}

resolve_version() {
	if [ "$VERSION" != "latest" ]; then
		return
	fi
	api_url="https://api.github.com/repos/${REPO}/releases/latest"
	VERSION="$(curl -fsSL "$api_url" | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')"
	[ -n "$VERSION" ] || die "could not resolve latest release tag from $api_url"
}

install_binary() {
	asset="${BIN_NAME}_${VERSION#v}_${goos}_${goarch}.tar.gz"
	base_url="https://github.com/${REPO}/releases/download/${VERSION}"
	tmp_dir="$(mktemp -d)"
	trap 'rm -rf "$tmp_dir"' EXIT

	log "downloading ${asset} (${VERSION})..."
	curl -fsSL "${base_url}/${asset}" -o "${tmp_dir}/${asset}" ||
		die "download failed: ${base_url}/${asset}"
	curl -fsSL "${base_url}/checksums.txt" -o "${tmp_dir}/checksums.txt" ||
		die "download failed: ${base_url}/checksums.txt"

	log "verifying checksum..."
	(
		cd "$tmp_dir"
		if command -v sha256sum >/dev/null 2>&1; then
			grep " ${asset}\$" checksums.txt | sha256sum -c - >/dev/null
		elif command -v shasum >/dev/null 2>&1; then
			grep " ${asset}\$" checksums.txt | shasum -a 256 -c - >/dev/null
		else
			die "neither sha256sum nor shasum found; cannot verify checksum"
		fi
	) || die "checksum verification failed for ${asset}"

	tar -xzf "${tmp_dir}/${asset}" -C "$tmp_dir" "$BIN_NAME"
	mkdir -p "$INSTALL_DIR"
	mv "${tmp_dir}/${BIN_NAME}" "${INSTALL_DIR}/${BIN_NAME}"
	chmod +x "${INSTALL_DIR}/${BIN_NAME}"
	log "installed ${INSTALL_DIR}/${BIN_NAME}"

	case ":$PATH:" in
	*":$INSTALL_DIR:"*) ;;
	*) log "note: $INSTALL_DIR is not on your PATH — add it to your shell profile" ;;
	esac
}

detect_platform
resolve_version
install_binary
