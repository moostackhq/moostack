#!/bin/sh
# Installs mooctl, or updates it, from the newest build of main:
#
#   curl -fsSL https://raw.githubusercontent.com/moostackhq/moostack/main/install.sh | sh
#
# MOOCTL_INSTALL_DIR is the directory mooctl is put in: /usr/local/bin if
# it can be written, else ~/.local/bin.
# MOOCTL_RELEASE_URL is where the archives are downloaded from.
set -eu

main() {
	url=${MOOCTL_RELEASE_URL:-https://github.com/moostackhq/moostack/releases/download/edge}

	case $(uname -s) in
	Linux) os=linux ;;
	Darwin) os=darwin ;;
	*) fail "mooctl has no build for $(uname -s)" ;;
	esac
	case $(uname -m) in
	x86_64 | amd64) arch=amd64 ;;
	aarch64 | arm64) arch=arm64 ;;
	*) fail "mooctl has no build for $(uname -m)" ;;
	esac

	if [ -n "${MOOCTL_INSTALL_DIR:-}" ]; then
		dir=$MOOCTL_INSTALL_DIR
	elif [ -w /usr/local/bin ]; then
		dir=/usr/local/bin
	else
		dir=$HOME/.local/bin
	fi

	archive=mooctl-$os-$arch.tar.gz
	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' EXIT

	curl -fsSL "$url/$archive" -o "$tmp/$archive" || fail "downloading $url/$archive failed"
	curl -fsSL "$url/sha256sums.txt" -o "$tmp/sha256sums.txt" || fail "downloading $url/sha256sums.txt failed"

	want=$(grep " $archive\$" "$tmp/sha256sums.txt" | cut -d ' ' -f 1)
	[ -n "$want" ] || fail "sha256sums.txt has no checksum of $archive"
	if command -v sha256sum >/dev/null 2>&1; then
		got=$(sha256sum "$tmp/$archive" | cut -d ' ' -f 1)
	else
		got=$(shasum -a 256 "$tmp/$archive" | cut -d ' ' -f 1)
	fi
	[ "$got" = "$want" ] || fail "$archive doesn't match its checksum"

	tar -C "$tmp" -xzf "$tmp/$archive" mooctl
	mkdir -p "$dir"
	mv -f "$tmp/mooctl" "$dir/mooctl"
	echo "Installed mooctl in $dir"

	case :$PATH: in
	*:"$dir":*) ;;
	*) echo "$dir isn't in PATH" ;;
	esac
}

fail() {
	echo "install.sh: $1" >&2
	exit 1
}

main
