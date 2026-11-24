set -e
script=$(mktemp)
trap 'rm -f "$script"' EXIT
trap 'exit 143' INT TERM
curl -sfL --retry 3 --max-time 600 --speed-limit 10240 --speed-time 60 -o "$script" 'https://raw.githubusercontent.com/k3s-io/k3s/v1.37.2+k3s1/install.sh'
echo "ed01f89fd977bf20ac1516bbebf8370bf3ddbaa55dac8aba610956a4c78cc00b  $script" | sha256sum -c --quiet
INSTALL_K3S_VERSION='v1.37.2+k3s1' INSTALL_K3S_EXEC=server INSTALL_K3S_SKIP_START=false sh "$script"