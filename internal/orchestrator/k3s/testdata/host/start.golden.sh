systemctl start k3s-agent || {
	journalctl -u k3s-agent -n 20 --no-pager -o cat >&2
	exit 1
}