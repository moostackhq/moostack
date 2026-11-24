i=0
until [ -e /usr/local/bin/k3s ] || [ "$i" -ge 300 ]; do
	[ "$i" -ge 10 ] && ! pgrep -f 'INSTALL_K3[S]_EXEC' >/dev/null && break
	i=$((i + 1))
	sleep 0.2
done
[ -e /usr/local/bin/k3s ]