curl -sk --max-time 10 -o /dev/null https://10.0.0.2:6443/ping || {
	echo "it doesn't reach the control plane at 10.0.0.2:6443" >&2
	exit 1
}