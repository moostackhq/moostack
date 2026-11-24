echo "arch=$(uname -m)"
echo "mem_kib=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
[ -d /run/systemd/system ] && echo systemd=yes
[ -e /usr/local/bin/k3s ] || [ -e /etc/rancher/k3s ] && echo k3s=yes
for c in curl flock ip; do command -v $c >/dev/null && echo "has=$c"; done
ip -o -4 addr show 2>/dev/null | awk '{print "addr=" $2 " " $4}'
ip -4 route show default 2>/dev/null | awk '{for (i = 1; i < NF; i++) if ($i == "dev") print "route=" $(i+1)}'
