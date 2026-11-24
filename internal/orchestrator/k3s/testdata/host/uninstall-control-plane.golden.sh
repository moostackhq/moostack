for i in $(seq 60); do
	pgrep -f 'INSTALL_K3[S]_EXEC' >/dev/null || break
	sleep 1
done
s=0
if [ -x /usr/local/bin/k3s-uninstall.sh ]; then /usr/local/bin/k3s-uninstall.sh || s=$?; fi
rm -f /etc/rancher/k3s/config.yaml /etc/rancher/k3s/registries.yaml /etc/systemd/system/k3s.service.d/memory.conf /var/lib/rancher/k3s/agent/etc/kubelet.conf.d/10-moostack.conf /var/lib/rancher/k3s/server/manifests/moostack-traefik-config.yaml /var/lib/rancher/k3s/server/manifests/moostack-cert-manager.yaml /var/lib/rancher/k3s/server/manifests/moostack-issuer.yaml
for d in /etc/rancher/k3s /etc/rancher /etc/systemd/system/k3s.service.d /var/lib/rancher/k3s/server/manifests /var/lib/rancher/k3s/server /var/lib/rancher/k3s/agent/etc/kubelet.conf.d /var/lib/rancher/k3s/agent/etc /var/lib/rancher/k3s/agent /var/lib/rancher/k3s /var/lib/rancher; do rmdir "$d" 2>/dev/null; done
systemctl daemon-reload
exit $s