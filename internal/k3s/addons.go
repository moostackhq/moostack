package k3s

import (
	"encoding/json"
	"fmt"
	"net/netip"

	"github.com/moostackhq/moostack/internal/sshclient"
)

// zot, the registry of the cluster's app images.
const (
	zotVersion = "v2.1.23"
	// ZotUnit is the registry's systemd unit.
	ZotUnit   = "moostack-zot.service"
	zotConfig = "/etc/moostack/zot.json"
)

// zotSHA256 are the checksums of zot's minimal builds, by architecture.
var zotSHA256 = map[string]string{
	"amd64": "8a6e77686bc0e7751766c49828cff6c66b115c854d350c9d936b855463f6c973",
	"arm64": "931d687899cc4b1fcad4c026d2792df6132e6de1b2f329aa730dccee14ec0395",
}

// KeptImages is how many of an app's images zot keeps besides the one
// that runs: the ones pushed last.
const KeptImages = 3

// LiveTag is the tag of the image of a repository that runs, which zot
// always keeps.
const LiveTag = "live"

// ZotScript installs zot on the control plane, of architecture arch,
// listening on its private address, and waits until it answers. It
// starts its download once k3s's has ended, or after a minute.
func ZotScript(arch string, private netip.Addr) string {
	listen := netip.AddrPortFrom(private, RegistryPort)
	config, err := json.Marshal(map[string]any{
		"distSpecVersion": "1.1.1",
		"storage": map[string]any{
			"rootDirectory": "/var/lib/zot",
			"gc":            true,
			"retention": map[string]any{
				"delay": "1h",
				"policies": []any{map[string]any{
					"repositories":    []string{"**"},
					"deleteReferrers": true,
					"keepTags": []any{
						map[string]any{"patterns": []string{LiveTag}},
						map[string]any{"mostRecentlyPushedCount": KeptImages},
					},
				}},
			},
		},
		"http": map[string]any{"address": private.String(), "port": fmt.Sprint(RegistryPort), "readTimeout": "30m", "writeTimeout": "30m"},
		"log":  map[string]any{"level": "warn"},
	})
	if err != nil {
		panic(err)
	}
	unit := `[Unit]
Description=zot, the registry of the cluster's app images
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
ExecStart=/usr/local/bin/zot serve ` + zotConfig + `
DynamicUser=yes
StateDirectory=zot
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
`
	url := fmt.Sprintf("https://github.com/project-zot/zot/releases/download/%s/zot-linux-%s-minimal", zotVersion, arch)
	return fmt.Sprintf(`set -e
i=0
until [ -e /usr/local/bin/k3s ] || [ "$i" -ge 300 ]; do
	[ "$i" -ge 10 ] && ! pgrep -f 'INSTALL_K3[S]_EXEC' >/dev/null && break
	i=$((i + 1))
	sleep 0.2
done
curl -sfL --retry 3 %[7]s -o /usr/local/bin/zot.download %[1]s
echo '%[2]s  /usr/local/bin/zot.download' | sha256sum -c --quiet
chmod 755 /usr/local/bin/zot.download
mv /usr/local/bin/zot.download /usr/local/bin/zot
mkdir -p /etc/moostack
cat > %[8]s <<'EOF'
%[3]s
EOF
cat > /etc/systemd/system/%[4]s <<'EOF'
%[5]sEOF
systemctl daemon-reload
systemctl enable --now %[4]s
for i in $(seq 60); do
	curl -sf -o /dev/null http://%[6]s/v2/ && exit 0
	sleep 1
done
echo "zot doesn't answer at %[6]s" >&2
journalctl -u %[4]s -n 20 --no-pager >&2
exit 1`, sshclient.ShellQuote(url), zotSHA256[arch], config, ZotUnit, unit, listen, downloadLimits, zotConfig)
}

// TraefikConfig is the HelmChartConfig that has k3s's Traefik redirect
// HTTP to HTTPS, but for ACME's HTTP-01 challenges, route Ingresses to
// ExternalName Services, and run on every node behind a Service that
// sends a node's traffic to that node's Traefik, which keeps the
// visitor's address.
const TraefikConfig = `apiVersion: helm.cattle.io/v1
kind: HelmChartConfig
metadata:
  name: traefik
  namespace: kube-system
spec:
  valuesContent: |-
    ports:
      web:
        allowACMEByPass: true
        http:
          redirections:
            entryPoint:
              to: websecure
              scheme: https
              permanent: true
    providers:
      kubernetesIngress:
        allowExternalNameServices: true
    deployment:
      kind: DaemonSet
    service:
      spec:
        externalTrafficPolicy: Local
`

// cert-manager, which issues the certificates of apps' ingresses.
const certManagerVersion = "v1.21.3"

// Issuer is the ClusterIssuer that apps' ingresses get their certificates
// from.
const Issuer = "moostack"

// servicesCheck defines ready, which succeeds once the cluster's
// services are ready for apps: the issuer, cert-manager's webhook,
// Traefik and its ports. Otherwise missing names those that aren't, each
// followed by a comma.
const servicesCheck = `available() {
	n=$(k3s kubectl get "$1" "$3" -n "$2" --request-timeout=20s -o jsonpath="{.status.$4}" 2>/dev/null)
	[ "${n:-0}" -gt 0 ] 2>/dev/null
}
ready() {
	missing=
	[ "$(k3s kubectl get clusterissuer ` + Issuer + ` --request-timeout=20s -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" = True ] || missing="$missing the issuer ` + Issuer + `,"
	available deployment cert-manager cert-manager-webhook availableReplicas || missing="$missing cert-manager's webhook,"
	available daemonset kube-system traefik numberAvailable || missing="$missing Traefik,"
	[ -n "$(k3s kubectl get pods -n kube-system -l svccontroller.k3s.cattle.io/svcname=traefik --field-selector=status.phase=Running --request-timeout=20s -o name 2>/dev/null)" ] || missing="$missing Traefik's ports,"
	[ -z "$missing" ]
}
`

// ServicesReadyScript succeeds if the cluster's services are ready for
// apps, and fails, printing nothing, if not.
const ServicesReadyScript = servicesCheck + "ready"

// WaitServicesScript waits up to 5 minutes for the cluster's services to
// be ready for apps, and fails naming those that aren't.
const WaitServicesScript = servicesCheck + `i=0
until ready; do
	i=$((i + 1))
	if [ "$i" -ge 150 ]; then
		missing=${missing%,}
		echo "not ready after 5 minutes:$missing" >&2
		exit 1
	fi
	sleep 2
done`

// CertManager is the HelmChart that installs cert-manager.
func CertManager() string {
	return fmt.Sprintf(`apiVersion: helm.cattle.io/v1
kind: HelmChart
metadata:
  name: cert-manager
  namespace: kube-system
spec:
  repo: https://charts.jetstack.io
  chart: cert-manager
  version: %s
  targetNamespace: cert-manager
  createNamespace: true
  valuesContent: |-
    crds:
      enabled: true
`, certManagerVersion)
}

// IssuerManifest is the ClusterIssuer: Let's Encrypt's, over HTTP-01
// through Traefik, for email, or a self-signed one without.
func IssuerManifest(email string) string {
	if email == "" {
		return fmt.Sprintf(`apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: %s
spec:
  selfSigned: {}
`, Issuer)
	}
	return fmt.Sprintf(`apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: %s
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: %q
    privateKeySecretRef:
      name: moostack-acme-account
    solvers:
      - http01:
          ingress:
            ingressClassName: traefik
`, Issuer, email)
}

// AutoDeployFile is the path of the manifest called name in the control
// plane's directory of manifests that k3s applies itself, and applies
// again until it takes.
func AutoDeployFile(name string) string {
	return "/var/lib/rancher/k3s/server/manifests/moostack-" + name + ".yaml"
}
