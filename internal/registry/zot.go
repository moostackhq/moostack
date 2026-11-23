package registry

import (
	"encoding/json"
	"fmt"
	"net/netip"

	"github.com/moostackhq/moostack/internal/sshclient"
)

// Name is the registry name that nodes pull the cluster's own app images
// from.
const Name = "moostack.local"

// Port is the registry's port on the control plane's private address.
const Port = 5000

// KeptImages is how many of an app's images the registry keeps besides
// the one that runs: the ones pushed last.
const KeptImages = 3

// LiveTag is the tag of the image of a repository that runs, which the
// registry always keeps.
const LiveTag = "live"

// Image is the image of digest in the repository repo, as nodes pull it.
func Image(repo, digest string) string {
	return Name + "/" + repo + "@sha256:" + digest
}

const (
	zotVersion = "v2.1.23"
	zotUnit    = "moostack-zot.service"
	zotConfig  = "/etc/moostack/zot.json"
)

// zotSHA256 are the checksums of zot's minimal builds, by architecture.
var zotSHA256 = map[string]string{
	"amd64": "8a6e77686bc0e7751766c49828cff6c66b115c854d350c9d936b855463f6c973",
	"arm64": "931d687899cc4b1fcad4c026d2792df6132e6de1b2f329aa730dccee14ec0395",
}

// downloadLimits bound a download: it fails after 10 minutes, or below
// 10 KB/s for a minute.
const downloadLimits = "--max-time 600 --speed-limit 10240 --speed-time 60"

// ZotScript installs zot on the control plane, of architecture arch,
// listening on its private address, and waits until it answers. It runs
// the script first before it starts its download.
func ZotScript(arch string, private netip.Addr, first string) string {
	listen := netip.AddrPortFrom(private, Port)
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
		"http": map[string]any{"address": private.String(), "port": fmt.Sprint(Port), "readTimeout": "30m", "writeTimeout": "30m"},
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
%[9]s
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
exit 1`, sshclient.ShellQuote(url), zotSHA256[arch], config, zotUnit, unit, listen, downloadLimits, zotConfig, first)
}

// ZotUninstallScript removes zot and its images from the control plane,
// after waiting up to a minute for an install still running to end. The
// quotes in it keep its own command line from matching its pgrep pattern.
const ZotUninstallScript = `for i in $(seq 60); do
	pgrep -f 'zot[.]downloa[d]' >/dev/null || break
	sleep 1
done
systemctl disable --now ` + zotUnit + ` 2>/dev/null
rm -rf /var/lib/zot /var/lib/private/zot
rm -f /usr/local/bin/zot /usr/local/bin/zot.downloa"d" ` + zotConfig + ` /etc/systemd/system/` + zotUnit + `
rmdir /etc/moostack 2>/dev/null
systemctl daemon-reload`
