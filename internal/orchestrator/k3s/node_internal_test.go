package k3s

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"fmt"
	"k8s.io/client-go/rest"
	"net/netip"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/model"
)

// factsOf are factsScript's output on a host with addrs and a default
// route on each of routes.
func factsOf(t *testing.T, addrs []string, routes ...string) facts {
	t.Helper()
	out := "arch=x86_64\nmem_kib=2013456\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\n"
	for _, a := range addrs {
		out += "addr=" + a + "\n"
	}
	for _, r := range routes {
		out += "route=" + r + "\n"
	}
	f, err := parseFacts(out)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestParseFacts(t *testing.T) {
	f := factsOf(t, []string{"lo 127.0.0.1/8", "eth0 192.0.2.10/32", "eth0:1 10.19.0.5/16", "ens10@if3 10.0.0.2/32"}, "eth0")
	if f.architecture != "amd64" || f.memoryMiB != 1966 || !f.systemd || f.k3s {
		t.Errorf("facts = %+v", f)
	}
	want := []address{
		{iface: "lo", addr: netip.MustParseAddr("127.0.0.1")},
		{iface: "eth0", addr: netip.MustParseAddr("192.0.2.10")},
		{iface: "eth0", addr: netip.MustParseAddr("10.19.0.5")},
		{iface: "ens10", addr: netip.MustParseAddr("10.0.0.2")},
	}
	if len(f.addresses) != len(want) {
		t.Fatalf("addresses = %v, want %v", f.addresses, want)
	}
	for i := range want {
		if f.addresses[i] != want[i] {
			t.Errorf("address %d = %v, want %v", i, f.addresses[i], want[i])
		}
	}
	if err := f.check(controlPlaneRole); err != nil {
		t.Errorf("Check: %v", err)
	}
}

func TestCheckRefusesHostsThatCantBeNodes(t *testing.T) {
	for name, out := range map[string]string{
		"architecture": "arch=riscv64\nmem_kib=2000000\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\n",
		"no systemd":   "arch=x86_64\nmem_kib=2000000\nhas=curl\nhas=flock\nhas=ip\n",
		"k3s already":  "arch=aarch64\nmem_kib=2000000\nsystemd=yes\nk3s=yes\nhas=curl\nhas=flock\nhas=ip\n",
		"no curl":      "arch=x86_64\nmem_kib=2000000\nsystemd=yes\nhas=flock\nhas=ip\n",
		"no flock":     "arch=x86_64\nmem_kib=2000000\nsystemd=yes\nhas=curl\nhas=ip\n",
		"no ip":        "arch=x86_64\nmem_kib=2000000\nsystemd=yes\nhas=curl\nhas=flock\n",
		"too small":    "arch=x86_64\nmem_kib=500000\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\n",
		"no memory":    "arch=x86_64\nmem_kib=-5\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\n",
	} {
		t.Run(name, func(t *testing.T) {
			f, err := parseFacts(out)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.check(agentRole); err == nil {
				t.Error("Check passed")
			}
		})
	}
}

func TestPrivateAddress(t *testing.T) {
	for _, c := range []struct {
		name   string
		addrs  []string
		routes []string
		want   string
	}{
		{"Hetzner", []string{"lo 127.0.0.1/8", "eth0 192.0.2.10/32", "enp7s0 10.0.0.2/32"}, []string{"eth0"}, "10.0.0.2 enp7s0"},
		{"Hetzner ens10", []string{"eth0 192.0.2.10/32", "ens10 10.1.0.3/32"}, []string{"eth0"}, "10.1.0.3 ens10"},
		{"DigitalOcean anchor and VPC", []string{"eth0 192.0.2.10/20", "eth0 10.19.0.5/16", "eth1 10.116.0.2/20"}, []string{"eth0"}, "10.116.0.2 eth1"},
		{"Docker", []string{"eth0 192.0.2.10/32", "ens10 10.0.0.2/32", "docker0 172.17.0.1/16", "br-1a2b 172.18.0.1/16"}, []string{"eth0"}, "10.0.0.2 ens10"},
		{"after k3s", []string{"eth0 192.0.2.10/32", "ens10 10.0.0.2/32", "flannel.1 10.42.0.0/32", "cni0 10.42.0.1/24"}, []string{"eth0"}, "10.0.0.2 ens10"},
		{"only a private network, with the default route", []string{"eth0 192.168.1.20/24"}, []string{"eth0"}, "192.168.1.20 eth0"},
		{"a management link and a private network", []string{"ens3 10.0.2.15/24", "ens4 10.0.1.2/24"}, []string{"ens3"}, "10.0.1.2 ens4"},
	} {
		t.Run(c.name, func(t *testing.T) {
			a, err := factsOf(t, c.addrs, c.routes...).privateAddress(netip.Addr{})
			if err != nil {
				t.Fatal(err)
			}
			if got := a.addr.String() + " " + a.iface; got != c.want {
				t.Errorf("got %s, want %s", got, c.want)
			}
		})
	}
}

func TestPrivateAddressRefusesWhenItCantTell(t *testing.T) {
	none := factsOf(t, []string{"lo 127.0.0.1/8", "eth0 192.0.2.10/32", "docker0 172.17.0.1/16"}, "eth0")
	if _, err := none.privateAddress(netip.Addr{}); err == nil || !strings.Contains(err.Error(), "--private-address") {
		t.Errorf("no private address: err = %v", err)
	}
	two := factsOf(t, []string{"eth0 192.0.2.10/32", "ens10 10.0.0.2/32", "ens11 10.1.0.2/32"}, "eth0")
	_, err := two.privateAddress(netip.Addr{})
	if err == nil || !strings.Contains(err.Error(), "10.0.0.2 on ens10") || !strings.Contains(err.Error(), "10.1.0.2 on ens11") {
		t.Errorf("two private networks: err = %v", err)
	}
	a, err := two.privateAddress(netip.MustParseAddr("10.1.0.2"))
	if err != nil || a.iface != "ens11" {
		t.Errorf("override: %v, %v", a, err)
	}
	if _, err := two.privateAddress(netip.MustParseAddr("10.9.9.9")); err == nil {
		t.Error("an override the host doesn't have was taken")
	}
}

func TestMemory(t *testing.T) {
	for _, c := range []struct {
		mib                    int
		controlPlane           bool
		wantGOGC, wantMemLimit string
	}{
		{1966, true, "25", "491MiB"},
		{1966, false, "25", "196MiB"},
		{4096, true, "25", "1024MiB"},
		{3900, false, "25", "390MiB"},
		{7900, true, "", "1975MiB"},
		{16000, false, "", "1600MiB"},
	} {
		gogc, limit := memorySettings(c.mib, roleOf(c.controlPlane))
		if gogc != c.wantGOGC || limit != c.wantMemLimit {
			t.Errorf("Memory(%d, %v) = %q, %q, want %q, %q", c.mib, c.controlPlane, gogc, limit, c.wantGOGC, c.wantMemLimit)
		}
	}
}

func TestMemoryDropInLeavesGOGCToGoWithoutOne(t *testing.T) {
	if got := memoryDropIn("", "1600MiB"); strings.Contains(got, "GOGC") || !strings.Contains(got, "Environment=GOMEMLIMIT=1600MiB") {
		t.Errorf("drop-in:\n%s", got)
	}
	if got := memoryDropIn("25", "491MiB"); !strings.Contains(got, "Environment=GOGC=25\n") {
		t.Errorf("drop-in:\n%s", got)
	}
}

func TestConfigs(t *testing.T) {
	n := model.Node{Name: "node2", PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10"}
	agent := agentConfig(n, netip.MustParseAddr("10.0.0.2"), "K10::server:abc")
	for _, want := range []string{"server: https://10.0.0.2:6443", "token: K10::server:abc", "node-ip: 10.0.0.3", "flannel-iface: ens10", "node-name: node2"} {
		if !strings.Contains(agent, want) {
			t.Errorf("agent config lacks %q:\n%s", want, agent)
		}
	}
	reg := registries("moostack.local", netip.MustParseAddrPort("10.0.0.2:5000"))
	if !strings.Contains(reg, "moostack.local:") || !strings.Contains(reg, "http://10.0.0.2:5000") {
		t.Errorf("registries:\n%s", reg)
	}
}

func TestKubeletConfigReservesMemoryForK3s(t *testing.T) {
	for _, c := range []struct {
		mib          int
		controlPlane bool
		want         []string
	}{
		{1974, true, []string{"memory: 200Mi", "memory: 650Mi", "memory.available: 100Mi", "kind: KubeletConfiguration"}},
		{1974, false, []string{"memory: 250Mi", "memory.available: 100Mi"}},
		{16000, false, []string{"memory.available: 800Mi"}},
	} {
		got := kubeletConfig(c.mib, roleOf(c.controlPlane))
		for _, w := range c.want {
			if !strings.Contains(got, w) {
				t.Errorf("KubeletConfig(%d, %v) lacks %q:\n%s", c.mib, c.controlPlane, w, got)
			}
		}
	}
}

func TestCheckWantsMemoryForTheRole(t *testing.T) {
	facts := func(mib int) facts {
		f, err := parseFacts(fmt.Sprintf("arch=x86_64\nmem_kib=%d\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\n", mib*1024))
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	for _, role := range []role{controlPlaneRole, agentRole} {
		least := minMemoryMiB(role)
		if err := facts(least).check(role); err != nil {
			t.Errorf("%s of %d MiB refused: %v", role, least, err)
		}
		if err := facts(least - 1).check(role); err == nil || !strings.Contains(err.Error(), "MiB of memory") {
			t.Errorf("%s of %d MiB: %v", role, least-1, err)
		}
	}
	if minMemoryMiB(controlPlaneRole) != 200+650+100+256 || minMemoryMiB(agentRole) != 200+250+100+256 {
		t.Errorf("MinMemoryMiB = %d, %d", minMemoryMiB(controlPlaneRole), minMemoryMiB(agentRole))
	}
}

func TestPrivateAddressOverrideMustBePrivateIPv4(t *testing.T) {
	f := factsOf(t, []string{"eth0 203.0.113.10/32", "ens10 10.0.0.2/32"}, "eth0")
	if _, err := f.privateAddress(netip.MustParseAddr("203.0.113.10")); err == nil {
		t.Error("a public address was taken")
	}
	if _, err := f.privateAddress(netip.MustParseAddr("fd00::1")); err == nil {
		t.Error("an IPv6 address was taken")
	}
	if a, err := f.privateAddress(netip.MustParseAddr("10.0.0.2")); err != nil || a.iface != "ens10" {
		t.Errorf("a private override: %v, %v", a, err)
	}
}

func TestServerConfigBindsThePrivateAddressAndSetsAnAgentToken(t *testing.T) {
	got := serverConfig(model.Node{Name: "node1", PrivateAddress: netip.MustParseAddr("10.0.0.2"), Interface: "ens10"}, "agenttoken")
	for _, want := range []string{"bind-address: 10.0.0.2", "advertise-address: 10.0.0.2", "node-ip: 10.0.0.2", "agent-token: agenttoken"} {
		if !strings.Contains(got, want) {
			t.Errorf("lacks %q:\n%s", want, got)
		}
	}
}

func TestInstallScriptNeedsAChecksumOfTheVersionsScript(t *testing.T) {
	if _, err := installScript("v0.0.0+k3s1", agentRole); err == nil {
		t.Error("a version without a checksum was taken")
	}
	got, err := installScript("v1.37.2+k3s1", agentRole)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"sha256sum -c", "INSTALL_K3S_EXEC=agent", "--max-time", "mktemp"} {
		if !strings.Contains(got, want) {
			t.Errorf("lacks %q:\n%s", want, got)
		}
	}
}

func TestNodeScriptsParse(t *testing.T) {
	install, _ := installScript("v1.37.2+k3s1", controlPlaneRole)
	scripts := map[string]string{
		"facts":          factsScript,
		"install":        install,
		"uninstall cp":   uninstallScript(controlPlaneRole),
		"uninstall node": uninstallScript(agentRole),
		"downloaded":     downloadedScript,
		"reach":          reachScript(netip.MustParseAddr("10.0.0.2")),
	}
	for _, sh := range []string{"sh", "bash", "dash"} {
		if _, err := exec.LookPath(sh); err != nil {
			continue
		}
		for name, script := range scripts {
			if out, err := exec.Command(sh, "-n", "-c", script).CombinedOutput(); err != nil {
				t.Errorf("%s: %s doesn't parse: %v\n%s", sh, name, err, out)
			}
		}
	}
}

func TestUninstallRemovesOnlyWhatMooctlWrote(t *testing.T) {
	agent := uninstallScript(agentRole)
	for _, never := range []string{"/var/lib/moostack", "/etc/moostack", "rm -rf"} {
		if strings.Contains(agent, never) {
			t.Errorf("a node's uninstall touches %s:\n%s", never, agent)
		}
	}
	cp := uninstallScript(controlPlaneRole)
	for _, want := range []string{configFile, kubeletDropIn, "rmdir"} {
		if !strings.Contains(cp, want) {
			t.Errorf("the control plane's uninstall lacks %s:\n%s", want, cp)
		}
	}
}

func TestTheFilesOfANodeNameTheRegistryAndUninstallRemovesEachOfThem(t *testing.T) {
	for _, r := range []role{controlPlaneRole, agentRole} {
		n := model.Node{Name: "node2", ControlPlane: r.controlPlane, PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10", MemoryMiB: 4096, GOMEMLIMIT: "409MiB"}
		files := map[string]string{}
		uninstall := uninstallScript(r)
		for _, f := range nodeFiles(n, Cluster{Registry: "registry.test", RegistryPort: 5001}, netip.MustParseAddr("10.0.0.2"), "t") {
			files[f.path] = f.content
			if !strings.Contains(uninstall, " "+f.path) {
				t.Errorf("%s: the uninstall leaves %s:\n%s", r, f.path, uninstall)
			}
		}
		if got, want := files[registriesFile], "mirrors:\n    registry.test:\n        endpoint:\n            - http://10.0.0.2:5001\n"; got != want {
			t.Errorf("%s: registries:\n%s\nwant:\n%s", r, got, want)
		}
	}
}

func TestTheWaitsForAnInstallFindARunningOne(t *testing.T) {
	install, err := installScript("v1.37.2+k3s1", agentRole)
	if err != nil {
		t.Fatal(err)
	}
	for name, script := range map[string]string{"uninstall": uninstallScript(agentRole), "downloaded": downloadedScript} {
		m := regexp.MustCompile(`pgrep -f '([^']+)'`).FindStringSubmatch(script)
		if m == nil {
			t.Fatalf("%s: no pgrep in\n%s", name, script)
		}
		if !regexp.MustCompile(m[1]).MatchString(install) {
			t.Errorf("%s: its pgrep pattern %q doesn't match the install's command line:\n%s", name, m[1], install)
		}
		if regexp.MustCompile(m[1]).MatchString(script) {
			t.Errorf("%s: its own command line matches its pgrep pattern %q", name, m[1])
		}
	}
}

func TestUninstallDoesntWaitForItself(t *testing.T) {
	for _, role := range []role{controlPlaneRole, agentRole} {
		script := uninstallScript(role)
		m := regexp.MustCompile(`pgrep -f '([^']+)'`).FindStringSubmatch(script)
		if m == nil {
			t.Fatalf("%s: no pgrep in\n%s", role, script)
		}
		if regexp.MustCompile(m[1]).MatchString(script) {
			t.Errorf("%s: the script's own command line matches its pgrep pattern %q", role, m[1])
		}
		if !strings.Contains(script, "exit $s") {
			t.Errorf("%s: the script doesn't exit with the uninstall's status", role)
		}
	}
}

func TestPublicAddress(t *testing.T) {
	for _, c := range []struct {
		name   string
		addrs  []string
		routes []string
		want   string
	}{
		{"Hetzner", []string{"lo 127.0.0.1/8", "eth0 192.0.2.10/32", "enp7s0 10.0.0.2/32"}, []string{"eth0"}, "192.0.2.10"},
		{"DigitalOcean anchor and VPC", []string{"eth0 192.0.2.10/20", "eth0 10.19.0.5/16", "eth1 10.116.0.2/20"}, []string{"eth0"}, "192.0.2.10"},
		{"a floating address after the first", []string{"eth0 192.0.2.10/32", "eth0 198.51.100.7/32", "ens10 10.0.0.2/32"}, []string{"eth0"}, "192.0.2.10"},
		{"the one on the default route", []string{"eth1 198.51.100.7/32", "eth0 192.0.2.10/32", "ens10 10.0.0.2/32"}, []string{"eth0"}, "192.0.2.10"},
		{"no default route", []string{"eth1 198.51.100.7/32", "ens10 10.0.0.2/32"}, nil, "198.51.100.7"},
		{"behind NAT", []string{"eth0 192.168.1.20/24"}, []string{"eth0"}, "invalid IP"},
		{"a management link", []string{"ens3 10.0.2.15/24", "ens4 10.0.1.2/24"}, []string{"ens3"}, "invalid IP"},
		{"Tailscale", []string{"eth0 10.0.0.2/24", "tailscale0 100.101.102.103/32"}, []string{"eth0"}, "invalid IP"},
		{"link-local", []string{"eth0 169.254.169.254/32", "ens10 10.0.0.2/32"}, []string{"eth0"}, "invalid IP"},
		{"a container bridge with a public range", []string{"docker0 198.51.100.1/24", "ens10 10.0.0.2/32"}, nil, "invalid IP"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := factsOf(t, c.addrs, c.routes...).publicAddress().String(); got != c.want {
				t.Errorf("PublicAddress = %s, want %s", got, c.want)
			}
		})
	}
}

func TestConfigsNameThePublicAddressAsExternal(t *testing.T) {
	n := model.Node{Name: "node1", PrivateAddress: netip.MustParseAddr("10.0.0.2"), Interface: "ens10"}
	for name, config := range map[string]string{"server": serverConfig(n, "t"), "agent": agentConfig(n, n.PrivateAddress, "t")} {
		if strings.Contains(config, "node-external-ip") {
			t.Errorf("%s config of a node without a public address:\n%s", name, config)
		}
	}
	n.PublicAddress = netip.MustParseAddr("192.0.2.10")
	for name, config := range map[string]string{"server": serverConfig(n, "t"), "agent": agentConfig(n, n.PrivateAddress, "t")} {
		if !strings.Contains(config, "node-external-ip: 192.0.2.10\n") || !strings.Contains(config, "node-ip: 10.0.0.2\n") {
			t.Errorf("%s config:\n%s", name, config)
		}
	}
}

func TestTraefikConfigKeepsTheVisitorsAddress(t *testing.T) {
	var config struct {
		Spec struct {
			ValuesContent string `yaml:"valuesContent"`
		}
	}
	if err := yaml.Unmarshal([]byte(traefikConfig), &config); err != nil {
		t.Fatal(err)
	}
	var values struct {
		Deployment struct{ Kind string }
		Service    struct {
			Spec struct {
				Type                  string
				ExternalTrafficPolicy string `yaml:"externalTrafficPolicy"`
			}
		}
		Ports map[string]struct {
			HostPort int `yaml:"hostPort"`
		}
	}
	if err := yaml.Unmarshal([]byte(config.Spec.ValuesContent), &values); err != nil {
		t.Fatal(err)
	}
	if values.Deployment.Kind != "DaemonSet" || values.Service.Spec.ExternalTrafficPolicy != "Local" {
		t.Errorf("Traefik's kind %q and traffic policy %q", values.Deployment.Kind, values.Service.Spec.ExternalTrafficPolicy)
	}
	if values.Service.Spec.Type != "" {
		t.Errorf("the Service's type is %q, not the chart's LoadBalancer", values.Service.Spec.Type)
	}
	for name, p := range values.Ports {
		if p.HostPort != 0 {
			t.Errorf("port %s has a host port, which stops a new Traefik from starting beside the old", name)
		}
	}
}

func TestTheCauseOfACutOffCallKeepsWhatItsUndoLeft(t *testing.T) {
	cause := errors.New("the operation lock is lost")
	ended, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	failed := errors.Join(
		fmt.Errorf("draining node2: %w", context.Canceled),
		leftError{errors.New("uncordoning node2: connection reset")},
	)
	got := causeAndLeft(ended, fmt.Errorf("removing: %w", failed))
	if want := "the operation lock is lost\nuncordoning node2: connection reset"; got == nil || got.Error() != want || !errors.Is(got, cause) {
		t.Errorf("the error of a cut-off call whose undo failed is %q, want %q", got, want)
	}
	if got := causeAndLeft(ended, errors.New("draining node2: stopped")); got == nil || got.Error() != cause.Error() {
		t.Errorf("the error of a cut-off call is %q, want the cause alone", got)
	}
	if got := causeAndLeft(context.Background(), failed); got != failed {
		t.Errorf("the error of a call that wasn't cut off is %q, want it as it was", got)
	}
	late, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	if got := causeAndLeft(late, failed); got != failed {
		t.Errorf("the error of a call whose time was up is %q, want it as it was", got)
	}
	if got := causeAndLeft(ended, nil); got != nil {
		t.Errorf("no error became %q", got)
	}
}

func TestACommandIsAskedOnlyOfAnAPIWhoseCertificateTheClustersCASigned(t *testing.T) {
	requests := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.NotFound(w, r)
	}))
	defer server.Close()
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(cryptorand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	other := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	a, err := newAPI(&rest.Config{Host: server.URL, BearerToken: "secret", TLSClientConfig: rest.TLSClientConfig{CAData: other}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err = a.exec(ctx, "kube-system", "pod", "console", "console", "token")
	if err == nil || !strings.Contains(err.Error(), "x509") || requests != 0 {
		t.Errorf("exec at an API of another certificate: %v, after %d requests reached it; want its certificate refused and none", err, requests)
	}
}

func FuzzTheFactsOfAHost(f *testing.F) {
	f.Add("arch=x86_64\nmem_kib=2013456\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\naddr=eth0 192.0.2.10/32\naddr=ens10@if3 10.0.0.2/32\nroute=eth0\n")
	f.Add("arch=aarch64\nmem_kib=-5\nk3s=yes\naddr=eth0:1 10.19.0.5/16\n")
	f.Add("mem_kib=lots\n")
	f.Add("addr=\naddr=x\naddr=x y\naddr= 10.0.0.1/8\n=\n\x00=\x00\n")
	f.Fuzz(func(t *testing.T, out string) {
		facts, err := parseFacts(out)
		if err != nil {
			if !strings.HasPrefix(err.Error(), "the host's memory: ") && !strings.Contains(err.Error(), "token too long") {
				t.Errorf("the facts %q are refused with %v, which names nothing of them", out, err)
			}
			return
		}
		for _, a := range facts.addresses {
			if !a.addr.IsValid() || strings.ContainsAny(a.iface, ":@") {
				t.Errorf("the facts %q have the address %v on %q", out, a.addr, a.iface)
			}
		}
		// What is judged and chosen of any facts is a refusal or an
		// address of the host's.
		facts.check(agentRole)
		if private, err := facts.privateAddress(netip.Addr{}); err == nil && (!private.addr.Is4() || !private.addr.IsPrivate()) {
			t.Errorf("the private address of the facts %q is %v", out, private.addr)
		}
		if public := facts.publicAddress(); public.IsValid() && (public.IsPrivate() || !public.Is4()) {
			t.Errorf("the public address of the facts %q is %v", out, public)
		}
	})
}

func FuzzTheMessagesOfACommand(f *testing.F) {
	f.Add([]byte("\x01out"), []byte("\x02err"), []byte("\x03"+`{"status":"Success"}`))
	f.Add([]byte{}, []byte{3}, []byte("\x03{"))
	f.Add([]byte{9, 9}, []byte("\x03null"), []byte("\x03[]"))
	f.Fuzz(func(t *testing.T, first, second, third []byte) {
		var out execOutput
		var printed, said int
		for _, message := range [][]byte{first, second, third} {
			if err := out.take(message); err != nil {
				if len(message) == 0 || message[0] != execStatus {
					t.Errorf("the message %q is refused with %v, and is no status", message, err)
				}
				continue
			}
			if len(message) > 0 && message[0] == execStdout {
				printed += len(message) - 1
			}
			if len(message) > 0 && message[0] == execStderr {
				said += len(message) - 1
			}
		}
		if out.stdout.Len() != printed || out.stderr.Len() != said {
			t.Errorf("of %d and %d bytes sent as output and error output, %d and %d were taken", printed, said, out.stdout.Len(), out.stderr.Len())
		}
	})
}

// A shell reads back each word as it was quoted, and runs nothing of
// it.
func TestAShellReadsBackWhatIsQuoted(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("no sh")
	}
	alphabet := []rune("ab '\"\\$`;|&<>(){}[]*?~#!\n\t=%-é\u2028")
	rng := rand.New(rand.NewPCG(1, 2))
	words := []string{"", "'", "''", `'\''`, "$(touch pwned)", "`touch pwned`", "a\nb", "-n", "v1.37.2+k3s1"}
	for range 300 {
		word := make([]rune, rng.IntN(12))
		for i := range word {
			word[i] = alphabet[rng.IntN(len(alphabet))]
		}
		words = append(words, string(word))
	}
	dir := t.TempDir()
	for _, word := range words {
		cmd := exec.Command("sh", "-c", "printf %s "+shellQuote(word))
		cmd.Dir = dir
		out, err := cmd.Output()
		if err != nil || string(out) != word {
			t.Errorf("a shell reads %q, quoted as %s, back as %q (%v)", word, shellQuote(word), out, err)
		}
	}
	if left, _ := os.ReadDir(dir); len(left) != 0 {
		t.Errorf("reading the words back made %d files", len(left))
	}
}

// Whatever the email holds, the issuer says it as given, and nothing
// beside. A node's name and its interface's are refused, or the files
// written of them say them as given.
func TestTheFilesOfANodeHoldTheEmailAndTheNamesAsGiven(t *testing.T) {
	alphabet := []rune("ab01.-_@: '\"\\#{}[],&*!|>%\n\t\u0085\u2028\ufeffé")
	rng := rand.New(rand.NewPCG(3, 4))
	values := []string{"ops@example.com", "a\"\nspec:\n  selfSigned: {}", "x: y", "- a", "'", "\\", "#", "yes", "null", "~", "0755", "2024-01-02", "node1", "a.b-c", "\nnode1", "node1\t"}
	for range 2000 {
		v := make([]rune, 1+rng.IntN(8))
		for i := range v {
			v[i] = alphabet[rng.IntN(len(alphabet))]
		}
		values = append(values, string(v))
	}
	taken := 0
	for _, v := range values {
		var issuer struct {
			Kind string
			Spec struct {
				SelfSigned map[string]any `yaml:"selfSigned"`
				Acme       struct{ Email string }
			}
		}
		if err := yaml.Unmarshal([]byte(issuerManifest(v)), &issuer); err != nil || issuer.Spec.Acme.Email != v || issuer.Spec.SelfSigned != nil || issuer.Kind != "ClusterIssuer" {
			t.Errorf("the issuer of the email %q reads back as %q, self-signed %v (%v)", v, issuer.Spec.Acme.Email, issuer.Spec.SelfSigned, err)
		}
		name, iface := v, v
		if checkNodeNames(name, "") != nil {
			name = "node1"
		}
		if checkNodeNames("node1", iface) != nil {
			iface = "ens10"
		}
		if name == v || iface == v {
			taken++
		}
		n := model.Node{Name: name, Interface: iface, PrivateAddress: netip.MustParseAddr("10.0.0.3")}
		for role, text := range map[string]string{"server": serverConfig(n, "t"), "agent": agentConfig(n, netip.MustParseAddr("10.0.0.2"), "t")} {
			var config map[string]string
			if err := yaml.Unmarshal([]byte(text), &config); err != nil || config["node-name"] != name || config["flannel-iface"] != iface {
				t.Errorf("the %s config of the name %q on %q reads back as %q (%v)", role, name, iface, config, err)
			}
			if want := map[string]int{"server": 7, "agent": 5}[role]; len(config) != want {
				t.Errorf("the %s config of the name %q on %q has %d keys, want %d:\n%s", role, name, iface, len(config), want, text)
			}
		}
	}
	if taken < 100 {
		t.Errorf("only %d of %d values were taken as a name or an interface", taken, len(values))
	}
}

func TestTheBoundsOfANodesOperations(t *testing.T) {
	for name, bound := range map[string][2]time.Duration{
		"a drain":                       {drainTimeout, 3 * time.Minute},
		"the wait for a node":           {readyTimeout, 10 * time.Minute},
		"an undo":                       {undoTimeout, 5 * time.Minute},
		"an uncordon":                   {uncordonTimeout, 30 * time.Second},
		"the wait for a deleted node":   {goneTimeout, time.Minute},
		"the console's objects applied": {consoleAppliedTimeout, 2 * time.Minute},
		"the console getting ready":     {consoleReadyTimeout, 5 * time.Minute},
		"a request to the API":          {requestTimeout, time.Minute},
	} {
		if bound[0] != bound[1] {
			t.Errorf("%s has %s, want %s", name, bound[0], bound[1])
		}
	}
}
