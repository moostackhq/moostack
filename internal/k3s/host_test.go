package k3s_test

import (
	"flag"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/k3s"
)

// facts are FactsScript's output on a host with addrs and a default
// route on each of routes.
func facts(t *testing.T, addrs []string, routes ...string) k3s.Facts {
	t.Helper()
	out := "arch=x86_64\nmem_kib=2013456\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\n"
	for _, a := range addrs {
		out += "addr=" + a + "\n"
	}
	for _, r := range routes {
		out += "route=" + r + "\n"
	}
	f, err := k3s.ParseFacts(out)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestParseFacts(t *testing.T) {
	f := facts(t, []string{"lo 127.0.0.1/8", "eth0 192.0.2.10/32", "eth0:1 10.19.0.5/16", "ens10@if3 10.0.0.2/32"}, "eth0")
	if f.Architecture != "amd64" || f.MemoryMiB != 1966 || !f.Systemd || f.K3s {
		t.Errorf("facts = %+v", f)
	}
	want := []k3s.Address{
		{Interface: "lo", Addr: netip.MustParseAddr("127.0.0.1")},
		{Interface: "eth0", Addr: netip.MustParseAddr("192.0.2.10")},
		{Interface: "eth0", Addr: netip.MustParseAddr("10.19.0.5")},
		{Interface: "ens10", Addr: netip.MustParseAddr("10.0.0.2")},
	}
	if len(f.Addresses) != len(want) {
		t.Fatalf("addresses = %v, want %v", f.Addresses, want)
	}
	for i := range want {
		if f.Addresses[i] != want[i] {
			t.Errorf("address %d = %v, want %v", i, f.Addresses[i], want[i])
		}
	}
	if err := f.Check(k3s.ControlPlane); err != nil {
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
			f, err := k3s.ParseFacts(out)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Check(k3s.Agent); err == nil {
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
			a, err := facts(t, c.addrs, c.routes...).PrivateAddress(netip.Addr{})
			if err != nil {
				t.Fatal(err)
			}
			if got := a.Addr.String() + " " + a.Interface; got != c.want {
				t.Errorf("got %s, want %s", got, c.want)
			}
		})
	}
}

func TestPrivateAddressRefusesWhenItCantTell(t *testing.T) {
	none := facts(t, []string{"lo 127.0.0.1/8", "eth0 192.0.2.10/32", "docker0 172.17.0.1/16"}, "eth0")
	if _, err := none.PrivateAddress(netip.Addr{}); err == nil || !strings.Contains(err.Error(), "--private-address") {
		t.Errorf("no private address: err = %v", err)
	}
	two := facts(t, []string{"eth0 192.0.2.10/32", "ens10 10.0.0.2/32", "ens11 10.1.0.2/32"}, "eth0")
	_, err := two.PrivateAddress(netip.Addr{})
	if err == nil || !strings.Contains(err.Error(), "10.0.0.2 on ens10") || !strings.Contains(err.Error(), "10.1.0.2 on ens11") {
		t.Errorf("two private networks: err = %v", err)
	}
	a, err := two.PrivateAddress(netip.MustParseAddr("10.1.0.2"))
	if err != nil || a.Interface != "ens11" {
		t.Errorf("override: %v, %v", a, err)
	}
	if _, err := two.PrivateAddress(netip.MustParseAddr("10.9.9.9")); err == nil {
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
		gogc, limit := k3s.Memory(c.mib, k3s.RoleOf(c.controlPlane))
		if gogc != c.wantGOGC || limit != c.wantMemLimit {
			t.Errorf("Memory(%d, %v) = %q, %q, want %q, %q", c.mib, c.controlPlane, gogc, limit, c.wantGOGC, c.wantMemLimit)
		}
	}
}

func TestMemoryDropInLeavesGOGCToGoWithoutOne(t *testing.T) {
	if got := k3s.MemoryDropIn("", "1600MiB"); strings.Contains(got, "GOGC") || !strings.Contains(got, "Environment=GOMEMLIMIT=1600MiB") {
		t.Errorf("drop-in:\n%s", got)
	}
	if got := k3s.MemoryDropIn("25", "491MiB"); !strings.Contains(got, "Environment=GOGC=25\n") {
		t.Errorf("drop-in:\n%s", got)
	}
}

func TestConfigs(t *testing.T) {
	n := k3s.Node{Name: "node2", PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10"}
	agent := k3s.AgentConfig(n, netip.MustParseAddr("10.0.0.2"), "K10::server:abc")
	for _, want := range []string{"server: https://10.0.0.2:6443", "token: K10::server:abc", "node-ip: 10.0.0.3", "flannel-iface: ens10", "node-name: node2"} {
		if !strings.Contains(agent, want) {
			t.Errorf("agent config lacks %q:\n%s", want, agent)
		}
	}
	reg := k3s.Registries("moostack.local", netip.MustParseAddrPort("10.0.0.2:5000"))
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
		got := k3s.KubeletConfig(c.mib, k3s.RoleOf(c.controlPlane))
		for _, w := range c.want {
			if !strings.Contains(got, w) {
				t.Errorf("KubeletConfig(%d, %v) lacks %q:\n%s", c.mib, c.controlPlane, w, got)
			}
		}
	}
}

func TestCheckWantsMemoryForTheRole(t *testing.T) {
	facts := func(mib int) k3s.Facts {
		f, err := k3s.ParseFacts(fmt.Sprintf("arch=x86_64\nmem_kib=%d\nsystemd=yes\nhas=curl\nhas=flock\nhas=ip\n", mib*1024))
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	for _, role := range []k3s.Role{k3s.ControlPlane, k3s.Agent} {
		least := k3s.MinMemoryMiB(role)
		if err := facts(least).Check(role); err != nil {
			t.Errorf("%s of %d MiB refused: %v", role, least, err)
		}
		if err := facts(least - 1).Check(role); err == nil || !strings.Contains(err.Error(), "MiB of memory") {
			t.Errorf("%s of %d MiB: %v", role, least-1, err)
		}
	}
	if k3s.MinMemoryMiB(k3s.ControlPlane) != 200+650+100+256 || k3s.MinMemoryMiB(k3s.Agent) != 200+250+100+256 {
		t.Errorf("MinMemoryMiB = %d, %d", k3s.MinMemoryMiB(k3s.ControlPlane), k3s.MinMemoryMiB(k3s.Agent))
	}
}

func TestPrivateAddressOverrideMustBePrivateIPv4(t *testing.T) {
	f := facts(t, []string{"eth0 203.0.113.10/32", "ens10 10.0.0.2/32"}, "eth0")
	if _, err := f.PrivateAddress(netip.MustParseAddr("203.0.113.10")); err == nil {
		t.Error("a public address was taken")
	}
	if _, err := f.PrivateAddress(netip.MustParseAddr("fd00::1")); err == nil {
		t.Error("an IPv6 address was taken")
	}
	if a, err := f.PrivateAddress(netip.MustParseAddr("10.0.0.2")); err != nil || a.Interface != "ens10" {
		t.Errorf("a private override: %v, %v", a, err)
	}
}

func TestServerConfigBindsThePrivateAddressAndSetsAnAgentToken(t *testing.T) {
	got := k3s.ServerConfig(k3s.Node{Name: "node1", PrivateAddress: netip.MustParseAddr("10.0.0.2"), Interface: "ens10"}, "agenttoken")
	for _, want := range []string{"bind-address: 10.0.0.2", "advertise-address: 10.0.0.2", "node-ip: 10.0.0.2", "agent-token: agenttoken"} {
		if !strings.Contains(got, want) {
			t.Errorf("lacks %q:\n%s", want, got)
		}
	}
}

func TestInstallScriptNeedsAChecksumOfTheVersionsScript(t *testing.T) {
	if _, err := k3s.InstallScript("v0.0.0+k3s1", k3s.Agent); err == nil {
		t.Error("a version without a checksum was taken")
	}
	got, err := k3s.InstallScript("v1.37.2+k3s1", k3s.Agent)
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
	install, _ := k3s.InstallScript("v1.37.2+k3s1", k3s.ControlPlane)
	scripts := map[string]string{
		"facts":          k3s.FactsScript,
		"install":        install,
		"wait":           k3s.WaitReadyScript("node1", netip.MustParseAddr("10.0.0.2")),
		"uninstall cp":   k3s.UninstallScript(k3s.ControlPlane),
		"uninstall node": k3s.UninstallScript(k3s.Agent),
		"downloaded":     k3s.DownloadedScript,
		"reach":          k3s.ReachScript(netip.MustParseAddr("10.0.0.2")),
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
	agent := k3s.UninstallScript(k3s.Agent)
	for _, never := range []string{"/var/lib/moostack", "/etc/moostack", "rm -rf"} {
		if strings.Contains(agent, never) {
			t.Errorf("a node's uninstall touches %s:\n%s", never, agent)
		}
	}
	cp := k3s.UninstallScript(k3s.ControlPlane)
	for _, want := range []string{k3s.ConfigFile, k3s.KubeletDropIn, "rmdir"} {
		if !strings.Contains(cp, want) {
			t.Errorf("the control plane's uninstall lacks %s:\n%s", want, cp)
		}
	}
}

func TestWrittenFilesNameTheRegistryAndUninstallRemovesEachOfThem(t *testing.T) {
	for _, role := range []k3s.Role{k3s.ControlPlane, k3s.Agent} {
		w := k3s.Written{
			Node:         k3s.Node{Name: "node2", PrivateAddress: netip.MustParseAddr("10.0.0.3"), Interface: "ens10"},
			Role:         role,
			MemoryMiB:    4096,
			GOMEMLIMIT:   "409MiB",
			ControlPlane: netip.MustParseAddr("10.0.0.2"),
			Token:        "t",
			Registry:     "registry.test",
			RegistryPort: 5001,
		}
		files := map[string]string{}
		uninstall := k3s.UninstallScript(role)
		for _, f := range w.Files() {
			files[f.Path] = f.Content
			if !strings.Contains(uninstall, " "+f.Path) {
				t.Errorf("%s: the uninstall leaves %s:\n%s", role, f.Path, uninstall)
			}
		}
		if got, want := files[k3s.RegistriesFile], "mirrors:\n    registry.test:\n        endpoint:\n            - http://10.0.0.2:5001\n"; got != want {
			t.Errorf("%s: registries:\n%s\nwant:\n%s", role, got, want)
		}
	}
}

func TestTheWaitsForAnInstallFindARunningOne(t *testing.T) {
	install, err := k3s.InstallScript("v1.37.2+k3s1", k3s.Agent)
	if err != nil {
		t.Fatal(err)
	}
	for name, script := range map[string]string{"uninstall": k3s.UninstallScript(k3s.Agent), "downloaded": k3s.DownloadedScript} {
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
	for _, role := range []k3s.Role{k3s.ControlPlane, k3s.Agent} {
		script := k3s.UninstallScript(role)
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
			if got := facts(t, c.addrs, c.routes...).PublicAddress().String(); got != c.want {
				t.Errorf("PublicAddress = %s, want %s", got, c.want)
			}
		})
	}
}

func TestConfigsNameThePublicAddressAsExternal(t *testing.T) {
	n := k3s.Node{Name: "node1", PrivateAddress: netip.MustParseAddr("10.0.0.2"), Interface: "ens10"}
	for name, config := range map[string]string{"server": k3s.ServerConfig(n, "t"), "agent": k3s.AgentConfig(n, n.PrivateAddress, "t")} {
		if strings.Contains(config, "node-external-ip") {
			t.Errorf("%s config of a node without a public address:\n%s", name, config)
		}
	}
	n.PublicAddress = netip.MustParseAddr("192.0.2.10")
	for name, config := range map[string]string{"server": k3s.ServerConfig(n, "t"), "agent": k3s.AgentConfig(n, n.PrivateAddress, "t")} {
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
	if err := yaml.Unmarshal([]byte(k3s.TraefikConfig), &config); err != nil {
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

var update = flag.Bool("update", false, "rewrite the golden files")

func TestTheConsolesObjects(t *testing.T) {
	got := k3s.Console("moostack.local/kube-system/console@sha256:0a1b2c", []netip.Addr{netip.MustParseAddr("203.0.113.7"), netip.MustParseAddr("10.0.0.2")})
	const golden = "testdata/console.golden.yaml"
	if *update {
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Errorf("the console's objects aren't %s; run with -update and review the change:\n%s", golden, got)
	}
	kinds := []string{}
	documents := yaml.NewDecoder(strings.NewReader(got))
	for {
		var object struct {
			Kind     string
			Metadata struct{ Namespace string }
		}
		if err := documents.Decode(&object); err != nil {
			if err != io.EOF {
				t.Fatalf("the objects aren't YAML: %v", err)
			}
			break
		}
		if object.Metadata.Namespace != "kube-system" && !strings.HasPrefix(object.Kind, "ClusterRole") {
			t.Errorf("the %s is in the namespace %q", object.Kind, object.Metadata.Namespace)
		}
		kinds = append(kinds, object.Kind)
	}
	if got := strings.Join(kinds, " "); got != "ServiceAccount ClusterRole ClusterRoleBinding Deployment Service IngressRoute" {
		t.Errorf("the objects are %s", got)
	}
	if one := k3s.Console("moostack.local/kube-system/console@sha256:0a1b2c", []netip.Addr{netip.MustParseAddr("10.0.0.2")}); !strings.Contains(one, "match: (Host(`10.0.0.2`)) && (Path(`/console`) || PathPrefix(`/console/`))") {
		t.Errorf("the route of a node with one address:\n%s", one)
	}
}

func TestTheConsolesCommandsQuoteWhatAShellWouldExpand(t *testing.T) {
	for name, script := range map[string]string{
		"ConsoleImageScript": k3s.ConsoleImageScript, "ConsoleReadyScript": k3s.ConsoleReadyScript, "ConsoleTokenScript": k3s.ConsoleTokenScript,
	} {
		unquoted := regexp.MustCompile(`'[^']*'`).ReplaceAllString(script, "")
		if strings.ContainsAny(unquoted, "[]{}*?$`\\\"<>|&;()~#") {
			t.Errorf("%s has, outside single quotes, what a shell would expand: %s", name, unquoted)
		}
	}
}

func TestTheConsolesURLIsItsLoginPageAtTheAddress(t *testing.T) {
	for address, want := range map[string]string{
		"203.0.113.7": "https://203.0.113.7/console/login",
		"2001:db8::7": "https://[2001:db8::7]/console/login",
	} {
		if got := k3s.ConsoleURL(netip.MustParseAddr(address)); got != want {
			t.Errorf("the console's URL at %s is %q, want %q", address, got, want)
		}
	}
}
