//go:build linux

package agent

import (
	"slices"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/containerd/containerd/v2/pkg/identifiers"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
)

// valid is a container that Run accepts, with every property set.
func valid() *nodev1.Container {
	user := "1000:1000"
	return &nodev1.Container{Name: "app-web", Image: "docker.io/library/nginx:1.27", User: &user, ReadOnlyRoot: true,
		Capabilities: &nodev1.Capabilities{Names: []string{"CAP_NET_BIND_SERVICE", "CAP_CHOWN"}}, Api: "moostack.dns.v1.DNS"}
}

func TestRunAcceptsEachKindOfValidContainer(t *testing.T) {
	for name, edit := range map[string]func(c *nodev1.Container){
		"every property set":        func(*nodev1.Container) {},
		"a name and an image only":  func(c *nodev1.Container) { *c = nodev1.Container{Name: "app-web", Image: c.Image} },
		"a one-letter name":         func(c *nodev1.Container) { c.Name = "a" },
		"a name of 63":              func(c *nodev1.Container) { c.Name = "a" + strings.Repeat("b-", 30) + "c9" },
		"a uid":                     func(c *nodev1.Container) { u := "0"; c.User = &u },
		"no capabilities":           func(c *nodev1.Container) { c.Capabilities = &nodev1.Capabilities{} },
		"an API of one part":        func(c *nodev1.Container) { c.Api = "Echo" },
		"an image by digest":        func(c *nodev1.Container) { c.Image = "example.test/app@sha256:" + strings.Repeat("a", 64) },
		"an image of 1024":          func(c *nodev1.Container) { c.Image = strings.Repeat("a", 1024) },
		"an API of 255":             func(c *nodev1.Container) { c.Api = "a" + strings.Repeat(".b", 127) },
		"the highest uid and gid":   func(c *nodev1.Container) { u := "2147483647:2147483647"; c.User = &u },
		"a uid of ten digits":       func(c *nodev1.Container) { u := "0000000001:0000000001"; c.User = &u },
		"every capability":          func(c *nodev1.Container) { c.Capabilities.Names = slices.Clone(nodev1.LinuxCapabilities) },
		"capabilities out of order": func(c *nodev1.Container) { c.Capabilities.Names = []string{"CAP_KILL", "CAP_CHOWN"} },
	} {
		c := valid()
		edit(c)
		if err := checkContainer(containerOf(c)); err != nil {
			t.Errorf("%s: %v, want it accepted", name, err)
		}
	}
}

func TestRunRefusesEachKindOfInvalidContainer(t *testing.T) {
	for name, edit := range map[string]func(c *nodev1.Container){
		"no name":                    func(c *nodev1.Container) { c.Name = "" },
		"a name of 64":               func(c *nodev1.Container) { c.Name = "a" + strings.Repeat("b", 63) },
		"a name with a capital":      func(c *nodev1.Container) { c.Name = "App" },
		"a name starting with digit": func(c *nodev1.Container) { c.Name = "1app" },
		"a name starting with -":     func(c *nodev1.Container) { c.Name = "-app" },
		"a name with a dot":          func(c *nodev1.Container) { c.Name = "app.web" },
		"a name with a slash":        func(c *nodev1.Container) { c.Name = "app/../web" },
		"a name ending in a hyphen":  func(c *nodev1.Container) { c.Name = "app-" },
		"a name with two hyphens":    func(c *nodev1.Container) { c.Name = "app--web" },
		"no image":                   func(c *nodev1.Container) { c.Image = "" },
		"an image with a space":      func(c *nodev1.Container) { c.Image = "nginx 1.27" },
		"an image with a line break": func(c *nodev1.Container) { c.Image = "nginx\n" },
		"an image of invalid UTF-8":  func(c *nodev1.Container) { c.Image = "nginx\xff" },
		"an empty user":              func(c *nodev1.Container) { u := ""; c.User = &u },
		"a user of a colon":          func(c *nodev1.Container) { u := ":"; c.User = &u },
		"a user without a group":     func(c *nodev1.Container) { u := "1000:"; c.User = &u },
		"a group without a user":     func(c *nodev1.Container) { u := ":1000"; c.User = &u },
		"a user of three parts":      func(c *nodev1.Container) { u := "1:2:3"; c.User = &u },
		"a user with a space":        func(c *nodev1.Container) { u := "a b"; c.User = &u },
		"a user starting with -":     func(c *nodev1.Container) { u := "-1"; c.User = &u },
		"a capability without CAP_":  func(c *nodev1.Container) { c.Capabilities.Names = []string{"NET_ADMIN"} },
		"a capability in lower case": func(c *nodev1.Container) { c.Capabilities.Names = []string{"cap_net_admin"} },
		"an empty capability":        func(c *nodev1.Container) { c.Capabilities.Names = []string{""} },
		"a capability twice":         func(c *nodev1.Container) { c.Capabilities.Names = []string{"CAP_CHOWN", "CAP_CHOWN"} },
		"an API with a space":        func(c *nodev1.Container) { c.Api = "moostack dns" },
		"an API starting with digit": func(c *nodev1.Container) { c.Api = "1moostack.DNS" },
		"an API with an empty part":  func(c *nodev1.Container) { c.Api = "moostack..DNS" },
		"an API with a slash":        func(c *nodev1.Container) { c.Api = "moostack.dns.v1.DNS/Echo" },
		"an image of 1025":           func(c *nodev1.Container) { c.Image = strings.Repeat("a", 1025) },
		"an API of 256":              func(c *nodev1.Container) { c.Api = "ab" + strings.Repeat(".b", 127) },
		"a uid past runc's":          func(c *nodev1.Container) { u := "2147483648"; c.User = &u },
		"a gid past runc's":          func(c *nodev1.Container) { u := "0:2147483648"; c.User = &u },
		"a uid past 2^32":            func(c *nodev1.Container) { u := "4294967296:0"; c.User = &u },
		"a uid of eleven digits":     func(c *nodev1.Container) { u := "00000000001"; c.User = &u },
		"a uid of 5000 zeros":        func(c *nodev1.Container) { u := strings.Repeat("0", 5000) + "1"; c.User = &u },
		"a user by name":             func(c *nodev1.Container) { u := "nginx"; c.User = &u },
		"a uid and a group by name":  func(c *nodev1.Container) { u := "0:www-data"; c.User = &u },
		"a negative uid":             func(c *nodev1.Container) { u := "-1"; c.User = &u },
		"a capability Linux lacks":   func(c *nodev1.Container) { c.Capabilities.Names = []string{"CAP_XAA"} },
		"a capability misspelt":      func(c *nodev1.Container) { c.Capabilities.Names = []string{"CAP_NET_BIND_SERVIC"} },
		"a capability twice, apart":  func(c *nodev1.Container) { c.Capabilities.Names = []string{"CAP_KILL", "CAP_CHOWN", "CAP_KILL"} },
	} {
		c := valid()
		edit(c)
		if err := checkContainer(containerOf(c)); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v, want it refused as an invalid argument", name, err)
		}
	}
}

// Nothing a request holds makes the check panic, and what it accepts
// holds a name, an image, a user, capabilities and an API of the forms a
// container's are.
func FuzzCheckContainer(f *testing.F) {
	f.Add("app-web", "docker.io/library/nginx:1.27", true, "1000:1000", true, "CAP_CHOWN,CAP_KILL", "moostack.dns.v1.DNS")
	f.Add("a", "x", false, "", false, "", "")
	f.Add("A", "", true, ":", true, "CAP_CHOWN,CAP_CHOWN", "a..b")
	f.Fuzz(func(t *testing.T, name, image string, hasUser bool, user string, hasCaps bool, caps, api string) {
		c := &nodev1.Container{Name: name, Image: image, Api: api}
		if hasUser {
			c.User = &user
		}
		if hasCaps {
			c.Capabilities = &nodev1.Capabilities{}
			if caps != "" {
				c.Capabilities.Names = strings.Split(caps, ",")
			}
		}
		err := checkContainer(containerOf(c))
		if err != nil {
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("refused with %v, want an invalid argument", err)
			}
			return
		}
		// containerd takes it as a container's and a snapshot's name.
		if len(name) > 63 || name[0] < 'a' || name[0] > 'z' || identifiers.Validate(name) != nil {
			t.Errorf("accepted the name %q", name)
		}
		if image == "" || len(image) > 1024 || !utf8.ValidString(image) || strings.ContainsFunc(image, func(r rune) bool { return unicode.IsSpace(r) || !unicode.IsPrint(r) }) {
			t.Errorf("accepted the image %q", image)
		}
		if hasUser && (user == "" || len(user) > 65 || strings.Count(user, ":") > 1 || strings.HasPrefix(user, ":") || strings.HasSuffix(user, ":") || strings.ContainsFunc(user, unicode.IsSpace)) {
			t.Errorf("accepted the user %q", user)
		}
		if hasCaps {
			seen := map[string]bool{}
			for _, cap := range c.Capabilities.Names {
				if !strings.HasPrefix(cap, "CAP_") || len(cap) > len("CAP_CHECKPOINT_RESTORE") || strings.ToUpper(cap) != cap || seen[cap] {
					t.Errorf("accepted the capabilities %q", c.Capabilities.Names)
				}
				seen[cap] = true
			}
		}
		if api != "" && (len(api) > 255 || strings.Contains(api, "..") || strings.ContainsAny(api, " /") || strings.HasPrefix(api, ".") || strings.HasSuffix(api, ".")) {
			t.Errorf("accepted the API %q", api)
		}
	})
}

func TestRemoveRefusesAnInvalidName(t *testing.T) {
	for _, name := range []string{"", "App", "1app", "app.web", "a/../b", "app-", "app--web", "a" + strings.Repeat("b", 63)} {
		if _, err := (&containers{}).Remove(t.Context(), name, nil); status.Code(err) != codes.InvalidArgument {
			t.Errorf("removing %q: %v, want it refused as an invalid argument", name, err)
		}
	}
}
