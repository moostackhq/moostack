//go:build linux

package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// What the list stores reads back as it was, whatever the containers'
// properties and the unused images.
func FuzzTheStoredListReadsBackAsStored(f *testing.F) {
	f.Add("app-web", "nginx", true, "1000:1000", false, true, "CAP_KILL", "", "old")
	f.Add("dns", "moostack.local/dns:1.0.0", true, "65534:65534", true, true, "", "moostack.dns.v1.DNS", "")
	f.Fuzz(func(t *testing.T, name, image string, hasUser bool, user string, readOnly, hasCaps bool, capability, api, unused string) {
		c := &container{Name: name, Image: image, API: api, properties: properties{ReadOnlyRoot: readOnly}}
		if hasUser {
			c.User = &user
		}
		if hasCaps {
			caps := []string{}
			if capability != "" {
				caps = append(caps, capability)
			}
			c.Capabilities = &caps
		}
		var images []unusedImage
		if unused != "" {
			images = []unusedImage{{Image: unused, Updated: time.Date(2026, 10, 25, 9, 0, 0, 1, time.UTC)}}
		}
		s := &containers{file: filepath.Join(t.TempDir(), "containers")}
		if err := s.store([]*container{c}, images); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(s.file)
		if err != nil {
			t.Fatal(err)
		}
		var got listFile
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		// JSON keeps valid UTF-8 alone; the agent takes only that.
		want := listFile{Containers: []*container{c}, Unused: images}
		for _, p := range []*string{&c.Name, &c.Image, &c.API} {
			*p = string([]rune(*p))
		}
		for i := range want.Unused {
			want.Unused[i].Image = string([]rune(want.Unused[i].Image))
		}
		if len(got.Containers) != 1 || got.Containers[0].properties.label() != c.properties.label() ||
			got.Containers[0].Name != c.Name || got.Containers[0].Image != c.Image || got.Containers[0].API != c.API ||
			!reflect.DeepEqual(got.Unused, want.Unused) {
			t.Errorf("stored %+v, read back %+v", want, got)
		}
	})
}

// A stored file that isn't the list, as one cut short or of the shape a
// build before this one wrote, stops the agent saying which file, rather
// than being taken for no containers, which would forget them; a missing
// one is no containers.
func TestAStoredFileThatIsntTheListIsRefused(t *testing.T) {
	for name, data := range map[string]string{
		"cut short":                `{"containers":[{"name":"app-web"`,
		"a bare list":              `[{"name":"dns","image":"moostack.local/dns:1.0.0"}]`,
		"containers not a list":    `{"containers":{"name":"app-web"}}`,
		"unused images not a list": `{"containers":[],"unused":"example.test/app:1"}`,
	} {
		file := filepath.Join(t.TempDir(), "containers")
		if err := os.WriteFile(file, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadContainers(t.Context(), nil, nil, Config{ContainersFile: file}); err == nil || !strings.Contains(err.Error(), file) {
			t.Errorf("%s: loading the containers says %v, want it refused, naming %s", name, err, file)
		}
	}
	s, err := loadContainers(t.Context(), nil, nil, Config{ContainersFile: filepath.Join(t.TempDir(), "containers")})
	if err != nil || len(s.list) != 0 || len(s.unused) != 0 {
		t.Errorf("loading a missing file: %v, %v, want no containers", s, err)
	}
}

// An image's config read without its user has no user however its keys
// are cased, as encoding/json, which WithImageConfig reads it with,
// matches them, and keeps every other field.
func TestAnImageConfigWithoutItsUserKeepsTheRest(t *testing.T) {
	for name, data := range map[string]string{
		"its user":               `{"architecture":"amd64","config":{"User":"nobody","Entrypoint":["/app"]}}`,
		"its user in lowercase":  `{"architecture":"amd64","config":{"user":"nobody","Entrypoint":["/app"]}}`,
		"its config capitalised": `{"architecture":"amd64","Config":{"USER":"nobody","Entrypoint":["/app"]}}`,
		"no user":                `{"architecture":"amd64","config":{"Entrypoint":["/app"]}}`,
		"no config":              `{"architecture":"amd64"}`,
		"a config that is null":  `{"architecture":"amd64","config":null}`,
	} {
		got, err := configWithoutUser([]byte(data))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		var img ocispec.Image
		if err := json.Unmarshal(got, &img); err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		wantEntrypoint := strings.Contains(data, "Entrypoint")
		if img.Config.User != "" || img.Architecture != "amd64" || (len(img.Config.Entrypoint) == 1) != wantEntrypoint {
			t.Errorf("%s: read without its user, %s reads as %+v", name, got, img)
		}
	}
}
