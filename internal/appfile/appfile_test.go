package appfile_test

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/appfile"
)

// write writes data as the app file of a new directory, and returns the
// directory.
func write(t testing.TB, data string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, appfile.FileName), []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestReadTakesEachKindOfValidAppFile(t *testing.T) {
	for data, want := range map[string]appfile.App{
		"name: web\nimage: nginx:1.27\n":                                                         {Name: "web", Image: "docker.io/library/nginx:1.27"},
		"name: web\nimage: nginx\n":                                                              {Name: "web", Image: "docker.io/library/nginx:latest"},
		"image: nginx:1.27\nname: web\n":                                                         {Name: "web", Image: "docker.io/library/nginx:1.27"},
		"name: \"web\"\nimage: 'ghcr.io/foo/bar:1'\n":                                            {Name: "web", Image: "ghcr.io/foo/bar:1"},
		"# the web app\nname: web # its name\nimage: nginx:1.27\n":                               {Name: "web", Image: "docker.io/library/nginx:1.27"},
		"---\nname: web\nimage: nginx:1.27\n":                                                    {Name: "web", Image: "docker.io/library/nginx:1.27"},
		"{name: web, image: nginx:1.27}":                                                         {Name: "web", Image: "docker.io/library/nginx:1.27"},
		"name: a\nimage: nginx:1.27\n":                                                           {Name: "a", Image: "docker.io/library/nginx:1.27"},
		"name: a1-b2-c3\nimage: nginx:1.27\n":                                                    {Name: "a1-b2-c3", Image: "docker.io/library/nginx:1.27"},
		"name: " + strings.Repeat("a", 59) + "\nimage: nginx:1.27\n":                             {Name: strings.Repeat("a", 59), Image: "docker.io/library/nginx:1.27"},
		"name: web\nimage: registry.test:5000/team/web@sha256:" + strings.Repeat("a", 64) + "\n": {Name: "web", Image: "registry.test:5000/team/web@sha256:" + strings.Repeat("a", 64)},
	} {
		got, err := appfile.Read(write(t, data))
		if err != nil || *got != want {
			t.Errorf("Read of\n%s= %+v, %v, want %+v", data, got, err, want)
		}
	}
}

func TestReadRefusesEachKindOfInvalidAppFileNamingTheFieldAndWhy(t *testing.T) {
	for data, want := range map[string]struct{ field, reason string }{
		"":            {"", "empty"},
		"# nothing\n": {"", "empty"},
		"name: web\nimage: nginx\n---\nname: api\n": {"", "more than one YAML document"},
		"- web\n":                                                    {"", "a mapping of the fields name, image, build"},
		"web\n":                                                      {"", "a mapping of the fields"},
		"name: web\nimage: [nginx\n":                                 {"", "yaml"},
		"image: nginx:1.27\n":                                        {"name", "is missing"},
		"name: \"\"\nimage: nginx:1.27\n":                            {"name", "is missing"},
		"name:\nimage: nginx:1.27\n":                                 {"name", "is empty, not a string"},
		"name: 7\nimage: nginx:1.27\n":                               {"name", "is a number, not a string"},
		"name: true\nimage: nginx:1.27\n":                            {"name", "is true or false, not a string"},
		"name: [web]\nimage: nginx:1.27\n":                           {"name", "is a list, not a string"},
		"name: {a: b}\nimage: nginx:1.27\n":                          {"name", "is a mapping, not a string"},
		"name: Web\nimage: nginx:1.27\n":                             {"name", `"Web" isn't lowercase letters, digits and single hyphens`},
		"name: 1web\nimage: nginx:1.27\n":                            {"name", "starting with a letter"},
		"name: web-\nimage: nginx:1.27\n":                            {"name", "single hyphens between them"},
		"name: -web\nimage: nginx:1.27\n":                            {"name", "single hyphens between them"},
		"name: we--b\nimage: nginx:1.27\n":                           {"name", "single hyphens between them"},
		"name: we_b\nimage: nginx:1.27\n":                            {"name", "lowercase letters, digits"},
		"name: we.b\nimage: nginx:1.27\n":                            {"name", "lowercase letters, digits"},
		"name: wéb\nimage: nginx:1.27\n":                             {"name", "lowercase letters, digits"},
		"name: " + strings.Repeat("a", 60) + "\nimage: nginx:1.27\n": {"name", "at most 59"},
		"name: web\n":                                                {"image", "is missing; name a public image"},
		"name: web\nimage: NGINX\n":                                  {"image", "must be lowercase"},
		"name: web\nimage: \"nginx:\"\n":                             {"image", "isn't an image's reference"},
		"name: web\nimage: 1.27\n":                                   {"image", "is a number, not a string"},
		"name: web\nimage: nginx:1.27\nbuild: .\n":                   {"image", "is given with build; give one of them"},
		"name: web\nbuild: .\n":                                      {"build", "isn't supported yet"},
		"name: web\nimage: nginx:1.27\nimage: nginx:1.28\n":          {"image", "is given twice"},
		"name: web\nimage: nginx:1.27\nports: [80]\n":                {"ports", "isn't a field of an app file, which has name, image, build"},
		"name: web\nimage: nginx:1.27\nNAME: web\n":                  {"NAME", "isn't a field"},
		"base: &b {name: web}\n<<: *b\nimage: nginx:1.27\n":          {"base", "isn't a field"},
		"name: !%1B[2J web\nimage: nginx:1.27\n":                     {"name", `is of type "!\x1b[2J", not a string`},
		"name: &n web\nimage: *n\n":                                  {"image", "is an alias, not a string"},
		"name: web\nimage: nginx:1.27\nversion: 2\n":                 {"version", "isn't a field"},
		"name: web\nimage: nginx:1.27\n? [a]\n: b\n":                 {"", "a key that isn't a field's name, a list, isn't a field"},
		"name: web\nimage: nginx:1.27\n\"\\e[2J\": x\n":              {`"\x1b[2J"`, "isn't a field"},
	} {
		dir := write(t, data)
		_, err := appfile.Read(dir)
		var e *appfile.Error
		if !errors.As(err, &e) {
			t.Errorf("Read of\n%s= %v, want an *appfile.Error", data, err)
			continue
		}
		if e.File != filepath.Join(dir, appfile.FileName) || e.Field != want.field || !strings.Contains(e.Reason, want.reason) {
			t.Errorf("Read of\n%s= %q (file %s, field %q), want the file, the field %q and %q", data, err, e.File, e.Field, want.field, want.reason)
		}
		if want.field != "" && !strings.Contains(err.Error(), e.File+": "+want.field+": ") {
			t.Errorf("Read of\n%s= %q, which doesn't name the file and the field", data, err)
		}
	}
}

func TestReadSaysToRunInTheAppsDirectoryWithoutAnAppFile(t *testing.T) {
	dir := t.TempDir()
	_, err := appfile.Read(dir)
	if err == nil || !strings.Contains(err.Error(), filepath.Join(dir, "app.yml")) || !strings.Contains(err.Error(), "run mooctl deploy in the app's directory") {
		t.Errorf("Read of a directory without an app file: %v", err)
	}
}

func TestReadSaysToGiveTheDirectoryWhenGivenTheFile(t *testing.T) {
	file := filepath.Join(write(t, "name: web\nimage: nginx:1.27\n"), appfile.FileName)
	_, err := appfile.Read(file)
	if err == nil || !strings.Contains(err.Error(), file+" isn't a directory; give mooctl deploy the app's directory") {
		t.Errorf("Read of the app file itself: %v", err)
	}
}

func TestReadFailsForAnAppFileItCantRead(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, appfile.FileName), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := appfile.Read(dir); err == nil {
		t.Error("Read of an app file that is a directory succeeded")
	}
}

func TestContainerNamesAreAppsAndOnlyThose(t *testing.T) {
	if got := appfile.ContainerName("web"); got != "app-web" {
		t.Errorf("ContainerName(web) = %q, want app-web", got)
	}
	for name, want := range map[string]bool{
		"app-web": true, "app-a": true, "app-": false, "app": false, "service-dns": false, "dns": false, "web": false, "apps-web": false,
	} {
		if got := appfile.IsApp(name); got != want {
			t.Errorf("IsApp(%q) = %v, want %v", name, got, want)
		}
	}
}

// allowed is what an app's name may hold at most: lowercase letters,
// digits and hyphens, starting with a letter, at most 59, which the
// node's rule for a container's name narrows to single hyphens between
// them.
var allowed = regexp.MustCompile(`^[a-z][a-z0-9-]{0,58}$`)

// Whatever Read takes is an app the node runs: its container's name is
// one the agent takes, never a cluster service's, and its image a
// normalized reference, which the agent's check takes. A name it takes is
// one allowed matches, not ending in a hyphen.
func FuzzRead(f *testing.F) {
	for _, seed := range []string{
		"name: web\nimage: nginx:1.27\n", "name: web\nbuild: .\n", "name: we--b\nimage: nginx\n", "name: Web\nimage: nginx\n",
		"{name: a, image: b}", "name: !%1B[2J web\nimage: !%FF x\n", "name: web\nimage: nginx:1.27\nports: [80]\n", "", "- a\n", "name: &n web\nimage: *n\n",
		"name: " + strings.Repeat("a", 59) + "\nimage: ghcr.io/a/b@sha256:" + strings.Repeat("a", 64) + "\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data string) {
		dir := write(t, data)
		app, err := appfile.Read(dir)
		if err != nil {
			var e *appfile.Error
			if !errors.As(err, &e) {
				t.Fatalf("Read of %q = %v, not an *appfile.Error", data, err)
			}
			// What it says of the file is printable, whatever the file holds.
			if msg := strings.TrimPrefix(err.Error(), e.File); strings.ContainsFunc(msg, func(r rune) bool { return !strconv.IsPrint(r) }) || !utf8.ValidString(msg) {
				t.Fatalf("Read of %q says %q, which isn't printable", data, msg)
			}
			return
		}
		container := appfile.ContainerName(app.Name)
		if err := nodev1.CheckName(container); err != nil {
			t.Fatalf("Read of %q took the name %q, whose container the agent refuses: %v", data, app.Name, err)
		}
		if !appfile.IsApp(container) || strings.HasPrefix(container, "service-") {
			t.Fatalf("Read of %q took the name %q, whose container %s isn't an app's", data, app.Name, container)
		}
		if !allowed.MatchString(app.Name) || strings.HasSuffix(app.Name, "-") {
			t.Fatalf("Read of %q took the name %q, which isn't an app's name", data, app.Name)
		}
		if err := nodev1.CheckReference(app.Image); err != nil {
			t.Fatalf("Read of %q took the image %q, which the agent refuses: %v", data, app.Image, err)
		}
	})
}

// ReadName and Read agree on every file: what Read takes, ReadName takes
// by the same name, and what ReadName refuses, Read refuses alike, while
// a file Read refuses for anything but image or build, ReadName refuses
// alike too.
func FuzzReadNameAgreesWithRead(f *testing.F) {
	for _, seed := range []string{
		"name: web\nimage: nginx:1.27\n", "name: web\nbuild: .\n", "name: web\n", "name: web\nimage: 1.27\n", "name: Web\nimage: nginx\n",
		"name: web\nimage: NGINX\nbuild: [x]\n", "image: nginx\n", "name: web\nports: 80\n", "", "- a\n", "name: &n web\nimage: *n\n",
		"name: web\nimage: nginx\nimage: httpd\n", "name: web\nbuild: .\nbuild: [x]\n",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data string) {
		dir := write(t, data)
		app, readErr := appfile.Read(dir)
		name, nameErr := appfile.ReadName(dir)
		if nameErr != nil {
			var e *appfile.Error
			if !errors.As(nameErr, &e) {
				t.Fatalf("ReadName of %q = %v, not an *appfile.Error", data, nameErr)
			}
			if msg := strings.TrimPrefix(nameErr.Error(), e.File); strings.ContainsFunc(msg, func(r rune) bool { return !strconv.IsPrint(r) }) || !utf8.ValidString(msg) {
				t.Fatalf("ReadName of %q says %q, which isn't printable", data, msg)
			}
			// Of image and build, only the file's shape counts: a field
			// given twice.
			if (e.Field == "image" || e.Field == "build") && e.Reason != "is given twice" {
				t.Fatalf("ReadName of %q refused %s: %v", data, e.Field, nameErr)
			}
			if readErr == nil || readErr.Error() != nameErr.Error() {
				t.Fatalf("ReadName of %q = %v, but Read = %v", data, nameErr, readErr)
			}
			return
		}
		if readErr == nil && app.Name != name {
			t.Fatalf("ReadName of %q = %q, but Read took the name %q", data, name, app.Name)
		}
		var e *appfile.Error
		if readErr != nil && (!errors.As(readErr, &e) || e.Field != "image" && e.Field != "build" || e.Reason == "is given twice") {
			t.Fatalf("ReadName of %q took %q, but Read refused it for other than image or build: %v", data, name, readErr)
		}
	})
}

// ReadName takes an app file by its name alone: image and build, whether
// given, valid, or both, aren't checked.
func TestReadNameTakesAnAppFileByItsNameAlone(t *testing.T) {
	for _, data := range []string{
		"name: web\nimage: nginx:1.27\n",
		"name: web\nbuild: .\n",
		"name: web\n",
		"name: web\nimage: nginx:1.27\nbuild: .\n",
		"name: web\nimage: NGINX\n",
		"name: web\nimage: \"nginx:\"\n",
		"name: web\nimage: 1.27\n",
		"name: web\nimage:\n",
		"name: web\nimage: [nginx]\n",
		"name: web\nbuild: {context: .}\n",
	} {
		if got, err := appfile.ReadName(write(t, data)); err != nil || got != "web" {
			t.Errorf("ReadName of\n%s= %q, %v, want web", data, got, err)
		}
	}
}

// ReadName refuses what Read refuses of the file's shape and its name.
func TestReadNameRefusesABadNameOrFileNamingTheFieldAndWhy(t *testing.T) {
	for data, want := range map[string]struct{ field, reason string }{
		"":                                {"", "empty"},
		"- web\n":                         {"", "a mapping of the fields name, image, build"},
		"image: nginx:1.27\n":             {"name", "is missing"},
		"name: 7\n":                       {"name", "is a number, not a string"},
		"name: Web\n":                     {"name", `"Web" isn't lowercase letters, digits and single hyphens`},
		"name: we--b\nbuild: .\n":         {"name", "single hyphens between them"},
		"name: web\nports: [80]\n":        {"ports", "isn't a field of an app file"},
		"name: web\nimage: a\nimage: b\n": {"image", "is given twice"},
		"name: web\nbuild: .\nbuild: x\n": {"build", "is given twice"},
		"name: web\nname: api\n":          {"name", "is given twice"},
	} {
		dir := write(t, data)
		_, err := appfile.ReadName(dir)
		var e *appfile.Error
		if !errors.As(err, &e) || e.File != filepath.Join(dir, appfile.FileName) || e.Field != want.field || !strings.Contains(e.Reason, want.reason) {
			t.Errorf("ReadName of\n%s= %v, want the field %q refused for %q", data, err, want.field, want.reason)
		}
	}
	if _, err := appfile.ReadName(t.TempDir()); err == nil || !strings.Contains(err.Error(), "there is no app file") {
		t.Errorf("ReadName of a directory without an app file: %v, want it to say there is none", err)
	}
}
