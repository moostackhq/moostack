// Package appfile reads an app file: the app.yml in an app's own
// directory, which names the app and the image it runs, or the build
// context its image is built from.
package appfile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"unicode/utf8"

	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/strictyaml"
)

// FileName is the app file's name in the app's directory.
const FileName = "app.yml"

// App is what an app file says.
type App struct {
	// Name is the app's identity in a cluster: deploying a name that runs
	// already replaces it.
	Name string
	// Image is the reference of the public image the app runs,
	// normalized, as nodev1.NormalizeReference makes it, empty if the app
	// is built.
	Image string
	// Build is the directory of the build context the app's image is
	// built from, with a Dockerfile at its root, the app directory joined
	// with the file's build, empty if the app runs a public image.
	Build string
}

// fields are the fields an app file has, in the order an error lists
// them.
var fields = []string{"name", "image", "build"}

// namePattern is what an app's name may be: lowercase letters, digits and
// single hyphens between them, starting with a letter, so that its
// container's name, ContainerName, is one, and it is a DNS label.
var namePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*$`)

// maxName bounds an app's name, so that its container's name is at most
// 63, as a container's and a DNS label's are.
const maxName = 63 - len(containerPrefix)

// Error is why an app file is refused: what is wrong with its field, or
// with the file as a whole if Field is empty.
type Error struct {
	File, Field, Reason string
}

func (e *Error) Error() string {
	if e.Field == "" {
		return e.File + ": " + e.Reason
	}
	return e.File + ": " + e.Field + ": " + e.Reason
}

// Read reads and checks the app file in dir: one YAML document, a
// mapping of the fields name, required, and image, each a string, and no
// other field. name must be a name as the name pattern says, at most
// maxName, and either image, a reference that nodev1.NormalizeReference
// takes, which App holds normalized, or build, a path relative to dir,
// which may lead outside it, to a directory with a Dockerfile at its
// root. Every refusal is an *Error, but for a file that can't be read.
func Read(dir string) (*App, error) {
	file, values, err := read(dir)
	if err != nil {
		return nil, err
	}
	refuse := func(field, reason string, args ...any) (*App, error) {
		return nil, &Error{File: file, Field: field, Reason: fmt.Sprintf(reason, args...)}
	}
	for _, field := range []string{"image", "build"} {
		if v, ok := values[field]; ok && !isString(v) {
			return refuse(field, "is %s, not a string", kindOf(v))
		}
	}
	imageNode, hasImage := values["image"]
	_, hasBuild := values["build"]
	switch {
	case hasImage && hasBuild:
		return refuse("image", "is given with build; give one of them")
	case hasBuild:
		build, reason := buildContext(dir, values["build"].Value)
		if reason != "" {
			return refuse("build", "%s", reason)
		}
		return &App{Name: values["name"].Value, Build: build}, nil
	case !hasImage:
		return refuse("image", "is missing; name a public image, such as nginx:1.27")
	}
	normalized, err := nodev1.NormalizeReference(imageNode.Value)
	if err != nil {
		return refuse("image", "%s", status.Convert(err).Message())
	}
	return &App{Name: values["name"].Value, Image: normalized}, nil
}

// buildContext is the build context's directory that build, a build
// field's value, names relative to dir, the app directory, or else why
// it doesn't name one.
func buildContext(dir, build string) (string, string) {
	shown := printable(build)
	switch {
	case build == "":
		return "", "is empty; give the build context's directory, such as ., relative to the app directory"
	case filepath.IsAbs(build):
		return "", shown + " is an absolute path; give the build context relative to the app directory, so that the app file works wherever the app is"
	}
	path := filepath.Join(dir, build)
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "", shown + " doesn't exist; give the build context's directory, relative to the app directory"
	case err != nil:
		return "", fmt.Sprintf("%s can't be read: %v", shown, errors.Unwrap(err))
	case !info.IsDir():
		return "", shown + " isn't a directory; give the build context's directory, relative to the app directory"
	}
	// buildx reads the Dockerfile within the build context, where the
	// links it follows aren't the host's: one that is a link is refused,
	// rather than followed as buildx may not.
	switch info, err := os.Lstat(filepath.Join(path, dockerfile)); {
	case errors.Is(err, fs.ErrNotExist) || err == nil && info.IsDir():
		return "", shown + " has no " + dockerfile + " at its root, which mooctl builds the app with"
	case err != nil:
		return "", fmt.Sprintf("%s's %s can't be read: %v", shown, dockerfile, unwrapPath(err))
	case info.Mode()&fs.ModeSymlink != 0:
		return "", shown + "'s " + dockerfile + " is a link, which mooctl doesn't follow, as buildx follows it within the build context and not as this machine does; put the Dockerfile itself at the build context's root"
	case !info.Mode().IsRegular():
		return "", shown + "'s " + dockerfile + " isn't a file; put the Dockerfile itself at the build context's root"
	}
	// buildx must read it, not only find it, and list the context it
	// sends.
	f, err := os.Open(filepath.Join(path, dockerfile))
	if err != nil {
		return "", fmt.Sprintf("%s's %s can't be read: %v", shown, dockerfile, unwrapPath(err))
	}
	f.Close()
	// One name is enough to tell, however large the context.
	d, err := os.Open(path)
	if err == nil {
		_, err = d.Readdirnames(1)
		d.Close()
	}
	if err != nil {
		return "", fmt.Sprintf("%s can't be listed, as buildx lists the build context: %v", shown, unwrapPath(err))
	}
	return path, ""
}

// unwrapPath is err without the path an *fs.PathError names.
func unwrapPath(err error) error {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return pe.Err
	}
	return err
}

// dockerfile is the name of the file at a build context's root that the
// app's image is built with.
const dockerfile = "Dockerfile"

// ReadName reads the app file in dir for the app's name alone, as Read
// checks the file but for image and build, which it doesn't check: what
// they are, or whether either is given.
func ReadName(dir string) (string, error) {
	_, values, err := read(dir)
	if err != nil {
		return "", err
	}
	return values["name"].Value, nil
}

// read reads the app file in dir, and checks its shape and its name as
// Read says, but not what image and build are: it returns the file's
// path and its fields' values, by field.
func read(dir string) (string, map[string]*yaml.Node, error) {
	file := filepath.Join(dir, FileName)
	data, err := os.ReadFile(file)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil, &Error{File: file, Reason: "there is no app file; run mooctl deploy in the app's directory, or give it the directory"}
	}
	if errors.Is(err, syscall.ENOTDIR) {
		return "", nil, &Error{File: file, Reason: dir + " isn't a directory; give mooctl deploy the app's directory, which holds its " + FileName}
	}
	if err != nil {
		return "", nil, err
	}
	refuse := func(field, reason string, args ...any) (string, map[string]*yaml.Node, error) {
		return "", nil, &Error{File: file, Field: field, Reason: fmt.Sprintf(reason, args...)}
	}
	var doc yaml.Node
	if err := strictyaml.Unmarshal(data, &doc); err != nil {
		return refuse("", "%v", err)
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return refuse("", "an app file is a mapping of the fields %s", strings.Join(fields, ", "))
	}
	values := map[string]*yaml.Node{}
	pairs := doc.Content[0].Content
	for i := 0; i < len(pairs); i += 2 {
		key, value := pairs[i], pairs[i+1]
		if key.Kind != yaml.ScalarNode {
			return refuse("", "a key that isn't a field's name, %s, isn't a field of an app file, which has %s", kindOf(key), strings.Join(fields, ", "))
		}
		if !slices.Contains(fields, key.Value) {
			return refuse(printable(key.Value), "isn't a field of an app file, which has %s", strings.Join(fields, ", "))
		}
		if _, ok := values[key.Value]; ok {
			return refuse(key.Value, "is given twice")
		}
		values[key.Value] = value
	}
	var name string
	if v, ok := values["name"]; ok {
		if !isString(v) {
			return refuse("name", "is %s, not a string", kindOf(v))
		}
		name = v.Value
	}
	switch {
	case name == "":
		return refuse("name", "is missing; give the app a name, such as web")
	case len(name) > maxName || !namePattern.MatchString(name):
		return refuse("name", "%q isn't lowercase letters, digits and single hyphens between them, starting with a letter, at most %d", name, maxName)
	}
	return file, values, nil
}

// isString reports whether the field's value node is a string.
func isString(node *yaml.Node) bool {
	return node.Kind == yaml.ScalarNode && node.ShortTag() == "!!str"
}

// printable is s, a text from the file, quoted if it holds a rune that
// isn't printable or isn't valid UTF-8, so that an error repeating it
// can't act on the operator's terminal.
func printable(s string) string {
	if !utf8.ValidString(s) || strings.ContainsFunc(s, func(r rune) bool { return !strconv.IsPrint(r) }) {
		return strconv.Quote(s)
	}
	return s
}

// kindOf is what node is, as an error says it.
func kindOf(node *yaml.Node) string {
	switch {
	case node.Kind == yaml.MappingNode:
		return "a mapping"
	case node.Kind == yaml.SequenceNode:
		return "a list"
	case node.Kind == yaml.AliasNode:
		return "an alias"
	case node.ShortTag() == "!!null":
		return "empty"
	case node.ShortTag() == "!!int", node.ShortTag() == "!!float":
		return "a number"
	case node.ShortTag() == "!!bool":
		return "true or false"
	}
	return fmt.Sprintf("of type %s", printable(node.ShortTag()))
}

// containerPrefix starts the name of every app's container.
//
// Temporary: the prefix, with the cluster services' service-, is what
// keeps an app from replacing a cluster service by its name, until a
// container's name gets an owner on the node.
const containerPrefix = "app-"

// ContainerName is the name of the container that the app called name
// runs as.
func ContainerName(name string) string { return containerPrefix + name }

// IsApp reports whether the container called container is an app's, as
// ContainerName names it.
func IsApp(container string) bool {
	return strings.HasPrefix(container, containerPrefix) && len(container) > len(containerPrefix)
}
