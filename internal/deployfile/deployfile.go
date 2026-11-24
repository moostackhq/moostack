// Package deployfile reads a deploy file: the deploy.yaml in an app's own
// directory, which names the cluster the app is deployed to and what it
// runs: its containers, or its own manifests or chart.
package deployfile

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/strictyaml"
)

// FileName is the deploy file's name in the app's directory.
const FileName = "deploy.yaml"

// App is what a deploy file says. Its paths are absolute. One of
// Containers, Manifests and Chart is set.
type App struct {
	Name string
	// Cluster is the ref of the cluster the app is deployed to, empty if
	// the file names none.
	Cluster string
	// Containers are the containers the app runs, in the order of their
	// names.
	Containers []Container
	// Manifests is a directory of manifests, in which ${IMAGE} is the
	// built image.
	Manifests string
	// Chart is a chart's directory; the built image is its value
	// image.
	Chart string
	// Build is how the image of the manifests or the chart is built; nil
	// if none is.
	Build *Build
	// Secrets are the names of the secret store's secrets the app gets.
	Secrets []string
	Dir     string
}

// Container is a container an app runs. One of Build and Image is set.
type Container struct {
	Name string
	// Build is how the container's image is built.
	Build *Build
	// Image is the image the container runs as it is.
	Image string
	// Command is what the container runs in place of its image's
	// entrypoint and command; the image's if empty.
	Command []string
	// Port is the port the container listens on, 0 if none.
	Port int
	// Host is the host name the container is served at; none if empty.
	Host string
	// Replicas is how many replicas of the container run.
	Replicas int
}

// Build is how an image is built: from a Dockerfile at the root of a
// build context, or by Railpack from source. One is set.
type Build struct {
	Dockerfile string
	Railpack   string
}

// build is a Build as YAML holds it.
type build struct {
	Dockerfile string `yaml:"dockerfile"`
	Railpack   string `yaml:"railpack"`
}

// file is a deploy file as YAML holds it.
type file struct {
	Name       string `yaml:"name"`
	Cluster    string `yaml:"cluster"`
	Containers map[string]*struct {
		Build    *build   `yaml:"build"`
		Image    string   `yaml:"image"`
		Command  []string `yaml:"command"`
		Port     *int     `yaml:"port"`
		Host     string   `yaml:"host"`
		Replicas int      `yaml:"replicas"`
	} `yaml:"containers"`
	Manifests string   `yaml:"manifests"`
	Chart     string   `yaml:"chart"`
	Build     *build   `yaml:"build"`
	Secrets   []string `yaml:"secrets"`
}

// secretName is what a secret's name may be: an environment variable's
// name.
var secretName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// hostName is a DNS name of labels of 1 to 63 characters.
var hostName = regexp.MustCompile(`^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)*[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// CheckName refuses a name an app or a container can't have: one that
// isn't a DNS label starting with a letter.
func CheckName(name string) error {
	if err := clusterdir.CheckName(name); err != nil {
		return err
	}
	if name[0] < 'a' || name[0] > 'z' {
		return fmt.Errorf("%q must start with a letter", name)
	}
	return nil
}

// ReadRef reads the cluster and the name of the app in dir's deploy file,
// and nothing else of it. The cluster is empty if the file names none.
func ReadRef(dir string) (cluster, name string, err error) {
	path := filepath.Join(dir, FileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	var f struct {
		Cluster string `yaml:"cluster"`
		Name    string `yaml:"name"`
	}
	if err := yaml.Unmarshal(data, &f); err != nil {
		return "", "", fmt.Errorf("%s: %w", path, err)
	}
	if f.Cluster != "" {
		if _, err := clusterdir.ParseRef(f.Cluster); err != nil {
			return "", "", fmt.Errorf("%s: cluster: %w", path, err)
		}
	}
	if err := CheckName(f.Name); err != nil {
		return "", "", fmt.Errorf("%s: name: %w", path, err)
	}
	return f.Cluster, f.Name, nil
}

// Read reads the deploy file in dir.
func Read(dir string) (*App, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, FileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	app, err := parse(dir, data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return app, nil
}

func parse(dir string, data []byte) (*App, error) {
	var f file
	if err := strictyaml.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	var err error
	app := &App{Cluster: f.Cluster, Name: f.Name, Dir: dir}
	if f.Cluster != "" {
		if _, err := clusterdir.ParseRef(f.Cluster); err != nil {
			return nil, fmt.Errorf("cluster: %w", err)
		}
	}
	if err := CheckName(f.Name); err != nil {
		return nil, fmt.Errorf("name: %w", err)
	}
	set := 0
	for _, ok := range []bool{f.Containers != nil, f.Manifests != "", f.Chart != ""} {
		if ok {
			set++
		}
	}
	if set != 1 {
		return nil, errors.New("give one of containers, manifests and chart")
	}
	if f.Build != nil {
		if f.Containers != nil {
			return nil, errors.New("build: each container gives its own")
		}
		if app.Build, err = readBuild(dir, f.Build); err != nil {
			return nil, fmt.Errorf("build: %w", err)
		}
	}
	switch {
	case f.Containers != nil:
		if len(f.Containers) == 0 {
			return nil, errors.New("containers: give at least one")
		}
		hosts := map[string]string{}
		for _, name := range slices.Sorted(maps.Keys(f.Containers)) {
			c := f.Containers[name]
			if c == nil {
				return nil, fmt.Errorf("containers: %s: give one of build and image", name)
			}
			if err := CheckName(name); err != nil {
				return nil, fmt.Errorf("containers: %w", err)
			}
			out := Container{Name: name, Image: c.Image, Command: c.Command, Host: c.Host, Replicas: c.Replicas}
			if (c.Build == nil) == (c.Image == "") {
				return nil, fmt.Errorf("containers: %s: give one of build and image", name)
			}
			if c.Build != nil {
				if out.Build, err = readBuild(dir, c.Build); err != nil {
					return nil, fmt.Errorf("containers: %s: build: %w", name, err)
				}
			}
			if strings.ContainsAny(c.Image, " \t\n") {
				return nil, fmt.Errorf("containers: %s: image: %q isn't an image's name", name, c.Image)
			}
			if c.Command != nil && (len(c.Command) == 0 || c.Command[0] == "") {
				return nil, fmt.Errorf("containers: %s: command: give the program to run, and its arguments", name)
			}
			if c.Port != nil {
				if *c.Port < 1 || *c.Port > 65535 {
					return nil, fmt.Errorf("containers: %s: port: give the port the container listens on, 1 to 65535", name)
				}
				out.Port = *c.Port
			}
			if c.Host != "" {
				if len(c.Host) > 253 || !hostName.MatchString(c.Host) {
					return nil, fmt.Errorf("containers: %s: host: %q isn't a host name", name, c.Host)
				}
				if out.Port == 0 {
					return nil, fmt.Errorf("containers: %s: host: give the port the container serves HTTP on too", name)
				}
				if other, ok := hosts[c.Host]; ok {
					return nil, fmt.Errorf("containers: %s: host: %s is %s's already", name, c.Host, other)
				}
				hosts[c.Host] = name
			}
			if c.Replicas < 0 {
				return nil, fmt.Errorf("containers: %s: replicas: can't be negative", name)
			}
			if out.Replicas == 0 {
				out.Replicas = 1
			}
			app.Containers = append(app.Containers, out)
		}
	case f.Manifests != "":
		d, err := directory(dir, f.Manifests)
		if err != nil {
			return nil, fmt.Errorf("manifests: %w", err)
		}
		app.Manifests = d
	default:
		d, err := directory(dir, f.Chart)
		if err != nil {
			return nil, fmt.Errorf("chart: %w", err)
		}
		if _, err := os.Stat(filepath.Join(d, "Chart.yaml")); err != nil {
			return nil, fmt.Errorf("chart: %s has no Chart.yaml", d)
		}
		app.Chart = d
	}
	seen := map[string]bool{}
	for _, s := range f.Secrets {
		if !secretName.MatchString(s) {
			return nil, fmt.Errorf("secrets: %q isn't a name an environment variable can have", s)
		}
		if seen[s] {
			return nil, fmt.Errorf("secrets: %s is listed twice", s)
		}
		seen[s] = true
		app.Secrets = append(app.Secrets, s)
	}
	return app, nil
}

// readBuild is the build b of the app in dir.
func readBuild(dir string, b *build) (*Build, error) {
	switch {
	case (b.Dockerfile == "") == (b.Railpack == ""):
		return nil, errors.New("give one of dockerfile and railpack")
	case b.Dockerfile != "":
		ctx, err := directory(dir, b.Dockerfile)
		if err != nil {
			return nil, fmt.Errorf("dockerfile: %w", err)
		}
		if _, err := os.Stat(filepath.Join(ctx, "Dockerfile")); err != nil {
			return nil, fmt.Errorf("dockerfile: %s has no Dockerfile", ctx)
		}
		return &Build{Dockerfile: ctx}, nil
	default:
		src, err := directory(dir, b.Railpack)
		if err != nil {
			return nil, fmt.Errorf("railpack: %w", err)
		}
		return &Build{Railpack: src}, nil
	}
}

// directory is p, relative to dir, which must be a directory.
func directory(dir, p string) (string, error) {
	if !filepath.IsAbs(p) {
		p = filepath.Join(dir, p)
	}
	info, err := os.Stat(p)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%s isn't a directory", p)
	}
	return p, nil
}
