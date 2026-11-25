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
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/moostackhq/moostack/internal/clusterdir"
	"github.com/moostackhq/moostack/internal/strictyaml"
)

// FileName is the deploy file's name in the app's directory.
const FileName = "deploy.yaml"

// defaultStopDelay is the seconds a replica of a container with a port
// waits before it is stopped, if its file doesn't say.
const defaultStopDelay = 5

// Version is the latest version of the deploy file's format, which a
// file that states none is read as.
const Version = 1

// App is what a deploy file says. Its paths are absolute. It has
// Containers, Manifests or Chart, and Manifests and Chart are not both
// set.
type App struct {
	Name string
	// Cluster is the ref of the cluster the app is deployed to, empty if
	// the file names none.
	Cluster string
	// Containers are the app's containers, in the order of their names.
	// Those of an app with Manifests or Chart are only built: each has
	// its Name and its Build.
	Containers []Container
	// Manifests is a directory of manifests, in which
	// ${image:<container>} is the image built for a container.
	Manifests string
	// Chart is a chart's directory; the image built for a container is
	// its value images.<container>.
	Chart string
	// Secrets are the names of the secret store's secrets the app reads.
	Secrets []string
	Dir     string
}

// Container is a container of an app. One of Build and Image is set.
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
	// Env are the container's environment values, by variable.
	Env map[string]Value
	// Volumes are the directories of the container that outlive its
	// replicas, in the order of their names.
	Volumes []Volume
	// PreStart is the command that runs before each replica starts; nil
	// if none does.
	PreStart *Command
	// Cron are the commands that run on a schedule, in the order of
	// their names.
	Cron []Cron
	// Healthcheck is what must answer before a replica takes requests;
	// nil if the file gives none.
	Healthcheck *Healthcheck
	// Rolling says a container with Volumes is rolled.
	Rolling bool
	// StopDelay is the seconds a replica waits before it is stopped.
	StopDelay int
}

// Volume is a directory, at the absolute Path in its container, that
// outlives the container's replicas.
type Volume struct {
	Name, Path string
}

// Command is a command to run, as User if it isn't empty: a user's ID,
// or a user's and a group's with a colon between.
type Command struct {
	Command []string
	User    string
}

// Cron is a command that runs on Schedule, a crontab expression of five
// fields.
type Cron struct {
	Name     string
	Schedule string
	Command
}

// Healthcheck is a check of a replica: a GET of Path on its container's
// port, or Command run in it. One is set.
type Healthcheck struct {
	Path    string
	Command []string
}

// Value is an environment value: Value, or the secret called Secret, one
// of the app's Secrets.
type Value struct {
	Value  string
	Secret string
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

// command is a Command as YAML holds it.
type command struct {
	Command []string `yaml:"command"`
	User    string   `yaml:"user"`
}

// file is a deploy file as YAML holds it.
type file struct {
	Version    yaml.Node `yaml:"version"`
	Name       string    `yaml:"name"`
	Cluster    string    `yaml:"cluster"`
	Containers map[string]*struct {
		Build    *build               `yaml:"build"`
		Image    string               `yaml:"image"`
		Command  []string             `yaml:"command"`
		Port     *int                 `yaml:"port"`
		Host     string               `yaml:"host"`
		Replicas *int                 `yaml:"replicas"`
		Env      map[string]yaml.Node `yaml:"env"`
		Volumes  map[string]string    `yaml:"volumes"`
		PreStart *command             `yaml:"pre_start"`
		Cron     map[string]*struct {
			Schedule string `yaml:"schedule"`
			command  `yaml:",inline"`
		} `yaml:"cron"`
		Healthcheck *struct {
			Path    string   `yaml:"path"`
			Command []string `yaml:"command"`
		} `yaml:"healthcheck"`
		Rolling   *bool `yaml:"rolling"`
		StopDelay *int  `yaml:"stop_delay"`
	} `yaml:"containers"`
	Manifests string    `yaml:"manifests"`
	Chart     string    `yaml:"chart"`
	Build     yaml.Node `yaml:"build"`
	Secrets   []string  `yaml:"secrets"`
}

// variableName is what an environment variable's name, and so a
// secret's, may be.
var variableName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// userIDs is a user's ID, or a user's and a group's.
var userIDs = regexp.MustCompile(`^[0-9]{1,9}(:[0-9]{1,9})?$`)

// scheduleField is a field of a crontab expression.
var scheduleField = regexp.MustCompile(`^[0-9A-Za-z*/,?-]+$`)

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

// checkVersion refuses the version a deploy file states, n, unless it
// is Version. It takes a file that states none.
func checkVersion(n yaml.Node) error {
	if n.IsZero() {
		return nil
	}
	v, err := strconv.Atoi(n.Value)
	if n.Kind != yaml.ScalarNode || n.Tag != "!!int" || err != nil {
		return fmt.Errorf("version: give a whole number, such as %d", Version)
	}
	if v != Version {
		return fmt.Errorf("version: %d isn't a version this mooctl reads", v)
	}
	return nil
}

// ReadRef reads the cluster and the name of the app in dir's deploy file,
// and nothing else of it but its version. The cluster is empty if the
// file names none.
func ReadRef(dir string) (cluster, name string, err error) {
	path := filepath.Join(dir, FileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	var f struct {
		Version yaml.Node `yaml:"version"`
		Cluster string    `yaml:"cluster"`
		Name    string    `yaml:"name"`
	}
	if err := yaml.Unmarshal(data, &f); err != nil {
		return "", "", fmt.Errorf("%s: %w", path, err)
	}
	if err := checkVersion(f.Version); err != nil {
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
	var stated struct {
		Version yaml.Node `yaml:"version"`
	}
	if err := yaml.Unmarshal(data, &stated); err != nil {
		return nil, err
	}
	if err := checkVersion(stated.Version); err != nil {
		return nil, err
	}
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
	switch {
	case !f.Build.IsZero():
		return nil, errors.New("build: each container gives its own")
	case f.Manifests != "" && f.Chart != "":
		return nil, errors.New("give one of manifests and chart")
	case f.Containers == nil && f.Manifests == "" && f.Chart == "":
		return nil, errors.New("give containers, manifests or a chart")
	case f.Containers != nil && len(f.Containers) == 0:
		return nil, errors.New("containers: give at least one")
	}
	for _, s := range f.Secrets {
		if !variableName.MatchString(s) {
			return nil, fmt.Errorf("secrets: %q isn't a name an environment variable can have", s)
		}
		if slices.Contains(app.Secrets, s) {
			return nil, fmt.Errorf("secrets: %s is listed twice", s)
		}
		app.Secrets = append(app.Secrets, s)
	}
	onlyBuilt := f.Manifests != "" || f.Chart != ""
	hosts := map[string]string{}
	for _, name := range slices.Sorted(maps.Keys(f.Containers)) {
		c := f.Containers[name]
		if err := CheckName(name); err != nil {
			return nil, fmt.Errorf("containers: %w", err)
		}
		if onlyBuilt {
			if c == nil || c.Build == nil {
				return nil, fmt.Errorf("containers: %s: give its build", name)
			}
			for _, field := range []struct {
				name  string
				given bool
			}{
				{"image", c.Image != ""},
				{"command", c.Command != nil},
				{"port", c.Port != nil},
				{"host", c.Host != ""},
				{"replicas", c.Replicas != nil},
				{"env", c.Env != nil},
				{"volumes", c.Volumes != nil},
				{"pre_start", c.PreStart != nil},
				{"cron", c.Cron != nil},
				{"healthcheck", c.Healthcheck != nil},
				{"rolling", c.Rolling != nil},
				{"stop_delay", c.StopDelay != nil},
			} {
				if field.given {
					return nil, fmt.Errorf("containers: %s: %s: a container beside manifests or a chart is only built", name, field.name)
				}
			}
			b, err := readBuild(dir, c.Build)
			if err != nil {
				return nil, fmt.Errorf("containers: %s: build: %w", name, err)
			}
			app.Containers = append(app.Containers, Container{Name: name, Build: b})
			continue
		}
		if c == nil || (c.Build == nil) == (c.Image == "") {
			return nil, fmt.Errorf("containers: %s: give one of build and image", name)
		}
		out := Container{Name: name, Image: c.Image, Command: c.Command, Host: c.Host, Replicas: 1}
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
		if c.Replicas != nil {
			if *c.Replicas < 0 {
				return nil, fmt.Errorf("containers: %s: replicas: can't be negative", name)
			}
			if *c.Replicas > 0 {
				out.Replicas = *c.Replicas
			}
		}
		if out.Env, err = readEnv(c.Env, app.Secrets); err != nil {
			return nil, fmt.Errorf("containers: %s: env: %w", name, err)
		}
		for _, volume := range slices.Sorted(maps.Keys(c.Volumes)) {
			path := c.Volumes[volume]
			if err := CheckName(volume); err != nil {
				return nil, fmt.Errorf("containers: %s: volumes: %w", name, err)
			}
			if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
				return nil, fmt.Errorf("containers: %s: volumes: %s: give the directory's absolute path in the container", name, volume)
			}
			if i := slices.IndexFunc(out.Volumes, func(v Volume) bool { return v.Path == path }); i >= 0 {
				return nil, fmt.Errorf("containers: %s: volumes: %s: %s is %s's already", name, volume, path, out.Volumes[i].Name)
			}
			out.Volumes = append(out.Volumes, Volume{Name: volume, Path: path})
		}
		if len(out.Volumes) > 0 && out.Replicas > 1 {
			return nil, fmt.Errorf("containers: %s: replicas: a container with volumes runs as one replica", name)
		}
		if c.Rolling != nil {
			if len(out.Volumes) == 0 {
				return nil, fmt.Errorf("containers: %s: rolling: only a container with volumes takes it", name)
			}
			out.Rolling = *c.Rolling
		}
		if c.PreStart != nil {
			if out.PreStart, err = readCommand(*c.PreStart); err != nil {
				return nil, fmt.Errorf("containers: %s: pre_start: %w", name, err)
			}
		}
		for _, cron := range slices.Sorted(maps.Keys(c.Cron)) {
			entry := c.Cron[cron]
			if err := CheckName(cron); err != nil {
				return nil, fmt.Errorf("containers: %s: cron: %w", name, err)
			}
			if entry == nil {
				return nil, fmt.Errorf("containers: %s: cron: %s: give its schedule and its command", name, cron)
			}
			fields := strings.Fields(entry.Schedule)
			if len(fields) != 5 || slices.ContainsFunc(fields, func(f string) bool { return !scheduleField.MatchString(f) }) {
				return nil, fmt.Errorf("containers: %s: cron: %s: schedule: give a crontab expression of five fields, such as \"*/5 * * * *\"", name, cron)
			}
			run, err := readCommand(entry.command)
			if err != nil {
				return nil, fmt.Errorf("containers: %s: cron: %s: %w", name, cron, err)
			}
			out.Cron = append(out.Cron, Cron{Name: cron, Schedule: strings.Join(fields, " "), Command: *run})
		}
		if h := c.Healthcheck; h != nil {
			switch {
			case (h.Path == "") == (h.Command == nil):
				return nil, fmt.Errorf("containers: %s: healthcheck: give one of path and command", name)
			case h.Path != "" && !strings.HasPrefix(h.Path, "/"):
				return nil, fmt.Errorf("containers: %s: healthcheck: path: %q doesn't start with a slash", name, h.Path)
			case h.Path != "" && out.Port == 0:
				return nil, fmt.Errorf("containers: %s: healthcheck: path: give the port the container serves HTTP on too", name)
			case h.Command != nil && (len(h.Command) == 0 || h.Command[0] == ""):
				return nil, fmt.Errorf("containers: %s: healthcheck: command: give the program to run, and its arguments", name)
			}
			out.Healthcheck = &Healthcheck{Path: h.Path, Command: h.Command}
		}
		if out.Port != 0 {
			out.StopDelay = defaultStopDelay
		}
		if c.StopDelay != nil {
			if *c.StopDelay < 0 {
				return nil, fmt.Errorf("containers: %s: stop_delay: can't be negative", name)
			}
			out.StopDelay = *c.StopDelay
		}
		app.Containers = append(app.Containers, out)
	}
	switch {
	case f.Manifests != "":
		d, err := directory(dir, f.Manifests)
		if err != nil {
			return nil, fmt.Errorf("manifests: %w", err)
		}
		app.Manifests = d
	case f.Chart != "":
		d, err := directory(dir, f.Chart)
		if err != nil {
			return nil, fmt.Errorf("chart: %w", err)
		}
		if _, err := os.Stat(filepath.Join(d, "Chart.yaml")); err != nil {
			return nil, fmt.Errorf("chart: %s has no Chart.yaml", d)
		}
		app.Chart = d
	}
	return app, nil
}

// readCommand is the command c: a program and its arguments, and a
// user that is IDs if it is given.
func readCommand(c command) (*Command, error) {
	if len(c.Command) == 0 || c.Command[0] == "" {
		return nil, errors.New("command: give the program to run, and its arguments")
	}
	if c.User != "" && !userIDs.MatchString(c.User) {
		return nil, fmt.Errorf("user: %q isn't a user's ID, or a user's and a group's as in \"33:33\"", c.User)
	}
	return &Command{Command: c.Command, User: c.User}, nil
}

// readEnv is the environment values that env holds, each a scalar or
// {secret: NAME} with NAME one of secrets.
func readEnv(env map[string]yaml.Node, secrets []string) (map[string]Value, error) {
	if len(env) == 0 {
		return nil, nil
	}
	out := map[string]Value{}
	for _, variable := range slices.Sorted(maps.Keys(env)) {
		if !variableName.MatchString(variable) {
			return nil, fmt.Errorf("%q isn't a name an environment variable can have", variable)
		}
		n := env[variable]
		v := &n
		if v.Kind == yaml.AliasNode {
			v = v.Alias
		}
		switch {
		case v.Kind == yaml.ScalarNode && v.Tag != "!!null":
			out[variable] = Value{Value: v.Value}
		case v.Kind == yaml.MappingNode && len(v.Content) == 2 && v.Content[0].Value == "secret" && v.Content[1].Kind == yaml.ScalarNode && v.Content[1].Value != "":
			secret := v.Content[1].Value
			if !slices.Contains(secrets, secret) {
				return nil, fmt.Errorf("%s: the secret %s isn't in secrets", variable, secret)
			}
			out[variable] = Value{Secret: secret}
		default:
			return nil, fmt.Errorf("%s: give a value, or {secret: NAME}", variable)
		}
	}
	return out, nil
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
