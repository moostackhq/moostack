// Package sshconfig resolves OpenSSH settings to connect to a
// host, from the user's SSH client config.
package sshconfig

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Options says which config files to read and who the local user is.
type Options struct {
	// UserConfig is the user's config file, usually ~/.ssh/config. It is
	// read first. Empty means none, as does a file that doesn't exist.
	UserConfig string
	// SystemConfig is the system-wide config file, usually
	// /etc/ssh/ssh_config, read after UserConfig. Empty means none.
	SystemConfig string
	// LocalUser is the local user's name.
	LocalUser string
	// Home is the local user's home directory.
	Home string
}

// Settings are the resolved settings for one target.
type Settings struct {
	HostName      string
	User          string
	Port          int
	IdentityFiles []string
	// IdentitiesOnly says to use only IdentityFiles, even if the agent
	// offers other keys.
	IdentitiesOnly bool
	// IdentityAgent is empty when the config doesn't set it.
	IdentityAgent string
	// ProxyJump is the jump hosts as the config writes them, quotes
	// included, with its tokens expanded. It is empty when the config
	// doesn't set it or sets none.
	ProxyJump string
	// ConnectTimeout is zero when the config doesn't set it.
	ConnectTimeout time.Duration
}

// defaultIdentityFiles are the files in ~/.ssh that OpenSSH tries when the
// config names none.
var defaultIdentityFiles = []string{"id_rsa", "id_ecdsa", "id_ecdsa_sk", "id_ed25519", "id_ed25519_sk"}

// Resolve resolves the settings for target, which is a host or user@host.
// An error in a config file names the file and line.
func Resolve(target string, opts Options) (Settings, error) {
	t, err := parseTarget(target)
	if err != nil {
		return Settings{}, err
	}
	// The target's user is set first, so it wins.
	r := &resolver{opts: opts, host: t.host}
	if t.user != nil {
		r.user = &located{value: *t.user}
	}
	if opts.UserConfig != "" {
		user := file{includeDir: filepath.Join(opts.Home, ".ssh"), enabled: true, checkPermissions: true, userConfig: true}
		if err := r.readFile(opts.UserConfig, user); err != nil {
			return Settings{}, err
		}
	}
	if opts.SystemConfig != "" {
		system := file{includeDir: filepath.Dir(opts.SystemConfig), enabled: true}
		if err := r.readFile(opts.SystemConfig, system); err != nil {
			return Settings{}, err
		}
	}
	return r.settings()
}

// resolver holds the values set so far. A nil value hasn't been set, and
// the first value set wins.
type resolver struct {
	opts Options
	// host is the target's host.
	host string

	// hostName has its tokens expanded already.
	hostName            *string
	user, identityAgent *located
	port                *int
	identitiesOnly      *bool
	connectTimeout      *time.Duration
	identityFiles       []located
	// proxy is the first ProxyJump or ProxyCommand.
	proxy *proxy
}

// proxy is a ProxyJump or ProxyCommand. Its value is the jump hosts, or
// empty if it is none. command says it is a ProxyCommand other than none.
type proxy struct {
	located
	command bool
}

// located is a value from a config file, with the file and line it is on
// for errors in expanding its tokens. A value from the target has no
// location and isn't expanded.
type located struct {
	value, at string
}

// lineError is an error on a line of a config file.
type lineError struct {
	at  string
	err error
}

func (e *lineError) Error() string { return e.at + ": " + e.err.Error() }
func (e *lineError) Unwrap() error { return e.err }

// maxIncludeDepth is how deeply OpenSSH lets Include nest: the config
// files it reads directly are at depth 0.
const maxIncludeDepth = 16

// file is the config file being read.
type file struct {
	path string
	line int
	// includeDir is where relative Include paths are.
	includeDir string
	depth      int
	// enabled is false in a file included from a block that doesn't match,
	// so that none of it applies.
	enabled bool
	// checkPermissions is set for the user config and included files,
	// which OpenSSH refuses to read if others could have written them.
	checkPermissions bool
	// userConfig is set for the user config and the files it includes,
	// where Include paths may start with ~.
	userConfig bool
	// active says whether the current Host or Match block applies.
	active bool
}

// at is the current line's location.
func (f *file) at() string { return fmt.Sprintf("%s:%d", f.path, f.line) }

// readFile applies the config file at path, read as f says. A file that
// doesn't exist is skipped, as is a directory, which only an Include glob
// can name.
func (r *resolver) readFile(path string, f file) error {
	if f.depth > maxIncludeDepth {
		return errors.New("too many nested Include levels")
	}
	data, isDir, err := readConfig(path, f.checkPermissions)
	if errors.Is(err, fs.ErrNotExist) || isDir {
		return nil
	}
	if err != nil {
		return err
	}
	f.path, f.active = path, f.enabled
	for i, line := range strings.Split(string(data), "\n") {
		f.line = i + 1
		if err := r.applyLine(&f, line); err != nil {
			if errors.As(err, new(*lineError)) {
				// The error is in an included file, and says where.
				return err
			}
			return &lineError{at: f.at(), err: err}
		}
	}
	return nil
}

// hostNameSoFar is the hostname as the config has set it so far.
func (r *resolver) hostNameSoFar() string {
	if r.hostName != nil {
		return *r.hostName
	}
	return r.host
}

// userSoFar is the remote user as the target or config has set it so far,
// before expanding its tokens.
func (r *resolver) userSoFar() string {
	if r.user != nil {
		return r.user.value
	}
	return r.opts.LocalUser
}

// settings expands the values set and fills in the defaults, in the order
// OpenSSH does: the hostname, then the user, then the rest, which may refer
// to both.
func (r *resolver) settings() (Settings, error) {
	s := Settings{HostName: strings.ToLower(r.hostNameSoFar()), User: r.opts.LocalUser, Port: 22}
	if r.port != nil {
		s.Port = *r.port
	}
	tokens := map[byte]string{'h': s.HostName, 'p': strconv.Itoa(s.Port), 'u': r.opts.LocalUser, 'd': r.opts.Home}
	if r.user != nil {
		s.User = r.user.value
		if r.user.at != "" {
			var err error
			if s.User, err = r.expandAt(*r.user, "User", tokens, false); err != nil {
				return Settings{}, err
			}
		}
	}
	tokens['r'] = s.User

	for _, f := range r.identityFiles {
		path, err := r.expandAt(f, "IdentityFile", tokens, true)
		if err != nil {
			return Settings{}, err
		}
		s.IdentityFiles = append(s.IdentityFiles, path)
	}
	if len(s.IdentityFiles) == 0 {
		for _, name := range defaultIdentityFiles {
			s.IdentityFiles = append(s.IdentityFiles, filepath.Join(r.opts.Home, ".ssh", name))
		}
	}
	if r.identitiesOnly != nil {
		s.IdentitiesOnly = *r.identitiesOnly
	}
	if a := r.identityAgent; a != nil {
		s.IdentityAgent = a.value
		// "none", SSH_AUTH_SOCK and $NAME are left for the caller to
		// interpret, as OpenSSH leaves them.
		named := a.value == "none" || a.value == "SSH_AUTH_SOCK" ||
			strings.HasPrefix(a.value, "$") && !strings.HasPrefix(a.value, "${")
		if !named {
			var err error
			if s.IdentityAgent, err = r.expandAt(*a, "IdentityAgent", tokens, true); err != nil {
				return Settings{}, err
			}
		}
	}
	if p := r.proxy; p != nil && p.command {
		return Settings{}, &lineError{at: p.at, err: errors.New("ProxyCommand isn't supported")}
	}
	if p := r.proxy; p != nil && p.value != "" {
		jumpTokens := map[byte]string{'h': tokens['h'], 'p': tokens['p'], 'r': tokens['r']}
		var err error
		if s.ProxyJump, err = r.expandAt(p.located, "ProxyJump", jumpTokens, false); err != nil {
			return Settings{}, err
		}
	}
	if r.connectTimeout != nil {
		s.ConnectTimeout = *r.connectTimeout
	}
	return s, nil
}

// readConfig reads the file at path, unless it is a directory. With
// checkPermissions, it first refuses a file or directory not owned by root
// or the user, or writable by its group or others, following symlinks.
func readConfig(path string, checkPermissions bool) (data []byte, isDir bool, err error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, false, err
	}
	if checkPermissions && !securePermissions(info) {
		return nil, false, fmt.Errorf("bad owner or permissions on %s", path)
	}
	if info.IsDir() {
		return nil, true, nil
	}
	data, err = io.ReadAll(file)
	return data, false, err
}

// expandAt expands v's tokens, and a leading "~" if tilde is set, with an
// error that names v's location.
func (r *resolver) expandAt(v located, keyword string, tokens map[byte]string, tilde bool) (string, error) {
	s := v.value
	var err error
	if tilde {
		s, err = r.expandTilde(s)
	}
	if err == nil {
		s, err = expand(s, keyword, tokens)
	}
	if err != nil {
		return "", &lineError{at: v.at, err: err}
	}
	return s, nil
}

// include applies the files matching an Include line's globs, in order.
func (r *resolver) include(f *file, globs []string) error {
	for _, pattern := range globs {
		if strings.Contains(pattern, "%") || strings.Contains(pattern, "${") {
			return fmt.Errorf("Include path %q has a token or environment variable, which isn't supported", pattern)
		}
		if strings.HasPrefix(pattern, "~") && !f.userConfig {
			return fmt.Errorf("bad Include path %q: ~ is only allowed in the user config", pattern)
		}
		pattern, err := r.expandTilde(pattern)
		if err != nil {
			// glob(3) finds nothing in an unknown user's home.
			continue
		}
		if !filepath.IsAbs(pattern) {
			// Not filepath.Join, which would clean away "..": OpenSSH
			// keeps it, so a missing directory before it matches nothing.
			pattern = f.includeDir + "/" + pattern
		}
		included := file{includeDir: f.includeDir, depth: f.depth + 1, enabled: f.active, checkPermissions: true, userConfig: f.userConfig}
		for _, path := range glob(pattern) {
			if err := r.readFile(path, included); err != nil {
				return err
			}
		}
	}
	return nil
}
