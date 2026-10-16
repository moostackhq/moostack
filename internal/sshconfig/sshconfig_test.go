package sshconfig_test

import (
	"bufio"
	"cmp"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/internal/sshconfig"
)

// resolveCase is a set of config files and a target, with the settings
// expected for the local user "op" whose home is the test's directory.
type resolveCase struct {
	name string
	// files are written under the home directory. ".ssh/config" is the user
	// config and "etc/ssh/ssh_config" the system config. "{home}" in a file
	// is replaced by the home directory.
	files  map[string]string
	target string
	// want's strings may contain "{home}" too.
	want sshconfig.Settings
	// modes are the files' modes, where not 0600.
	modes map[string]os.FileMode
	// noSSH says why the case can't be compared with ssh -G, if it can't.
	noSSH string
}

var defaultIdentityFiles = []string{
	"{home}/.ssh/id_rsa",
	"{home}/.ssh/id_ecdsa",
	"{home}/.ssh/id_ecdsa_sk",
	"{home}/.ssh/id_ed25519",
	"{home}/.ssh/id_ed25519_sk",
}

var resolveCases = []resolveCase{
	{
		name:   "no config gives the defaults",
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "a matching Host block sets every option",
		files: map[string]string{".ssh/config": `
Host web1
  HostName 10.0.0.1
  User admin
  Port 2222
  IdentityFile {home}/keys/web
  IdentitiesOnly yes
  IdentityAgent {home}/agent.sock
  ProxyJump bastion
  ConnectTimeout 15
`},
		target: "web1",
		want: sshconfig.Settings{
			HostName: "10.0.0.1", User: "admin", Port: 2222,
			IdentityFiles: []string{"{home}/keys/web"}, IdentitiesOnly: true,
			IdentityAgent: "{home}/agent.sock", ProxyJump: "bastion", ConnectTimeout: 15 * time.Second,
		},
	},
	{
		name: "a Host block for another host is skipped",
		files: map[string]string{".ssh/config": `
Host web2
  Port 2222
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "the first value set wins and identity files add up",
		files: map[string]string{".ssh/config": `
Port 1000
Host web1
  Port 2000
  User first
  IdentityFile {home}/a
Host web*
  User second
  IdentityFile {home}/b
Host *
  IdentitiesOnly yes
  ConnectTimeout 1m30s
`},
		target: "web1",
		want: sshconfig.Settings{
			HostName: "web1", User: "first", Port: 1000,
			IdentityFiles: []string{"{home}/a", "{home}/b"}, IdentitiesOnly: true, ConnectTimeout: 90 * time.Second,
		},
	},
	{
		name: "wildcards match any run and any single character",
		files: map[string]string{".ssh/config": `
Host db??
  User nope
Host w?b*.example.*
  User yes
`},
		target: "web1.example.com",
		want:   sshconfig.Settings{HostName: "web1.example.com", User: "yes", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "a negated pattern excludes a host the other patterns match",
		files: map[string]string{".ssh/config": `
Host * !web1
  User other
Host !db1 web*
  Port 2222
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 2222, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "a negated pattern alone matches nothing",
		files: map[string]string{".ssh/config": `
Host !db1
  User other
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "Host patterns are case-sensitive and the hostname is lowercased",
		files: map[string]string{".ssh/config": `
Host web1
  User lower
Host WEB1
  HostName Web1.Example.COM
`},
		target: "WEB1",
		want:   sshconfig.Settings{HostName: "web1.example.com", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "the target's user wins over the config's",
		files: map[string]string{".ssh/config": `
Host web1
  User admin
`},
		target: "deploy@web1",
		want:   sshconfig.Settings{HostName: "web1", User: "deploy", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "the target's user ends at its last @ and keeps its tokens",
		files:  map[string]string{".ssh/config": "User admin\n"},
		target: "%u@corp@web1",
		want:   sshconfig.Settings{HostName: "web1", User: "%u@corp", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "ProxyJump none switches jumping off for later blocks",
		files: map[string]string{".ssh/config": `
Host web1
  ProxyJump none
Host *
  ProxyJump bastion
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "keywords are case-insensitive, may be followed by = and arguments may be quoted",
		files: map[string]string{".ssh/config": `
# a comment
HOST web1
  hostname=10.0.0.1
  port = 2222
  IdentityFile "{home}/my keys/web"
  identitiesonly True  # trailing comment
  User a\ b
`},
		target: "web1",
		want: sshconfig.Settings{
			HostName: "10.0.0.1", User: "a b", Port: 2222,
			IdentityFiles: []string{"{home}/my keys/web"}, IdentitiesOnly: true,
		},
	},
	{
		name: "keywords for settings Resolve doesn't hold are ignored",
		files: map[string]string{".ssh/config": `
SomeOptionNobodyUses whatever
ServerAliveInterval 30
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
		noSSH:  "ssh rejects keywords it doesn't know",
	},
	{
		name: "Include reads files matching a glob in order, and a glob matching nothing is fine",
		files: map[string]string{
			".ssh/config": `
Include {home}/conf.d/*.conf {home}/missing/*
Host *
  User fallback
`,
			"conf.d/b.conf": "Host web1\n  User b\n  Port 2000\n",
			"conf.d/a.conf": "Host web1\n  User a\n",
			"conf.d/c.txt":  "Host web1\n  Port 3000\n",
			// A hidden file needs a pattern that starts with a dot.
			"conf.d/.hidden.conf": "Host web1\n  User hidden\n",
			// A directory the glob matches is skipped.
			"conf.d/sub.conf/x": "Port 3000\n",
		},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "a", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "[!...] in an Include glob negates",
		files:  withConfig(globFiles, `Include {home}/g/[!!]`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_star", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "^ in an Include glob bracket is a plain character",
		files:  withConfig(globFiles, `Include {home}/g/[^x]`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_x", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "an unclosed [ in an Include glob is a plain character",
		files:  withConfig(globFiles, `Include {home}/g/[z`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_br", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "] first in an Include glob bracket is a plain character",
		files:  withConfig(globFiles, `Include {home}/g/[]]`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_close", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a range in an Include glob bracket",
		files:  withConfig(globFiles, `Include {home}/g/[x-y]`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_x", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a reversed range in an Include glob bracket matches nothing",
		files:  withConfig(globFiles, `Include {home}/g/[y-x]`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a backslash escapes a wildcard in an Include glob",
		files:  withConfig(globFiles, `Include {home}/g/\*`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_star", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a backslash escapes a wildcard beside another wildcard",
		files:  withConfig(globFiles, `Include {home}/g/\**`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_star", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a backslash escapes ] in an Include glob bracket",
		files:  withConfig(globFiles, `Include {home}/g/[\]]`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_close", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a character class in an Include glob bracket",
		files:  withConfig(globFiles, `Include {home}/g/[[:upper:]]`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_B", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a bracket can't match a leading dot",
		files:  withConfig(globFiles, `Include {home}/g/[.]hd/*`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "an escaped leading dot matches a dot",
		files:  withConfig(globFiles, `Include {home}/g/\.h?/*`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_hidden_dir", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "an Include glob goes through a symlinked directory",
		files:  withConfig(globFiles, `Include {home}/g/l*/f1`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_sub", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "an Include glob for names with a leading dot matches . and ..",
		files: map[string]string{
			".ssh/config": "Include {home}/d/e/.*/inc\n",
			"d/e/x":       "",
			"d/inc":       "User from_parent\n",
		},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_parent", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "an escaped slash in an Include path is a slash",
		files:  withConfig(globFiles, `Include {home}/g\/x`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_x", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "an escaped slash before a wildcard in an Include path is a slash",
		files:  withConfig(globFiles, `Include {home}/g\/[x]`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_x", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "Include sorts matches by byte order across directories",
		files: map[string]string{
			".ssh/config": "Include {home}/g/*/conf\n",
			"g/a/conf":    "User from_a\n",
			"g/a-b/conf":  "User from_a-b\n",
		},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_a-b", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "an included file readable by its group is fine",
		files: map[string]string{
			".ssh/config": "Include {home}/inc\n",
			"inc":         "User inc\n",
		},
		modes:  map[string]os.FileMode{"inc": 0o640},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "inc", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "Include may nest 16 deep, and the deepest file may include nothing",
		files: map[string]string{
			".ssh/config": "Include {home}/d/1\n",
			"d/1":         "Include {home}/d/2\n",
			"d/2":         "Include {home}/d/3\n",
			"d/3":         "Include {home}/d/4\n",
			"d/4":         "Include {home}/d/5\n",
			"d/5":         "Include {home}/d/6\n",
			"d/6":         "Include {home}/d/7\n",
			"d/7":         "Include {home}/d/8\n",
			"d/8":         "Include {home}/d/9\n",
			"d/9":         "Include {home}/d/10\n",
			"d/10":        "Include {home}/d/11\n",
			"d/11":        "Include {home}/d/12\n",
			"d/12":        "Include {home}/d/13\n",
			"d/13":        "Include {home}/d/14\n",
			"d/14":        "Include {home}/d/15\n",
			"d/15":        "Include {home}/d/16\n",
			"d/16":        "User deepest\nInclude {home}/nothing/*\n",
		},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "deepest", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "an empty argument is fine for a keyword Settings doesn't hold",
		files: map[string]string{".ssh/config": `
RemoteCommand printf ""
Host web1
  User admin
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "admin", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a keyword may be double-quoted, and = after the quote is part of the argument",
		files:  map[string]string{".ssh/config": "\"User\"=bob\n\"Port\"2000\n\"HostName web1.example\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "=bob", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "arguments split at spaces and tabs only, and trailing CR and form feed are dropped",
		files:  map[string]string{".ssh/config": "User\ta\rb\fc\vd\r\nPort 2000 \f\r\nHostName\rweb1.example\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1.example", User: "a\rb\fc\vd", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "Match all may end a list of criteria",
		files: map[string]string{".ssh/config": `
Match host db1 all
  User nope
Match host=web1 all # comment
  Port 2000
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a target's user may hold characters OpenSSH allows",
		target: "a$b\\c d*@web1",
		want:   sshconfig.Settings{HostName: "web1", User: "a$b\\c d*", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a [ whose only ] is its first character is a plain character",
		files:  withConfig(globFiles, `Include {home}/g/[]a`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_br_close_a", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a [ whose only ] is its escaped first character is a plain character",
		files:  withConfig(globFiles, `Include {home}/g/[\]`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_br_close", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "an unknown class in an Include glob bracket matches nothing",
		files:  withConfig(globFiles, `Include {home}/g/[[:foo:]x]`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a class that takes an Include glob bracket's only ] matches nothing",
		files:  withConfig(globFiles, `Include {home}/g/[[:alpha:]`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
		noSSH:  "ssh crashes on it",
	},
	{
		name:   "an Include path keeps .., so it needs the directory before it",
		files:  withConfig(globFiles, `Include {home}/g/nope/../x {home}/g/sub/../y`),
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "from_y", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "a keyword may have a quote inside it or follow =, but not two",
		files:  map[string]string{".ssh/config": "Us\"er\" bob\n = Port 2000\n==HostName nope\n= =HostName nope\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "bob", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "a leading dot in a file or directory name only matches a dot in the Include pattern",
		files: map[string]string{
			".ssh/config":    "Include {home}/inc/.* {home}/*/b.conf {home}/inc/?hidden\n",
			"inc/.dot":       "User dot\n",
			"inc/.hidden":    "ProxyJump hidden\n",
			".hide/b.conf":   "Port 1000\n",
			"visible/b.conf": "Port 2000\n",
		},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "dot", Port: 2000, IdentityFiles: defaultIdentityFiles, ProxyJump: "hidden"},
	},
	{
		name: "Include in a Host block only applies when the block matches, and Host lines in it end with the file",
		files: map[string]string{
			".ssh/config": `
Host db1
  Include {home}/db.conf
Host web1
  Include {home}/web.conf
  User web
`,
			"db.conf":  "User db\nHost *\n  Port 1000\nMatch all\n  Port 1000\n",
			"web.conf": "Port 2000\nHost other\n  ProxyJump nope\n",
		},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "web", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "a relative Include in the user config is under ~/.ssh, and ~ is the home directory",
		files: map[string]string{
			".ssh/config":          "Include conf.d/*\nInclude ~/other.conf\n",
			".ssh/conf.d/web.conf": "Host web1\n  User web\n",
			"other.conf":           "Port 2000\n",
		},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "web", Port: 2000, IdentityFiles: defaultIdentityFiles},
		noSSH:  "ssh would read the real ~/.ssh",
	},
	{
		name: "a relative Include path keeps .., and a file the user config includes may include ~ paths",
		files: map[string]string{
			".ssh/config": "Include nope/../a.conf sub/../b.conf\n",
			".ssh/a.conf": "User a\n",
			".ssh/b.conf": "Include ~/c.conf\n",
			".ssh/sub/x":  "",
			"c.conf":      "User c\n",
		},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "c", Port: 22, IdentityFiles: defaultIdentityFiles},
		noSSH:  "ssh would read the real ~/.ssh",
	},
	{
		name: "the system config is read after the user config, and a relative Include in it is under its directory",
		files: map[string]string{
			".ssh/config":                 "Host web1\n  User web\n",
			"etc/ssh/ssh_config":          "Include ssh_config.d/*.conf\n",
			"etc/ssh/ssh_config.d/a.conf": "User system\nPort 2000\n",
		},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "web", Port: 2000, IdentityFiles: defaultIdentityFiles},
		noSSH:  "ssh -F skips the system config",
	},
	{
		name:   "Port takes a service name",
		files:  map[string]string{".ssh/config": "Port https\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 443, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "a backslash escapes a space outside quotes but not inside them",
		files: map[string]string{".ssh/config": `
User "a\ b\"c\'d\\e"
IdentityAgent {home}/a\ b
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: `a\ b"c'd\e`, Port: 22, IdentityFiles: defaultIdentityFiles, IdentityAgent: "{home}/a b"},
	},
	{
		name: "the same identity file is listed once",
		files: map[string]string{".ssh/config": `
IdentityFile {home}/a
Host *
  IdentityFile {home}/b
  IdentityFile {home}/a
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: []string{"{home}/a", "{home}/b"}},
	},
	{
		name: "ConnectTimeout none leaves the timeout unset, so a later one applies",
		files: map[string]string{".ssh/config": `
Host web1
  ConnectTimeout none
Host *
  ConnectTimeout 5
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles, ConnectTimeout: 5 * time.Second},
	},
	{
		name: "ProxyCommand none switches ProxyJump off",
		files: map[string]string{".ssh/config": `
Host web1
  ProxyCommand none
Host db1
  ProxyJump bastion
Host *
  ProxyCommand nc %h %p
  ProxyJump other
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "ProxyCommand none disables the proxy in any case",
		files:  map[string]string{".ssh/config": "ProxyCommand None\nProxyJump j\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "ProxyJump none disables the proxy in any case",
		files:  map[string]string{".ssh/config": "ProxyJump NONE\nProxyJump j\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "ProxyCommand none may follow any run of whitespace and =",
		files:  map[string]string{".ssh/config": "ProxyCommand= = none\nProxyJump j\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name:   "ProxyJump is the rest of the line, quotes and spaces included, up to a #",
		files:  map[string]string{".ssh/config": "ProxyJump = \"j\" k\\\\ l # comment\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles, ProxyJump: `"j" k\\ l`},
	},
	{
		name:   "ProxyJump none with a comment is a jump host named none",
		files:  map[string]string{".ssh/config": "ProxyJump none # c\nProxyJump j\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles, ProxyJump: "none"},
	},
	{
		name:   "ProxyJump hops may have users, ports, service names and bracketed hosts",
		files:  map[string]string{".ssh/config": "ProxyJump x:https,[::1]:2222,a@b@c,-d;e,j%h,u@jump:22\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles, ProxyJump: "x:https,[::1]:2222,a@b@c,-d;e,jweb1,u@jump:22"},
		noSSH:  "ssh -G prints ProxyJump's tokens unexpanded",
	},
	{
		name:   "ProxyJump hops may have users, ports, service names and bracketed hosts, as ssh -G prints them",
		files:  map[string]string{".ssh/config": "ProxyJump x:https,[::1]:2222,a@b@c,-d;e,u@jump:22\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles, ProxyJump: "x:https,[::1]:2222,a@b@c,-d;e,u@jump:22"},
	},
	{
		name:   "an Include of an unknown user's home matches nothing",
		files:  map[string]string{".ssh/config": "Host other\nInclude ~no-such-moostack-user/config\nHost *\nInclude ~no-such-moostack-user\nUser after\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "after", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "ProxyJump set before a ProxyCommand stays",
		files: map[string]string{".ssh/config": `
ProxyJump bastion
ProxyCommand nc %h %p
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles, ProxyJump: "bastion"},
	},
	{
		name:   "ConnectTimeout goes up to the largest int of seconds",
		files:  map[string]string{".ssh/config": "ConnectTimeout 35791394m7s\n"},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles, ConnectTimeout: 2147483647 * time.Second},
	},
	{
		name: "ConnectTimeout takes OpenSSH time units",
		files: map[string]string{".ssh/config": `
ConnectTimeout 1h2M3s
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles, ConnectTimeout: time.Hour + 2*time.Minute + 3*time.Second},
	},
	{
		name: "Match host matches the hostname set so far, Match originalhost the target",
		files: map[string]string{".ssh/config": `
Host web1
  HostName 10.0.0.1
Match originalhost web1 host 10.0.0.*
  User both
Match host web1
  Port 1000
Match host 10.0.0.1
  Port 2000
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "10.0.0.1", User: "both", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "Match host and originalhost take comma-separated, case-insensitive patterns with negation",
		files: map[string]string{".ssh/config": `
Match host db*,!web2,WEB?
  User listed
Match originalhost *,!web1
  Port 1000
Match originalhost *,!WEB1
  Port 2000
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "listed", Port: 22, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "Match user matches the remote user set so far",
		files: map[string]string{".ssh/config": `
Match user deploy
  User nope
  Port 1000
Host web1
  User admin
Match user adm*,!root
  Port 2000
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "admin", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "Match user sees the target's user",
		files: map[string]string{".ssh/config": `
Match user deploy
  Port 2000
`},
		target: "deploy@web1",
		want:   sshconfig.Settings{HostName: "web1", User: "deploy", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "a criterion prefixed with ! is negated, and Match all always matches",
		files: map[string]string{".ssh/config": `
Match !host web1
  User nope
Match !all
  Port 1000
Match !host db1 originalhost web1
  User negated
Match all
  Port 2000
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "negated", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "a Match criterion may be joined to its patterns with =",
		files: map[string]string{".ssh/config": `
Match originalhost=web1 host=web*
  Port 2000
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "Host after Match starts a new block",
		files: map[string]string{".ssh/config": `
Match host db1
  User nope
Host *
  Port 2000
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "tokens expand in HostName, User and IdentityAgent",
		files: map[string]string{".ssh/config": `
Host web1
  HostName %h.Example.com
  User %u-%h-%p-%%
  Port 2222
  IdentityAgent ~/agents/%r/%h:%p-%u-%d
`},
		target: "web1",
		want: sshconfig.Settings{
			HostName: "web1.example.com", User: "op-web1.example.com-2222-%", Port: 2222, IdentityFiles: defaultIdentityFiles,
			IdentityAgent: "{home}/agents/op-web1.example.com-2222-%/web1.example.com:2222-op-{home}",
		},
	},
	{
		name: "tokens and ~ expand in IdentityFile and ProxyJump",
		files: map[string]string{".ssh/config": `
Host web1
  User admin
  Port 2222
  IdentityFile ~/.ssh/%r@%h-%p
  IdentityFile %d/keys/%u
  ProxyJump jump-%r@%h
`},
		target: "web1",
		want: sshconfig.Settings{
			HostName: "web1", User: "admin", Port: 2222,
			IdentityFiles: []string{"{home}/.ssh/admin@web1-2222", "{home}/keys/op"},
			ProxyJump:     "jump-admin@web1",
		},
		noSSH: "ssh -G prints IdentityFile and ProxyJump unexpanded",
	},
	{
		name: "Match host sees HostName with %h expanded",
		files: map[string]string{".ssh/config": `
Host web1
  HostName %h.example.com
Match host web1.example.com
  Port 2000
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1.example.com", User: "op", Port: 2000, IdentityFiles: defaultIdentityFiles},
	},
	{
		name: "~user is that user's home directory",
		files: map[string]string{".ssh/config": `
IdentityAgent ~root
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles, IdentityAgent: "/root/"},
	},
	{
		name: "the IdentityAgent values that name an environment variable stay as they are",
		files: map[string]string{".ssh/config": `
Host web1
  IdentityAgent $MY_AGENT
Host *
  IdentityAgent SSH_AUTH_SOCK
`},
		target: "web1",
		want:   sshconfig.Settings{HostName: "web1", User: "op", Port: 22, IdentityFiles: defaultIdentityFiles, IdentityAgent: "$MY_AGENT"},
	},
}

// errorCase is a malformed config and the start of the error it gives.
type errorCase struct {
	name string
	// files are written as in resolveCase.
	files map[string]string
	modes map[string]os.FileMode
	// target is "web1" if empty.
	target string
	// wantErr is the start of the error, with "{home}" for the home
	// directory.
	wantErr string
	// sshAccepts says ssh -G accepts the config, and why.
	sshAccepts string
}

var errorCases = []errorCase{
	{
		name:    "a port that is out of range",
		files:   map[string]string{".ssh/config": "Host web1\n  Port 0\n"},
		wantErr: `{home}/.ssh/config:2: bad port "0"`,
	},
	{
		name:    "a port that isn't a number or a service, in a block that doesn't match",
		files:   map[string]string{".ssh/config": "Host db1\n  Port nosuchservice\n"},
		wantErr: `{home}/.ssh/config:2: bad port "nosuchservice"`,
	},
	{
		name:    "an option with two arguments",
		files:   map[string]string{".ssh/config": "\nHostName a b\n"},
		wantErr: `{home}/.ssh/config:2: keyword hostname has extra arguments`,
	},
	{
		name:    "a keyword without an argument",
		files:   map[string]string{".ssh/config": "Host web1\n  User\n"},
		wantErr: `{home}/.ssh/config:2: no argument after keyword "user"`,
	},
	{
		name:    "an empty quoted argument",
		files:   map[string]string{".ssh/config": "Host web1 \"\"\n"},
		wantErr: `{home}/.ssh/config:1: keyword host has an empty argument`,
	},
	{
		name:    "an empty quoted value",
		files:   map[string]string{".ssh/config": "Host web1\n  User ''\n"},
		wantErr: `{home}/.ssh/config:2: keyword user has an empty argument`,
	},
	{
		name:    "an empty quoted Match pattern",
		files:   map[string]string{".ssh/config": "Match host \"\"\n"},
		wantErr: `{home}/.ssh/config:1: keyword match has an empty argument`,
	},
	{
		name:    "an unterminated quote",
		files:   map[string]string{".ssh/config": "IdentityFile \"~/my key\n"},
		wantErr: `{home}/.ssh/config:1: unterminated quote`,
	},
	{
		name:    "a flag that isn't yes or no",
		files:   map[string]string{".ssh/config": "IdentitiesOnly maybe\n"},
		wantErr: `{home}/.ssh/config:1: bad yes/no value "maybe"`,
	},
	{
		name:    "a time with an unknown unit",
		files:   map[string]string{".ssh/config": "ConnectTimeout 5x\n"},
		wantErr: `{home}/.ssh/config:1: bad time value "5x"`,
	},
	{
		name:    "a time past the largest int of seconds",
		files:   map[string]string{".ssh/config": "ConnectTimeout 2147483648\n"},
		wantErr: `{home}/.ssh/config:1: bad time value "2147483648"`,
	},
	{
		name:    "a time whose parts only overflow added up",
		files:   map[string]string{".ssh/config": "ConnectTimeout 1h2147483647s\n"},
		wantErr: `{home}/.ssh/config:1: bad time value "1h2147483647s"`,
	},
	{
		name:    "a time that overflows a 64-bit duration",
		files:   map[string]string{".ssh/config": "ConnectTimeout 9223372037\n"},
		wantErr: `{home}/.ssh/config:1: bad time value "9223372037"`,
	},
	{
		name:    "a time with a unit too large for its number",
		files:   map[string]string{".ssh/config": "ConnectTimeout 3551w\n"},
		wantErr: `{home}/.ssh/config:1: bad time value "3551w"`,
	},
	{
		name:    "an empty time",
		files:   map[string]string{".ssh/config": "ConnectTimeout \"\"\n"},
		wantErr: `{home}/.ssh/config:1: keyword connecttimeout has an empty argument`,
	},
	{
		name: "an included file others can write to",
		files: map[string]string{
			".ssh/config": "Host *\n  Include {home}/inc\n",
			"inc":         "HostName elsewhere\n",
		},
		modes:   map[string]os.FileMode{"inc": 0o602},
		wantErr: `{home}/.ssh/config:2: bad owner or permissions on {home}/inc`,
	},
	{
		name: "an included file its group can write to, through a symlink",
		files: map[string]string{
			".ssh/config": "Include {home}/link\n",
			"inc":         "HostName elsewhere\n",
			"link":        "-> {home}/inc",
		},
		modes:   map[string]os.FileMode{"inc": 0o620},
		wantErr: `{home}/.ssh/config:1: bad owner or permissions on {home}/link`,
	},
	{
		name: "a directory an Include glob matches, which others can write to",
		files: map[string]string{
			".ssh/config":     "Include {home}/conf.d/*\n",
			"conf.d/sub/file": "User sub\n",
		},
		modes:   map[string]os.FileMode{"conf.d/sub": 0o777},
		wantErr: `{home}/.ssh/config:1: bad owner or permissions on {home}/conf.d/sub`,
	},
	{
		name:       "a ~ Include path in the system config",
		files:      map[string]string{"etc/ssh/ssh_config": "\nInclude ~/extra\n"},
		wantErr:    `{home}/etc/ssh/ssh_config:2: bad Include path "~/extra": ~ is only allowed in the user config`,
		sshAccepts: "ssh -F doesn't read the system config",
	},
	{
		name:       "an ssh:// target",
		target:     "ssh://bob@web1:2222",
		wantErr:    `target "ssh://bob@web1:2222": ssh:// targets aren't supported`,
		sshAccepts: "ssh supports ssh:// targets",
	},
	{
		name:       "an environment variable in an Include path",
		files:      map[string]string{".ssh/config": "Include ${HOME}/inc\n"},
		wantErr:    `{home}/.ssh/config:1: Include path "${HOME}/inc" has a token or environment variable, which isn't supported`,
		sshAccepts: "ssh expands environment variables in Include",
	},
	{
		name:       "a token in an Include path",
		files:      map[string]string{".ssh/config": "\nInclude {home}/%h.conf\n"},
		wantErr:    `{home}/.ssh/config:2: Include path "{home}/%h.conf" has a token or environment variable, which isn't supported`,
		sshAccepts: "ssh expands tokens in Include",
	},
	{
		name:       "a quoted ProxyCommand none, which is a command",
		files:      map[string]string{".ssh/config": "ProxyCommand \"none\"\nProxyJump j\n"},
		wantErr:    `{home}/.ssh/config:1: ProxyCommand isn't supported`,
		sshAccepts: "ssh supports ProxyCommand",
	},
	{
		name:       "a ProxyCommand none with a comment, which is a command",
		files:      map[string]string{".ssh/config": "ProxyCommand none # c\nProxyJump j\n"},
		wantErr:    `{home}/.ssh/config:1: ProxyCommand isn't supported`,
		sshAccepts: "ssh supports ProxyCommand",
	},
	{
		name:    "a ProxyJump port that isn't a number or a service",
		files:   map[string]string{".ssh/config": "ProxyJump jump:nosuchservice\n"},
		wantErr: `{home}/.ssh/config:1: bad ProxyJump "jump:nosuchservice"`,
	},
	{
		name:    "a ProxyJump port out of range",
		files:   map[string]string{".ssh/config": "ProxyJump jump:0\n"},
		wantErr: `{home}/.ssh/config:1: bad ProxyJump "jump:0"`,
	},
	{
		name:    "a ProxyJump with an empty hop",
		files:   map[string]string{".ssh/config": "ProxyJump jump,,other\n"},
		wantErr: `{home}/.ssh/config:1: bad ProxyJump "jump,,other"`,
	},
	{
		name:    "a ProxyJump starting with a comma",
		files:   map[string]string{".ssh/config": "ProxyJump ,jump\n"},
		wantErr: `{home}/.ssh/config:1: bad ProxyJump ",jump"`,
	},
	{
		name:    "a ProxyJump ending with a comma",
		files:   map[string]string{".ssh/config": "ProxyJump jump,\n"},
		wantErr: `{home}/.ssh/config:1: bad ProxyJump "jump,"`,
	},
	{
		name:    "a ProxyJump hop with an empty host",
		files:   map[string]string{".ssh/config": "ProxyJump u@\n"},
		wantErr: `{home}/.ssh/config:1: bad ProxyJump "u@"`,
	},
	{
		name:    "a ProxyJump hop with an empty user",
		files:   map[string]string{".ssh/config": "ProxyJump @jump\n"},
		wantErr: `{home}/.ssh/config:1: bad ProxyJump "@jump"`,
	},
	{
		name:    "a ProxyJump hop with an unbracketed IPv6 address",
		files:   map[string]string{".ssh/config": "ProxyJump ::1\n"},
		wantErr: `{home}/.ssh/config:1: bad ProxyJump "::1"`,
	},
	{
		name:    "a ProxyJump whose port is a token",
		files:   map[string]string{".ssh/config": "ProxyJump jump-%r@%h:%p\n"},
		wantErr: `{home}/.ssh/config:1: bad ProxyJump "jump-%r@%h:%p"`,
	},
	{
		name:    "a malformed ProxyJump in a block that doesn't match",
		files:   map[string]string{".ssh/config": "Host other\n  ProxyJump jump:abc\n"},
		wantErr: `{home}/.ssh/config:2: bad ProxyJump "jump:abc"`,
	},
	{
		name:    "a malformed ProxyJump after a proxy is set",
		files:   map[string]string{".ssh/config": "ProxyJump a\nProxyJump b:abc\n"},
		wantErr: `{home}/.ssh/config:2: bad ProxyJump "b:abc"`,
	},
	{
		name:       "an ssh:// ProxyJump hop",
		files:      map[string]string{".ssh/config": "ProxyJump a,ssh://u@jump:22\n"},
		wantErr:    `{home}/.ssh/config:1: bad ProxyJump "a,ssh://u@jump:22": ssh:// jump hosts aren't supported`,
		sshAccepts: "ssh supports ssh:// jump hosts",
	},
	{
		name:    "a ProxyJump that is only a comment",
		files:   map[string]string{".ssh/config": "ProxyJump #c\n"},
		wantErr: `{home}/.ssh/config:1: bad ProxyJump "#c"`,
	},
	{
		name:       "a user config others can write to",
		files:      map[string]string{".ssh/config": "User me\n"},
		modes:      map[string]os.FileMode{".ssh/config": 0o666},
		wantErr:    `bad owner or permissions on {home}/.ssh/config`,
		sshAccepts: "ssh -F doesn't check the file's permissions",
	},
	{
		name:    "Match all followed by another criterion",
		files:   map[string]string{".ssh/config": "Match host web1 all user op\n"},
		wantErr: `{home}/.ssh/config:1: Match all can't be combined with other criteria`,
	},
	{
		name:    "Match all twice",
		files:   map[string]string{".ssh/config": "Match all all\n"},
		wantErr: `{home}/.ssh/config:1: Match all can't be combined with other criteria`,
	},
	{
		name:    "a Match criterion with = and no patterns",
		files:   map[string]string{".ssh/config": "Match host=\n"},
		wantErr: `{home}/.ssh/config:1: missing argument for Match host`,
	},
	{
		name:    "a target with an empty user",
		target:  "@web1",
		wantErr: `target "@web1": empty user`,
	},
	{
		name:       "a target with an empty host",
		target:     "bob@",
		wantErr:    `target "bob@": empty host`,
		sshAccepts: "ssh -G prints an empty hostname, which can't be connected to",
	},
	{
		name:    "a target starting with -",
		target:  "-web1",
		wantErr: `target "-web1": user or host starts with -`,
	},
	{
		name:    "a target host starting with -",
		target:  "bob@-web1",
		wantErr: `target "bob@-web1": user or host starts with -`,
	},
	{
		name:    "a target user with a shell character",
		target:  "a'b@web1",
		wantErr: `target "a'b@web1": the user has a character OpenSSH refuses`,
	},
	{
		name:    "a target user with - after a space",
		target:  "a -b@web1",
		wantErr: `target "a -b@web1": the user has a character OpenSSH refuses`,
	},
	{
		name:    "a target user ending in a backslash",
		target:  `a\@web1`,
		wantErr: `target "a\\@web1": the user has a character OpenSSH refuses`,
	},
	{
		name:    "a target host with a shell character",
		target:  "web1;x",
		wantErr: `target "web1;x": the host has a character OpenSSH refuses`,
	},
	{
		name:    "a target host with a space",
		target:  "bob@web 1",
		wantErr: `target "bob@web 1": the host has a character OpenSSH refuses`,
	},
	{
		name:       "a Match criterion Resolve doesn't support",
		files:      map[string]string{".ssh/config": "Host *\n\nMatch host web1 exec \"true\"\n  Port 2000\n"},
		wantErr:    `{home}/.ssh/config:3: unsupported Match criterion "exec"`,
		sshAccepts: "ssh supports exec",
	},
	{
		name:       "a ProxyCommand that applies",
		files:      map[string]string{".ssh/config": "Host web1\n  ProxyCommand nc %h %p\nProxyJump bastion\n"},
		wantErr:    `{home}/.ssh/config:2: ProxyCommand isn't supported`,
		sshAccepts: "ssh supports ProxyCommand",
	},
	{
		name:    "Match all with another criterion",
		files:   map[string]string{".ssh/config": "Match all host web1\n"},
		wantErr: `{home}/.ssh/config:1: Match all can't be combined with other criteria`,
	},
	{
		name:    "a Match criterion without its patterns",
		files:   map[string]string{".ssh/config": "Match host\n"},
		wantErr: `{home}/.ssh/config:1: missing argument for Match host`,
	},
	{
		name: "an error in an included file names that file",
		files: map[string]string{
			".ssh/config":   "Host *\n  Include {home}/conf.d/*\n",
			"conf.d/a.conf": "Host web1\n  Port 0\n",
		},
		wantErr: `{home}/conf.d/a.conf:2: bad port "0"`,
	},
	{
		name:    "a file that includes itself",
		files:   map[string]string{".ssh/config": "Include {home}/.ssh/config\n"},
		wantErr: `{home}/.ssh/config:1: too many nested Include levels`,
	},
	{
		name:       "a token OpenSSH doesn't know",
		files:      map[string]string{".ssh/config": "Host web1\n  IdentityFile ~/.ssh/%z\n"},
		wantErr:    `{home}/.ssh/config:2: IdentityFile has an unknown token %z`,
		sshAccepts: "ssh -G doesn't expand IdentityFile",
	},
	{
		name:    "the remote user in User",
		files:   map[string]string{".ssh/config": "User %r\n"},
		wantErr: `{home}/.ssh/config:1: User has an unknown token %r`,
	},
	{
		name:    "a port in HostName",
		files:   map[string]string{".ssh/config": "HostName %h-%p\n"},
		wantErr: `{home}/.ssh/config:1: HostName has an unknown token %p`,
	},
	{
		name:       "the local user in ProxyJump",
		files:      map[string]string{".ssh/config": "ProxyJump %u@jump\n"},
		wantErr:    `{home}/.ssh/config:1: ProxyJump has an unknown token %u`,
		sshAccepts: "ssh -G doesn't expand ProxyJump",
	},
	{
		name:       "an environment variable in a path",
		files:      map[string]string{".ssh/config": "IdentityAgent ${XDG_RUNTIME_DIR}/agent\n"},
		wantErr:    `{home}/.ssh/config:1: IdentityAgent uses an environment variable, which isn't supported`,
		sshAccepts: "ssh supports environment variables",
	},
}

func TestResolveErrors(t *testing.T) {
	for _, c := range errorCases {
		t.Run(c.name, func(t *testing.T) {
			home := t.TempDir()
			writeFiles(t, home, c.files, c.modes)
			_, err := sshconfig.Resolve(cmp.Or(c.target, "web1"), sshconfig.Options{
				UserConfig:   filepath.Join(home, ".ssh", "config"),
				SystemConfig: filepath.Join(home, "etc", "ssh", "ssh_config"),
				LocalUser:    "op",
				Home:         home,
			})
			want := strings.ReplaceAll(c.wantErr, "{home}", home)
			if err == nil || !strings.HasPrefix(err.Error(), want) {
				t.Fatalf("got error %v, want one starting %q", err, want)
			}
		})
	}
}

// TestErrorsMatchOpenSSH checks that ssh -G rejects the malformed configs
// too, unless the case says why it doesn't.
func TestErrorsMatchOpenSSH(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("ssh is not installed")
	}
	for _, c := range errorCases {
		t.Run(c.name, func(t *testing.T) {
			if c.sshAccepts != "" {
				t.Skip(c.sshAccepts)
			}
			dir := t.TempDir()
			writeFiles(t, dir, c.files, c.modes)
			config := filepath.Join(dir, ".ssh", "config")
			if _, err := os.Stat(config); err != nil {
				config = os.DevNull
			}
			out, err := exec.Command("ssh", "-G", "-F", config, "--", cmp.Or(c.target, "web1")).CombinedOutput()
			if err == nil {
				t.Errorf("ssh -G accepted the config:\n%s", out)
			}
		})
	}
}

// globFiles are names to try Include globs on.
var globFiles = map[string]string{
	"g/x":      "User from_x\n",
	"g/y":      "User from_y\n",
	"g/[z":     "User from_br\n",
	"g/[a":     "User from_br_a\n",
	"g/[]a":    "User from_br_close_a\n",
	"g/[]":     "User from_br_close\n",
	"g/]":      "User from_close\n",
	"g/!":      "User from_bang\n",
	"g/*":      "User from_star\n",
	"g/B":      "User from_B\n",
	"g/b":      "User from_b\n",
	"g/ab":     "User from_ab\n",
	"g/sub/f1": "User from_sub\n",
	"g/.hd/f":  "User from_hidden_dir\n",
	"g/link":   "-> {home}/g/sub",
}

// withConfig is files with a user config added.
func withConfig(files map[string]string, config string) map[string]string {
	files = maps.Clone(files)
	files[".ssh/config"] = config
	return files
}

// writeFiles writes files under home, then sets the modes of files and
// directories. A file whose content is "-> target" is a symlink to target.
func writeFiles(t *testing.T, home string, files map[string]string, modes map[string]os.FileMode) {
	t.Helper()
	for name, content := range files {
		path := filepath.Join(home, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		content = strings.ReplaceAll(content, "{home}", home)
		if target, ok := strings.CutPrefix(content, "-> "); ok {
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for name, mode := range modes {
		// Chmod, unlike WriteFile and MkdirAll, isn't limited by the umask.
		if err := os.Chmod(filepath.Join(home, name), mode); err != nil {
			t.Fatal(err)
		}
	}
}

func withHome(s sshconfig.Settings, home string) sshconfig.Settings {
	r := strings.NewReplacer("{home}", home)
	s.HostName = r.Replace(s.HostName)
	s.User = r.Replace(s.User)
	s.IdentityAgent = r.Replace(s.IdentityAgent)
	s.ProxyJump = r.Replace(s.ProxyJump)
	var files []string
	for _, f := range s.IdentityFiles {
		files = append(files, r.Replace(f))
	}
	s.IdentityFiles = files
	return s
}

func TestResolve(t *testing.T) {
	for _, c := range resolveCases {
		t.Run(c.name, func(t *testing.T) {
			home := t.TempDir()
			writeFiles(t, home, c.files, c.modes)
			got, err := sshconfig.Resolve(c.target, sshconfig.Options{
				UserConfig:   filepath.Join(home, ".ssh", "config"),
				SystemConfig: filepath.Join(home, "etc", "ssh", "ssh_config"),
				LocalUser:    "op",
				Home:         home,
			})
			if err != nil {
				t.Fatal(err)
			}
			if want := withHome(c.want, home); !reflect.DeepEqual(got, want) {
				t.Errorf("got  %+v\nwant %+v", got, want)
			}
		})
	}
}

// TestManyWildcardsResolveQuickly checks that Host and Include patterns
// with many stars match in polynomial time: tried split by split, each of
// these takes about 2^20 steps.
func TestManyWildcardsResolveQuickly(t *testing.T) {
	home := t.TempDir()
	stars := strings.Repeat("*", 20)
	long := strings.Repeat("a", 30)
	writeFiles(t, home, map[string]string{
		".ssh/config": "Host " + stars + "b\n  User nope\nInclude {home}/inc/" + stars + "b\n",
		"inc/" + long: "User nope\n",
	}, nil)
	done := make(chan error, 1)
	go func() {
		s, err := sshconfig.Resolve(long, sshconfig.Options{UserConfig: filepath.Join(home, ".ssh", "config"), LocalUser: "op", Home: home})
		if err == nil && s.User != "op" {
			err = fmt.Errorf("got user %q, want op", s.User)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Resolve took over 5s")
	}
}

// TestResolveMatchesOpenSSH resolves every case of TestResolve as the user
// running the test and compares the result with what ssh -G prints for the
// same config.
func TestResolveMatchesOpenSSH(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("ssh is not installed")
	}
	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range resolveCases {
		t.Run(c.name, func(t *testing.T) {
			if c.noSSH != "" {
				t.Skip(c.noSSH)
			}
			dir := t.TempDir()
			writeFiles(t, dir, c.files, c.modes)
			config := filepath.Join(dir, ".ssh", "config")
			if _, err := os.Stat(config); err != nil {
				config = os.DevNull
			}
			want := sshG(t, config, c.target, me.HomeDir)
			if config == os.DevNull {
				// /dev/null is writable by anyone, so Resolve would refuse it.
				config = ""
			}
			got, err := sshconfig.Resolve(c.target, sshconfig.Options{
				UserConfig: config,
				LocalUser:  me.Username,
				Home:       me.HomeDir,
			})
			if err != nil {
				t.Fatal(err)
			}
			if c.files[".ssh/config"] == "" || !strings.Contains(c.files[".ssh/config"], "IdentityFile") {
				// OpenSSH versions differ in their default identity files,
				// so ours only have to be among them, in the same order.
				if !isSubsequence(got.IdentityFiles, want.IdentityFiles) {
					t.Errorf("default identity files %q are not among ssh's %q", got.IdentityFiles, want.IdentityFiles)
				}
				got.IdentityFiles, want.IdentityFiles = nil, nil
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("got     %+v\nssh -G %+v", got, want)
			}
		})
	}
}

// sshG runs ssh -G and reads back the settings that Resolve resolves. It
// expands "~/" in identity files, which ssh -G prints unexpanded.
func sshG(t *testing.T, config, target, home string) sshconfig.Settings {
	t.Helper()
	out, err := exec.Command("ssh", "-G", "-F", config, target).Output()
	if err != nil {
		t.Fatalf("ssh -G: %v", err)
	}
	var s sshconfig.Settings
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		key, value, _ := strings.Cut(sc.Text(), " ")
		switch key {
		case "hostname":
			s.HostName = value
		case "user":
			s.User = value
		case "port":
			s.Port, err = strconv.Atoi(value)
			if err != nil {
				t.Fatal(err)
			}
		case "identityfile":
			if rest, ok := strings.CutPrefix(value, "~/"); ok {
				value = filepath.Join(home, rest)
			}
			s.IdentityFiles = append(s.IdentityFiles, value)
		case "identitiesonly":
			s.IdentitiesOnly = value == "yes"
		case "identityagent":
			s.IdentityAgent = value
		case "proxyjump":
			s.ProxyJump = value
		case "connecttimeout":
			if value != "none" {
				n, err := strconv.Atoi(value)
				if err != nil {
					t.Fatal(err)
				}
				s.ConnectTimeout = time.Duration(n) * time.Second
			}
		}
	}
	return s
}

func isSubsequence(sub, of []string) bool {
	i := 0
	for _, s := range of {
		if i < len(sub) && sub[i] == s {
			i++
		}
	}
	return i == len(sub)
}
