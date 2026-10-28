package sshconfig

import (
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

// applyLine applies one line of f.
func (r *resolver) applyLine(f *file, line string) error {
	keyword, rest := splitKeyword(line)
	if keyword == "" || strings.HasPrefix(keyword, "#") {
		return nil
	}
	keyword = strings.ToLower(keyword)
	if rest == "" {
		return fmt.Errorf("no argument after keyword %q", keyword)
	}
	args, err := splitArgs(rest)
	if err != nil {
		return err
	}
	if keyword == "proxycommand" || keyword == "proxyjump" {
		return r.setProxy(f, keyword, rest)
	}
	if len(args) == 0 {
		return fmt.Errorf("no argument after keyword %q", keyword)
	}
	if !parsed[keyword] {
		// Settings doesn't depend on this keyword.
		return nil
	}
	if slices.Contains(args, "") {
		return fmt.Errorf("keyword %s has an empty argument", keyword)
	}

	switch keyword {
	case "host":
		f.active = f.enabled && matchPatterns(r.host, args)
		return nil
	case "match":
		matched, err := r.match(args)
		f.active = f.enabled && matched
		return err
	case "include":
		return r.include(f, args)
	}
	if len(args) > 1 {
		return fmt.Errorf("keyword %s has extra arguments at end of line", keyword)
	}
	arg := args[0]
	switch keyword {
	case "identityfile":
		if f.active && !slices.ContainsFunc(r.identityFiles, func(l located) bool { return l.value == arg }) {
			r.identityFiles = append(r.identityFiles, located{arg, f.at()})
		}
	case "hostname":
		if f.active && r.hostName == nil {
			// HostName only takes %h, which is the target's host, so it is
			// expanded straight away for Match host to see.
			name, err := expand(arg, "HostName", map[byte]string{'h': r.host})
			if err != nil {
				return err
			}
			r.hostName = &name
		}
	case "user":
		setOnce(f.active, &r.user, located{arg, f.at()})
	case "identityagent":
		setOnce(f.active, &r.identityAgent, located{arg, f.at()})
	case "port":
		port, err := parsePort(arg)
		if err != nil {
			return err
		}
		setOnce(f.active, &r.port, port)
	case "identitiesonly":
		var on bool
		switch strings.ToLower(arg) {
		case "yes", "true":
			on = true
		case "no", "false":
		default:
			return fmt.Errorf("bad yes/no value %q", arg)
		}
		setOnce(f.active, &r.identitiesOnly, on)
	case "connecttimeout":
		if arg == "none" {
			// OpenSSH takes none as not set, so a later value still applies.
			return nil
		}
		d, err := parseTime(arg)
		if err != nil {
			return err
		}
		setOnce(f.active, &r.connectTimeout, d)
	}
	return nil
}

// parsed are the keywords Settings depends on, besides ProxyCommand and
// ProxyJump. Each option among them takes one argument.
var parsed = map[string]bool{
	"host": true, "match": true, "include": true,
	"hostname": true, "user": true, "port": true, "identityfile": true, "identitiesonly": true,
	"identityagent": true, "connecttimeout": true,
}

// setProxy applies a ProxyCommand or ProxyJump line, whose rest is
// everything after the keyword. The two share one setting, so the first of
// them wins. OpenSSH takes both from the raw rest, quotes and backslashes
// included, after any run of whitespace and "=", and either disables the
// proxy if that is none in any case. A ProxyJump ends at its first "#",
// and is checked whether it applies or not, as OpenSSH checks it.
func (r *resolver) setProxy(f *file, keyword, rest string) error {
	raw := strings.TrimLeft(rest, lineSpace+"=")
	p := proxy{located: located{at: f.at()}}
	switch {
	case strings.EqualFold(raw, "none"):
	case keyword == "proxycommand":
		p.command = true
	default:
		jump, _, _ := strings.Cut(raw, "#")
		p.value = strings.TrimRight(jump, lineSpace)
		if err := checkJump(p.value); err != nil {
			return fmt.Errorf("bad ProxyJump %q: %w", raw, err)
		}
	}
	if f.active && r.proxy == nil {
		r.proxy = &p
	}
	return nil
}

// checkJump checks ProxyJump's hops before their tokens are expanded, as
// OpenSSH does: a comma-separated list of [user@]host[:port], where a host
// with colons is bracketed, the port may be empty and no host may be.
// ssh:// hops aren't supported.
func checkJump(jump string) error {
	for _, hop := range strings.Split(jump, ",") {
		if strings.HasPrefix(hop, "ssh://") {
			return errors.New("ssh:// jump hosts aren't supported")
		}
		hostPort := hop
		if i := strings.LastIndex(hop, "@"); i >= 0 {
			if i == 0 {
				return fmt.Errorf("jump host %q has an empty user", hop)
			}
			hostPort = hop[i+1:]
		}
		host, port, _ := strings.Cut(hostPort, ":")
		if bracketed, ok := strings.CutPrefix(hostPort, "["); ok {
			var after string
			if host, after, ok = strings.Cut(bracketed, "]"); !ok {
				return fmt.Errorf("jump host %q has an unclosed [", hop)
			}
			if port, ok = strings.CutPrefix(after, ":"); !ok && after != "" {
				return fmt.Errorf("jump host %q has characters after its ]", hop)
			}
		}
		if host == "" {
			return fmt.Errorf("jump host %q has an empty host", hop)
		}
		if port != "" {
			if _, err := parsePort(port); err != nil {
				return err
			}
		}
	}
	return nil
}

// setOnce sets *v to value if the block is active and *v isn't set yet.
func setOnce[T any](active bool, v **T, value T) {
	if active && *v == nil {
		*v = &value
	}
}

// parsePort parses a port number or a TCP service name.
func parsePort(s string) (int, error) {
	port, err := strconv.Atoi(s)
	if err != nil {
		port, err = net.LookupPort("tcp", s)
	}
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("bad port %q", s)
	}
	return port, nil
}

// splitKeyword splits a line into its keyword and the rest the way
// OpenSSH does, after dropping trailing whitespace and form feeds. A line
// whose first token is empty, as when it starts with whitespace or "=",
// takes its keyword from the next token. An empty keyword means the line
// is ignored.
func splitKeyword(line string) (keyword, rest string) {
	line = strings.TrimRight(line, lineSpace+"\f")
	keyword, rest, ok := strdelim(line)
	if ok && keyword == "" {
		keyword, rest, ok = strdelim(rest)
	}
	if !ok {
		return "", ""
	}
	return keyword, rest
}

// strdelim takes the first token off s, as OpenSSH's strdelim does. The
// token ends at whitespace, "=" or a double quote. At a quote, the token
// goes on to the next quote, with both dropped, and ok is false if there
// isn't one. The rest starts after the whitespace that follows, and after
// one "=" with whitespace around it if the token ended at whitespace.
func strdelim(s string) (token, rest string, ok bool) {
	end := strings.IndexAny(s, lineSpace+`"=`)
	if end < 0 {
		return s, "", true
	}
	if s[end] == '"' {
		quoted, after, ok := strings.Cut(s[end+1:], `"`)
		if !ok {
			return "", "", false
		}
		return s[:end] + quoted, strings.TrimLeft(after, lineSpace), true
	}
	token, rest = s[:end], strings.TrimLeft(s[end+1:], lineSpace)
	if s[end] != '=' {
		if r, ok := strings.CutPrefix(rest, "="); ok {
			rest = strings.TrimLeft(r, lineSpace)
		}
	}
	return token, rest, true
}

// lineSpace is the whitespace around keywords. Arguments are split at
// spaces and tabs only.
const lineSpace = " \t\r\n"

// splitArgs splits a line's arguments the way OpenSSH does: at spaces and
// tabs outside quotes, with a backslash escaping a backslash, a quote, or a space
// outside quotes, and a "#" at the start of an argument starting a comment.
func splitArgs(s string) ([]string, error) {
	var args []string
	var cur strings.Builder
	inArg := false
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s) && (strings.IndexByte(`\"'`, s[i+1]) >= 0 || quote == 0 && s[i+1] == ' '):
			i++
			cur.WriteByte(s[i])
			inArg = true
		case quote != 0:
			if c == quote {
				quote = 0
			} else {
				cur.WriteByte(c)
			}
		case c == '"' || c == '\'':
			quote = c
			inArg = true
		case c == ' ' || c == '\t':
			if inArg {
				args = append(args, cur.String())
				cur.Reset()
				inArg = false
			}
		case c == '#' && !inArg:
			return args, nil
		default:
			cur.WriteByte(c)
			inArg = true
		}
	}
	if quote != 0 {
		return nil, errors.New("unterminated quote")
	}
	if inArg {
		args = append(args, cur.String())
	}
	return args, nil
}

// parseTime parses a time the way OpenSSH's ConnectTimeout takes it: a
// number of seconds, or a sequence of numbers each followed by s, m, h, d
// or w, in either case, adding up to at most the largest int of seconds.
func parseTime(s string) (time.Duration, error) {
	bad := fmt.Errorf("bad time value %q", s)
	var total int64
	for rest := s; rest != ""; {
		end := strings.IndexFunc(rest, func(c rune) bool { return c < '0' || c > '9' })
		if end == 0 {
			return 0, bad
		}
		if end < 0 {
			end = len(rest)
		}
		n, err := strconv.ParseInt(rest[:end], 10, 64)
		if err != nil {
			return 0, bad
		}
		unit := int64(1)
		rest = rest[end:]
		if rest != "" {
			u, ok := timeUnits[strings.ToLower(rest[:1])[0]]
			if !ok {
				return 0, bad
			}
			unit, rest = u, rest[1:]
		}
		// Checked before multiplying and adding, so neither overflows.
		if n > (maxTime-total)/unit {
			return 0, bad
		}
		total += n * unit
	}
	return time.Duration(total) * time.Second, nil
}

// timeUnits are the seconds in each of OpenSSH's time units.
var timeUnits = map[byte]int64{'s': 1, 'm': 60, 'h': 60 * 60, 'd': 24 * 60 * 60, 'w': 7 * 24 * 60 * 60}

// maxTime is the most seconds OpenSSH takes, the largest C int.
const maxTime = math.MaxInt32
