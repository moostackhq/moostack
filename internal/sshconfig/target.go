package sshconfig

import (
	"errors"
	"fmt"
	"strings"
)

// target is what a target names: a host, and the user if it gives one.
type target struct {
	user *string
	host string
}

// parseTarget parses user@host or host, and refuses users and hosts with
// the characters ssh refuses. ssh:// targets aren't supported.
func parseTarget(s string) (target, error) {
	t, err := splitTarget(s)
	if err == nil && (strings.HasPrefix(s, "-") || strings.HasPrefix(t.host, "-")) {
		err = errors.New("user or host starts with -")
	}
	if err == nil && t.user != nil && !validUser(*t.user) {
		err = errors.New("the user has a character OpenSSH refuses")
	}
	if err == nil && !validHost(t.host) {
		err = errors.New("the host has a character OpenSSH refuses")
	}
	if err != nil {
		return target{}, fmt.Errorf("target %q: %w", s, err)
	}
	return t, nil
}

func splitTarget(s string) (target, error) {
	if strings.HasPrefix(s, "ssh://") {
		return target{}, errors.New("ssh:// targets aren't supported")
	}
	t := target{host: s}
	if i := strings.LastIndex(s, "@"); i >= 0 {
		user := s[:i]
		if user == "" {
			return target{}, errors.New("empty user")
		}
		t.user, t.host = &user, s[i+1:]
	}
	if t.host == "" {
		return target{}, errors.New("empty host")
	}
	return t, nil
}

// validUser reports whether ssh would take user from a target: no shell
// metacharacters, no "-" after whitespace and no trailing backslash.
func validUser(user string) bool {
	for i := 0; i < len(user); i++ {
		c := user[i]
		if strings.IndexByte("'`\";&<>|(){}", c) >= 0 {
			return false
		}
		if isSpace(c) && i+1 < len(user) && user[i+1] == '-' {
			return false
		}
		if c == '\\' && i+1 == len(user) {
			return false
		}
	}
	return true
}

// validHost reports whether ssh would take host from a target: no shell
// metacharacters, commas, whitespace or control characters.
func validHost(host string) bool {
	for i := 0; i < len(host); i++ {
		c := host[i]
		if strings.IndexByte("'`\"$\\;&<>|(){},", c) >= 0 || isSpace(c) || c < 0x20 || c == 0x7f {
			return false
		}
	}
	return true
}

func isSpace(c byte) bool { return c == ' ' || '\t' <= c && c <= '\r' }
