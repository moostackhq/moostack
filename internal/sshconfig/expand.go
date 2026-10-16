package sshconfig

import (
	"errors"
	"fmt"
	"os/user"
	"strings"
)

// expand replaces the %-tokens in s. tokens are the ones keyword takes,
// besides %%, and any other is an error, as in OpenSSH.
func expand(s, keyword string, tokens map[byte]string) (string, error) {
	if strings.Contains(s, "${") {
		return "", fmt.Errorf("%s uses an environment variable, which isn't supported", keyword)
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			b.WriteByte(s[i])
			continue
		}
		i++
		if i == len(s) {
			return "", fmt.Errorf("%s ends in a lone %%", keyword)
		}
		if s[i] == '%' {
			b.WriteByte('%')
			continue
		}
		value, ok := tokens[s[i]]
		if !ok {
			return "", fmt.Errorf("%s has an unknown token %%%c", keyword, s[i])
		}
		b.WriteString(value)
	}
	return b.String(), nil
}

// expandTilde replaces a leading "~" or "~/" with the local user's home
// directory and a slash, and a leading "~name" or "~name/" with name's.
func (r *resolver) expandTilde(path string) (string, error) {
	rest, ok := strings.CutPrefix(path, "~")
	if !ok {
		return path, nil
	}
	name, rest, _ := strings.Cut(rest, "/")
	home := r.opts.Home
	if name != "" {
		u, err := user.Lookup(name)
		if err != nil {
			return "", errors.New("unknown user in " + path)
		}
		home = u.HomeDir
	}
	// OpenSSH puts a slash after the home directory even when nothing
	// follows it.
	return strings.TrimSuffix(home, "/") + "/" + rest, nil
}
