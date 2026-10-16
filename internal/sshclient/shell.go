package sshclient

import (
	"errors"
	"strings"
)

// shellQuote quotes s as one word for the shell.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// errShellSpecial is the error for a word that a shell would expand,
// split or read differently from other shells.
var errShellSpecial = errors.New("a shell expansion isn't supported")

// shellSpecial are the characters that make a shell expand or split a
// word, or treat it as more than a word.
const shellSpecial = " \t\n;&|<>()$`*?[]{}~#\\!^="

// shellWord decodes s as a shell reads a single word, removing single and
// double quotes. OpenSSH uses the operator's $SHELL, so anything that
// shells read differently, or that a shell would expand, split or treat
// specially, isn't supported, rather than being guessed at: that includes
// every backslash.
func shellWord(s string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\'' || c == '"':
			end := strings.IndexByte(s[i+1:], c)
			if end < 0 {
				return "", errors.New("unclosed quote")
			}
			quoted := s[i+1 : i+1+end]
			if strings.Contains(quoted, "\\") || c == '"' && strings.ContainsAny(quoted, "$`") {
				return "", errShellSpecial
			}
			b.WriteString(quoted)
			i += end + 1
		case strings.IndexByte(shellSpecial, c) >= 0:
			return "", errShellSpecial
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), nil
}
