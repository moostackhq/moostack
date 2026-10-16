package sshconfig

import (
	"cmp"
	"os"
	"slices"
	"strings"
)

// glob returns the paths matching pattern the way glob(3), which OpenSSH
// uses for Include, finds them: sorted by byte order across the whole list,
// with "[!...]" negating a bracket, a backslash escaping the next character,
// an escaped slash still separating names, and a leading dot in a name,
// "." and ".." included, matching only a dot in the pattern.
// filepath.Glob differs in each of these.
func glob(pattern string) []string {
	var matches []string
	var walk func(dir string, parts []string)
	walk = func(dir string, parts []string) {
		if len(parts) == 0 {
			if _, err := os.Lstat(dir); err == nil {
				matches = append(matches, dir)
			}
			return
		}
		part, rest := parts[0], parts[1:]
		if !hasWildcard(part) {
			walk(joinPath(dir, unescape(part)), rest)
			return
		}
		entries, err := os.ReadDir(cmp.Or(dir, "."))
		if err != nil {
			return
		}
		// readdir(3), which glob(3) reads with, lists . and .., and
		// os.ReadDir doesn't.
		names := []string{".", ".."}
		for _, e := range entries {
			names = append(names, e.Name())
		}
		for _, name := range names {
			if matchName(part, name) {
				walk(joinPath(dir, name), rest)
			}
		}
	}
	parts := splitPattern(pattern)
	if parts[0] == "" {
		walk("/", parts[1:])
	} else {
		walk("", parts)
	}
	slices.Sort(matches)
	return matches
}

func joinPath(dir, name string) string {
	switch {
	case dir == "":
		return name
	case strings.HasSuffix(dir, "/"):
		return dir + name
	}
	return dir + "/" + name
}

// splitPattern splits pattern into names at each slash, escaped or not.
func splitPattern(pattern string) []string {
	var parts []string
	var part strings.Builder
	for i := 0; i < len(pattern); i++ {
		switch {
		case pattern[i] == '\\' && i+1 < len(pattern) && pattern[i+1] == '/':
			// The slash is the next character, and is handled there.
		case pattern[i] == '\\' && i+1 < len(pattern):
			part.WriteString(pattern[i : i+2])
			i++
		case pattern[i] == '/':
			parts = append(parts, part.String())
			part.Reset()
		default:
			part.WriteByte(pattern[i])
		}
	}
	return append(parts, part.String())
}

// hasWildcard reports whether a pattern has an unescaped *, ? or [.
func hasWildcard(pattern string) bool {
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			i++
		case '*', '?', '[':
			return true
		}
	}
	return false
}

// unescape drops the backslashes that escape characters in a pattern.
func unescape(pattern string) string {
	var b strings.Builder
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '\\' && i+1 < len(pattern) {
			i++
		}
		b.WriteByte(pattern[i])
	}
	return b.String()
}

// matchName reports whether name matches pattern. A leading dot in name
// only matches a dot, escaped or not, at the start of pattern.
func matchName(pattern, name string) bool {
	if strings.HasPrefix(name, ".") && !strings.HasPrefix(pattern, ".") && !strings.HasPrefix(pattern, `\.`) {
		return false
	}
	return fnmatch(pattern, name)
}

// fnmatch reports whether s matches pattern, in which * matches any run of
// characters, ? any one character, [...] one of a set and \ escapes the
// next character.
func fnmatch(pattern, s string) bool {
	return matchStars(pattern, s, func(pattern string, c byte) (int, bool) {
		switch pattern[0] {
		case '?':
			return 1, true
		case '[':
			switch found, width := matchBracket(pattern, c); found {
			case inSet:
				return width, true
			case notInSet, brokenSet:
				return 0, false
			}
			// The [ is a plain character.
		case '\\':
			if len(pattern) > 1 {
				return 2, pattern[1] == c
			}
		}
		return 1, pattern[0] == c
	})
}

// set is what matchBracket finds.
type set int

const (
	// noSet is a [ that doesn't start a set, and is a plain character.
	noSet set = iota
	inSet
	notInSet
	// brokenSet is a set that matches nothing: one with an unknown class,
	// or whose only ] a class took. OpenSSH crashes on the second.
	brokenSet
)

// matchBracket matches c against the set pattern starts with, parsed as
// OpenBSD's glob, which OpenSSH uses, parses it, and says how long the set
// is. A set needs an unescaped ] after its first character, which may
// itself be ].
func matchBracket(pattern string, c byte) (found set, width int) {
	i := 1
	negated := i < len(pattern) && pattern[i] == '!'
	if negated {
		i++
	}
	if i >= len(pattern) || !hasClose(pattern[i:]) {
		return noSet, 0
	}
	matched := false
	for first := true; ; first = false {
		if i >= len(pattern) {
			return brokenSet, 0
		}
		lo := pattern[i]
		if lo == ']' && !first {
			if matched != negated {
				return inSet, i + 1
			}
			return notInSet, i + 1
		}
		if lo == '[' && strings.HasPrefix(pattern[i:], "[:") {
			if end := strings.Index(pattern[i+2:], ":]"); end >= 0 {
				in, known := inClass(pattern[i+2:i+2+end], c)
				if !known {
					return brokenSet, 0
				}
				matched = matched || in
				i += 2 + end + 2
				continue
			}
		}
		if lo == '\\' && i+1 < len(pattern) {
			i++
			lo = pattern[i]
		}
		i++
		hi := lo
		if i+1 < len(pattern) && pattern[i] == '-' && pattern[i+1] != ']' {
			i++
			if pattern[i] == '\\' && i+1 < len(pattern) {
				i++
			}
			hi = pattern[i]
			i++
		}
		if lo <= c && c <= hi {
			matched = true
		}
	}
}

// hasClose reports whether an unescaped ] follows the first character of
// s, a set's contents.
func hasClose(s string) bool {
	i := 0
	if s[0] == '\\' {
		i++
	}
	for i++; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case ']':
			return true
		}
	}
	return false
}

// inClass reports whether c is in the POSIX character class named class,
// in the C locale, and whether there is such a class.
func inClass(class string, c byte) (in, known bool) {
	upper := 'A' <= c && c <= 'Z'
	lower := 'a' <= c && c <= 'z'
	digit := '0' <= c && c <= '9'
	switch class {
	case "alpha":
		return upper || lower, true
	case "upper":
		return upper, true
	case "lower":
		return lower, true
	case "digit":
		return digit, true
	case "alnum":
		return upper || lower || digit, true
	case "xdigit":
		return digit || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F', true
	case "space":
		return c == ' ' || '\t' <= c && c <= '\r', true
	case "blank":
		return c == ' ' || c == '\t', true
	case "cntrl":
		return c < 0x20 || c == 0x7f, true
	case "print":
		return 0x20 <= c && c < 0x7f, true
	case "graph":
		return 0x20 < c && c < 0x7f, true
	case "punct":
		return 0x20 < c && c < 0x7f && !upper && !lower && !digit, true
	}
	return false, false
}
