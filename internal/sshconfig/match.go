package sshconfig

import (
	"errors"
	"fmt"
	"strings"
)

// match reports whether a Match line's criteria all match. Criteria other
// than host, originalhost, user and all are an error.
func (r *resolver) match(criteria []string) (bool, error) {
	matched := true
	for i := 0; i < len(criteria); i++ {
		name, negated := strings.CutPrefix(criteria[i], "!")
		// A criterion may be joined to its patterns, as in host=web*.
		name, patterns, joined := strings.Cut(name, "=")
		name = strings.ToLower(name)
		if name == "all" {
			// all may only end the criteria.
			if i+1 < len(criteria) {
				return false, errors.New("Match all can't be combined with other criteria")
			}
			if negated {
				matched = false
			}
			continue
		}
		if name != "host" && name != "originalhost" && name != "user" {
			return false, fmt.Errorf("unsupported Match criterion %q", name)
		}
		if joined && patterns == "" {
			return false, fmt.Errorf("missing argument for Match %s", name)
		}
		if !joined {
			if i+1 == len(criteria) {
				return false, fmt.Errorf("missing argument for Match %s", name)
			}
			i++
			patterns = criteria[i]
		}
		var ok bool
		switch name {
		case "host":
			ok = matchList(strings.ToLower(r.hostNameSoFar()), strings.ToLower(patterns))
		case "originalhost":
			ok = matchList(strings.ToLower(r.host), strings.ToLower(patterns))
		case "user":
			ok = matchList(r.userSoFar(), patterns)
		}
		if ok == negated {
			matched = false
		}
	}
	return matched, nil
}

// matchList reports whether s matches a comma-separated pattern list: at
// least one pattern matches and no negated one does.
func matchList(s, list string) bool {
	return matchPatterns(s, strings.Split(list, ","))
}

// matchPatterns reports whether s matches patterns: at least one pattern
// matches and no negated one does.
func matchPatterns(s string, patterns []string) bool {
	matched := false
	for _, p := range patterns {
		if neg, ok := strings.CutPrefix(p, "!"); ok {
			if matchPattern(s, neg) {
				return false
			}
		} else if matchPattern(s, p) {
			matched = true
		}
	}
	return matched
}

// matchPattern reports whether s matches pattern, in which "*" matches any
// run of characters and "?" any one character. It is case-sensitive.
func matchPattern(s, pattern string) bool {
	return matchStars(pattern, s, func(pattern string, c byte) (int, bool) {
		return 1, pattern[0] == '?' || pattern[0] == c
	})
}

// matchStars reports whether s matches pattern, in which "*" matches any
// run of characters and one matches anything else, one character of s at
// a time: it says whether c matches what pattern starts with, and how much
// of pattern that is. On a mismatch, only the last "*" is retried with one
// more character, which is enough and keeps matching polynomial.
func matchStars(pattern, s string, one func(pattern string, c byte) (width int, ok bool)) bool {
	p, i := 0, 0
	star, starAt := -1, 0
	for i < len(s) {
		if p < len(pattern) && pattern[p] == '*' {
			star, starAt = p, i
			p++
			continue
		}
		if p < len(pattern) {
			if width, ok := one(pattern[p:], s[i]); ok {
				p += width
				i++
				continue
			}
		}
		if star < 0 {
			return false
		}
		starAt++
		p, i = star+1, starAt
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}
