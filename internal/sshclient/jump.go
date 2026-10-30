package sshclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/sshconfig"
)

// route is how to reach a host: directly, or through jump hosts whose
// connections close with the route.
type route struct {
	dial  func(ctx context.Context, addr string) (net.Conn, error)
	jumps []*ssh.Client
}

func (r *route) close() {
	for i := len(r.jumps) - 1; i >= 0; i-- {
		r.jumps[i].Close()
	}
}

// maxJumpDepth is how deeply jump hosts' own ProxyJumps may nest, which
// stops a loop of them.
const maxJumpDepth = 8

// routeTo returns the route to the host of the settings s, through the
// jump hosts its ProxyJump names. As in OpenSSH, the first jump host is
// reached through its own ProxyJump, and each later one through the jump
// host before it.
//
// A jump host's key isn't checked: only the node's key is pinned. The
// connection to the node runs end to end through the jump hosts, so a
// jump host can't read or change it.
func routeTo(ctx context.Context, s sshconfig.Settings, opts Options, depth int) (*route, error) {
	if s.ProxyJump == "" {
		var d net.Dialer
		return &route{dial: func(ctx context.Context, addr string) (net.Conn, error) {
			return d.DialContext(ctx, "tcp", addr)
		}}, nil
	}
	if depth >= maxJumpDepth {
		return nil, errors.New("jump hosts' ProxyJumps nest too deeply")
	}
	r := &route{}
	hops := strings.Split(s.ProxyJump, ",")
	for i, hop := range hops {
		if i < len(hops)-1 {
			decoded, err := earlierHop(hop)
			if err != nil {
				r.close()
				return nil, err
			}
			hop = decoded
		}
		hs, err := resolveHop(hop, opts)
		if err != nil {
			r.close()
			return nil, err
		}
		if i == 0 {
			first, err := routeTo(ctx, hs, opts, depth+1)
			if err != nil {
				return nil, err
			}
			r = first
		}
		jump, _, err := connectAsOperator(ctx, r, hs, opts, hostKeyCheck{callback: ssh.InsecureIgnoreHostKey()})
		if err != nil {
			r.close()
			return nil, fmt.Errorf("connecting to jump host %s: %w", hop, err)
		}
		r.jumps = append(r.jumps, jump)
		r.dial = func(ctx context.Context, addr string) (net.Conn, error) {
			return jump.DialContext(ctx, "tcp", addr)
		}
	}
	return r, nil
}

// resolveHop resolves the settings for one ProxyJump hop,
// [user@]host[:port]. The hop's user and port win over the config.
//
// OpenSSH reaches a jump host by running ssh through the shell, so the
// shell decodes the hop's user and host: ProxyJump "jump" names the host
// jump. They are decoded the same way here.
func resolveHop(hop string, opts Options) (sshconfig.Settings, error) {
	user, host, port := "", hop, ""
	if i := strings.LastIndex(hop, "@"); i >= 0 {
		user, host = hop[:i], hop[i+1:]
	}
	if h, p, err := net.SplitHostPort(host); err == nil {
		host, port = h, p
	} else if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	target, err := hopPart(host)
	if err != nil {
		return sshconfig.Settings{}, fmt.Errorf("jump host %s: %w", hop, err)
	}
	if user != "" {
		if user, err = hopPart(user); err != nil {
			return sshconfig.Settings{}, fmt.Errorf("jump host %s: %w", hop, err)
		}
		target = user + "@" + target
	}
	s, err := sshconfig.Resolve(target, opts.SSH)
	if err != nil {
		return sshconfig.Settings{}, err
	}
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil {
			n, err = net.LookupPort("tcp", port)
		}
		if err != nil || n < 1 || n > 65535 {
			return sshconfig.Settings{}, fmt.Errorf("jump host %s: bad port %q", hop, port)
		}
		s.Port = n
	}
	return s, nil
}

// hopPart decodes a ProxyJump hop's user or host as the shell OpenSSH
// runs it through does. OpenSSH also expands % in the command it runs, so
// % isn't supported.
func hopPart(s string) (string, error) {
	w, err := shellWord(s)
	if err == nil && strings.Contains(w, "%") {
		err = errors.New("% in ProxyJump isn't supported")
	}
	return w, err
}

// earlierHop decodes a ProxyJump hop before the last. OpenSSH passes those
// hops whole, as raw text, to the inner ssh it runs through the shell, and
// the inner ssh runs them through the shell again, so one decoding must
// leave nothing for another to change.
func earlierHop(hop string) (string, error) {
	w, err := shellWord(hop)
	if err == nil && strings.ContainsAny(w, shellSpecial+`'"%`) {
		err = errShellSpecial
	}
	if err != nil {
		return "", fmt.Errorf("jump host %s: %w", hop, err)
	}
	return w, nil
}
