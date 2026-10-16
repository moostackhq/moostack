package sshclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"

	"golang.org/x/crypto/ssh"

	"github.com/moostackhq/moostack/internal/sshconfig"
)

// hostKeyCheck is how a connection checks the host key: with callback,
// asking for algorithms, or the defaults if algorithms is nil.
type hostKeyCheck struct {
	callback   ssh.HostKeyCallback
	algorithms []string
}

// fetchAlgorithms are the host key algorithms FetchHostKey asks for, most
// preferred first.
var fetchAlgorithms = []string{
	ssh.KeyAlgoED25519,
	ssh.KeyAlgoECDSA256, ssh.KeyAlgoECDSA384, ssh.KeyAlgoECDSA521,
	ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256,
}

// errFetched stops the handshake once FetchHostKey has the key.
var errFetched = errors.New("fetched the host key")

// FetchHostKey returns the host key target presents, preferring ed25519,
// without trusting it or authenticating.
func FetchHostKey(ctx context.Context, target string, opts Options) (ssh.PublicKey, error) {
	s, err := sshconfig.Resolve(target, opts.SSH)
	if err != nil {
		return nil, err
	}
	r, err := routeTo(ctx, s, opts, 0)
	if err != nil {
		return nil, fmt.Errorf("fetching the host key of %s: %w", target, err)
	}
	defer r.close()
	key, err := fetchHostKey(ctx, r, s)
	if err != nil {
		return nil, fmt.Errorf("fetching the host key of %s: %w", target, err)
	}
	return key, nil
}

// fetchHostKey returns the host key the host of s presents over r.
func fetchHostKey(ctx context.Context, r *route, s sshconfig.Settings) (ssh.PublicKey, error) {
	var key ssh.PublicKey
	_, _, err := connect(ctx, r, s, &ssh.ClientConfig{
		User: s.User,
		HostKeyCallback: func(_ string, _ net.Addr, k ssh.PublicKey) error {
			key = k
			return errFetched
		},
		HostKeyAlgorithms: fetchAlgorithms,
	})
	if key == nil {
		return nil, err
	}
	return key, nil
}

// HostKeyMismatchError says a host presented a key the caller didn't pin.
type HostKeyMismatchError struct {
	Target string
	Got    ssh.PublicKey
	Pinned []ssh.PublicKey
}

func (e *HostKeyMismatchError) Error() string {
	pinned := make([]string, len(e.Pinned))
	for i, k := range e.Pinned {
		pinned[i] = ssh.FingerprintSHA256(k)
	}
	is := "key is"
	if len(pinned) != 1 {
		is = "keys are"
	}
	return fmt.Sprintf("host key mismatch for %s: it presented %s %s, but the pinned %s %s",
		e.Target, e.Got.Type(), ssh.FingerprintSHA256(e.Got), is, strings.Join(pinned, ", "))
}

// pinned is a host key callback that accepts only keys, the keys pinned
// for target.
func pinned(target string, keys []ssh.PublicKey) ssh.HostKeyCallback {
	return func(_ string, _ net.Addr, key ssh.PublicKey) error {
		if slices.ContainsFunc(keys, func(k ssh.PublicKey) bool { return sameKey(k, key) }) {
			return nil
		}
		return &HostKeyMismatchError{Target: target, Got: key, Pinned: keys}
	}
}

// sameKey reports whether a and b are the same key.
func sameKey(a, b ssh.PublicKey) bool { return bytes.Equal(a.Marshal(), b.Marshal()) }

// hostKeyAlgorithms are the algorithms that keys can be used with, so
// that a server with several host keys presents a pinned one.
func hostKeyAlgorithms(keys []ssh.PublicKey) []string {
	var algos []string
	for _, k := range keys {
		// An RSA key's or certificate's type names its legacy SHA-1
		// algorithm. Its SHA-2 algorithms, which OpenSSH uses, have names
		// of their own.
		switch k.Type() {
		case ssh.KeyAlgoRSA:
			algos = append(algos, ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256)
		case ssh.CertAlgoRSAv01:
			algos = append(algos, ssh.CertAlgoRSASHA512v01, ssh.CertAlgoRSASHA256v01)
		default:
			algos = append(algos, k.Type())
		}
	}
	return algos
}
