//go:build linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"slices"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hostlayout"
	"github.com/moostackhq/moostack/internal/identity"
)

// removedIdentities are the node identities, by fingerprint, that the
// node holds as removed from the cluster, kept in a file, and the open
// connections of the peers it accepted, so that removing an identity
// cuts off the peer that proves it.
type removedIdentities struct {
	file string
	mu   sync.Mutex
	// list is the fingerprints, in the order they were added.
	list  []string
	conns map[*peerConn]bool
}

// loadRemoved is the removed identities kept in file, none if there is
// no file. It fails if the file can't be read, so that the agent never
// accepts a peer it may have been told to refuse.
func loadRemoved(file string) (*removedIdentities, error) {
	r := &removedIdentities{file: file, conns: map[*peerConn]bool{}}
	data, err := os.ReadFile(file)
	if errors.Is(err, fs.ErrNotExist) {
		return r, nil
	}
	if err == nil {
		r.list, err = hostlayout.ParseRemovedIdentities(data)
	}
	if err != nil {
		return nil, fmt.Errorf("reading the removed identities: %s: %w", file, err)
	}
	return r, nil
}

// has says whether the identity of fingerprint is removed.
func (r *removedIdentities) has(fingerprint string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.list, fingerprint)
}

// add adds fingerprints to the removed identities, keeping those there,
// and cuts off the peers that prove one. The file is written only if the
// list grows, before the agent refuses them.
func (r *removedIdentities) add(fingerprints []string) error {
	r.mu.Lock()
	list := slices.Clone(r.list)
	for _, fp := range fingerprints {
		if !slices.Contains(list, fp) {
			list = append(list, fp)
		}
	}
	if len(list) == len(r.list) {
		r.mu.Unlock()
		return nil
	}
	if err := writeFile(r.file, hostlayout.MarshalRemovedIdentities(list)); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("keeping the removed identities: %w", err)
	}
	r.list = list
	var cut []*peerConn
	for c := range r.conns {
		if slices.Contains(list, c.fingerprint) {
			cut = append(cut, c)
		}
	}
	r.mu.Unlock()
	// A connection that registers after the unlock checks the new list,
	// so none slips through.
	for _, c := range cut {
		c.Close()
	}
	return nil
}

// track keeps conn, of a peer that proved the identity of fingerprint,
// until it closes, or refuses it if that identity is removed.
func (r *removedIdentities) track(conn net.Conn, fingerprint string) (net.Conn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if slices.Contains(r.list, fingerprint) {
		conn.Close()
		return nil, fmt.Errorf("the identity %s is removed from the cluster", fingerprint)
	}
	c := &peerConn{Conn: conn, fingerprint: fingerprint, removed: r}
	r.conns[c] = true
	return c, nil
}

// peerConn is a peer's connection, which the removed identities keep
// until it closes.
type peerConn struct {
	net.Conn
	fingerprint string
	removed     *removedIdentities
	once        sync.Once
}

func (c *peerConn) Close() error {
	c.once.Do(func() {
		c.removed.mu.Lock()
		delete(c.removed.conns, c)
		c.removed.mu.Unlock()
	})
	return c.Conn.Close()
}

// peerCredentials are the peer listener's TLS credentials, which accept
// only peers with an identity of the cluster that isn't removed, and
// keep each one's connection, so that removing its identity cuts it off.
type peerCredentials struct {
	credentials.TransportCredentials
	removed *removedIdentities
}

// newPeerCredentials are the credentials to serve peers with, as id,
// refusing the identities in removed.
func newPeerCredentials(id *identity.Identity, removed *removedIdentities) credentials.TransportCredentials {
	return peerCredentials{credentials.NewTLS(id.ServerTLS(removed.has)), removed}
}

func (c peerCredentials) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.TransportCredentials.ServerHandshake(raw)
	if err != nil {
		return nil, nil, err
	}
	tlsInfo, ok := info.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		conn.Close()
		return nil, nil, errors.New("the peer proved no identity")
	}
	tracked, err := c.removed.track(conn, identity.CertFingerprint(tlsInfo.State.PeerCertificates[0]))
	if err != nil {
		return nil, nil, err
	}
	return tracked, info, nil
}

func (c peerCredentials) Clone() credentials.TransportCredentials {
	return peerCredentials{c.TransportCredentials.Clone(), c.removed}
}

func (s *agentServer) RemoveIdentities(_ context.Context, req *nodev1.RemoveIdentitiesRequest) (*nodev1.RemoveIdentitiesResponse, error) {
	err := s.lock.fenced(req.Lease, func() error {
		if err := s.removed.add(req.Identities); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &nodev1.RemoveIdentitiesResponse{}, nil
}
