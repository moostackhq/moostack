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

// removedIdentities are the node identities that the node holds as
// removed from the cluster, kept in a file, and the open connections of
// the peers it accepted.
type removedIdentities struct {
	file string
	// onAdd, if set, is called with the identities each add added, once
	// their peers are cut off.
	onAdd func([]hostlayout.RemovedIdentity)
	mu    sync.Mutex
	// list is the identities, in the order they were added.
	list  []hostlayout.RemovedIdentity
	conns map[*peerConn]bool
}

// loadRemoved is the removed identities kept in file, none if there is
// no file. It fails if the file can't be read.
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
	return holds(r.list, fingerprint)
}

// holds says whether list has the identity of fingerprint.
func holds(list []hostlayout.RemovedIdentity, fingerprint string) bool {
	return slices.ContainsFunc(list, func(r hostlayout.RemovedIdentity) bool { return r.Identity == fingerprint })
}

// nodes are the names of the removed identities' nodes.
func (r *removedIdentities) nodes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, len(r.list))
	for i, id := range r.list {
		names[i] = id.Node
	}
	return names
}

// add adds identities to the removed ones, keeping those there. If the
// list grows, it writes the file, before the agent refuses them, cuts off
// the peers that prove one, and calls onAdd.
func (r *removedIdentities) add(identities []hostlayout.RemovedIdentity) error {
	r.mu.Lock()
	list := slices.Clone(r.list)
	for _, id := range identities {
		if !holds(list, id.Identity) {
			list = append(list, id)
		}
	}
	added := list[len(r.list):]
	if len(added) == 0 {
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
		if holds(list, c.fingerprint) {
			cut = append(cut, c)
		}
	}
	r.mu.Unlock()
	// A connection that registers after the unlock checks the new list.
	for _, c := range cut {
		c.Close()
	}
	if r.onAdd != nil {
		r.onAdd(added)
	}
	return nil
}

// track keeps conn, of a peer that proved the identity of fingerprint,
// until it closes, or refuses it if that identity is removed.
func (r *removedIdentities) track(conn net.Conn, fingerprint string) (net.Conn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if holds(r.list, fingerprint) {
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

// applyRemoval closes the membership connections of the peers that prove
// added, and forgets the removed nodes as forgetRemoved does, by the
// generation the node last recorded, if it can read it.
func (s *agentServer) applyRemoval(added []hostlayout.RemovedIdentity) {
	for _, id := range added {
		_ = s.members.CutOff(id.Identity)
	}
	if applied, err := s.applied(); err == nil {
		s.forgetRemoved(applied)
	}
}

// forgetRemoved has membership forget every node of a removed identity
// that a doesn't list. A node a lists stays until a recording leaves it
// out, and a node that has recorded no generation forgets nothing.
// Membership refuses the node's own name, and everything once it stopped.
func (s *agentServer) forgetRemoved(a appliedGeneration) {
	if a.Generation == 0 {
		return
	}
	for _, name := range s.removed.nodes() {
		if !slices.ContainsFunc(a.Nodes, func(n generationNode) bool { return n.Name == name }) {
			_ = s.members.Forget(name)
		}
	}
}

func (s *agentServer) RemoveIdentities(_ context.Context, req *nodev1.RemoveIdentitiesRequest) (*nodev1.RemoveIdentitiesResponse, error) {
	identities := make([]hostlayout.RemovedIdentity, len(req.Identities))
	for i, id := range req.Identities {
		identities[i] = hostlayout.RemovedIdentity{Node: id.Node, Identity: id.Identity}
		if err := identities[i].Check(); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	err := s.lock.fenced(req.Lease, func() error {
		if err := s.removed.add(identities); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &nodev1.RemoveIdentitiesResponse{}, nil
}
