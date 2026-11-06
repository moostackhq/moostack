//go:build linux

package agent

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/hivemindadapter"
)

func TestEveryMemberStateIsReported(t *testing.T) {
	for state, want := range map[hivemindadapter.State]nodev1.MemberState{
		hivemindadapter.Alive:   nodev1.MemberState_MEMBER_STATE_ALIVE,
		hivemindadapter.Suspect: nodev1.MemberState_MEMBER_STATE_SUSPECT,
		hivemindadapter.Dead:    nodev1.MemberState_MEMBER_STATE_DEAD,
		hivemindadapter.Left:    nodev1.MemberState_MEMBER_STATE_LEFT,
	} {
		m := membershipOf(hivemindadapter.Status{Members: []hivemindadapter.Member{{NodeID: "a", Address: "b", State: state}}, HealthScore: 3, Dropped: map[string]uint64{"unsent": 2}, Transport: map[string]uint64{"dials": 4}})
		if e := m.Members[0]; e.Node != "a" || e.Address != "b" || e.State != want || m.HealthScore != 3 || m.Dropped["unsent"] != 2 || m.Transport["dials"] != 4 {
			t.Errorf("%s is reported as %v", state, m)
		}
	}
}

// generationFile writes content where a server reads its generation.
func generationFile(t *testing.T, content []byte) *agentServer {
	t.Helper()
	s := &agentServer{generationFile: filepath.Join(t.TempDir(), "generation")}
	if err := os.WriteFile(s.generationFile, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestAGenerationFileOfANodeItCantUseIsUnreadable(t *testing.T) {
	for name, content := range map[string]string{
		"nodes by name":    `{"generation":1,"nodes":["a"]}`,
		"no name":          `{"generation":1,"nodes":[{"name":"","private_address":"192.0.2.1"}]}`,
		"no address":       `{"generation":1,"nodes":[{"name":"a"}]}`,
		"an empty address": `{"generation":1,"nodes":[{"name":"a","private_address":""}]}`,
		"a host name":      `{"generation":1,"nodes":[{"name":"a","private_address":"a.example"}]}`,
		"garbage":          `garbage`,
	} {
		s := generationFile(t, []byte(content))
		if a, err := s.applied(); err == nil || !strings.Contains(err.Error(), s.generationFile) {
			t.Errorf("%s: applied says %+v, %v, want an error naming the file", name, a, err)
		}
	}
	s := generationFile(t, []byte(`{"generation":1,"nodes":[{"name":"good","private_address":"192.0.2.1"},{"name":"bad"}]}`))
	if _, err := s.applied(); err == nil || !strings.Contains(err.Error(), `"bad"`) {
		t.Errorf("a bad node after a good one: %v, want an error naming it", err)
	}
	if a, err := generationFile(t, []byte(`{"generation":2,"nodes":[{"name":"a","private_address":"2001:db8::1"}]}`)).applied(); err != nil || a.Generation != 2 || a.Nodes[0].PrivateAddress.String() != "2001:db8::1" {
		t.Errorf("a good file: %+v, %v", a, err)
	}
}

// Whatever the generation file holds, what reads is a node list that seeds.
func FuzzGenerationFile(f *testing.F) {
	f.Add([]byte(`{"generation":1,"nodes":[{"name":"a","private_address":"192.0.2.1"}]}`))
	f.Add([]byte(`{"generation":1,"nodes":["a"]}`))
	f.Add([]byte(`{"nodes":[{"name":"a","private_address":""}]}`))
	f.Add([]byte(`garbage`))
	f.Fuzz(func(t *testing.T, content []byte) {
		a, err := generationFile(t, content).applied()
		if err != nil {
			return
		}
		for _, n := range a.Nodes {
			if n.Name == "" || !n.PrivateAddress.IsValid() {
				t.Fatalf("%q reads as a node %+v", content, n)
			}
		}
	})
}

// What generationNodes takes, the generation file reads back the same,
// and what it refuses is refused with INVALID_ARGUMENT.
func FuzzGenerationNodesReadBack(f *testing.F) {
	f.Add("node-1", "192.0.2.1")
	f.Add("", "192.0.2.1")
	f.Add("a", "")
	f.Add("a", "fe80::1%eth0")
	f.Add("a\nb", "::ffff:192.0.2.1")
	f.Fuzz(func(t *testing.T, name, address string) {
		nodes, err := generationNodes([]*nodev1.GenerationNode{{Name: name, PrivateAddress: address}})
		if err != nil {
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("%q at %q: %v", name, address, err)
			}
			return
		}
		data, err := json.Marshal(appliedGeneration{Generation: 1, Nodes: nodes})
		if err != nil {
			t.Fatal(err)
		}
		a, err := generationFile(t, data).applied()
		if err != nil {
			t.Fatalf("%q at %q doesn't read back: %v", name, address, err)
		}
		// JSON keeps text valid UTF-8, so a name that isn't reads back changed.
		if !strings.ContainsRune(name, '�') && strings.ToValidUTF8(name, "") == name && (len(a.Nodes) != 1 || a.Nodes[0] != nodes[0]) {
			t.Fatalf("%+v reads back as %+v", nodes[0], a.Nodes)
		}
	})
}

// A stop that never ends holds serveAll up for StopTimeout, and no more.
func TestServeAllWaitsForAStuckStopOnlyUntilStopTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	never := make(chan struct{})
	defer close(never)
	called := make(chan struct{})
	start := time.Now()
	err := serveAll(ctx, map[*grpc.Server]net.Listener{}, func() {
		close(called)
		<-never
	})
	if err != nil {
		t.Error(err)
	}
	if took := time.Since(start); took < StopTimeout || took > StopTimeout+2*time.Second {
		t.Errorf("serveAll took %s, want StopTimeout, %s", took, StopTimeout)
	}
	select {
	case <-called:
	default:
		t.Error("serveAll didn't call stop")
	}
	start = time.Now()
	if err := serveAll(ctx, map[*grpc.Server]net.Listener{}, func() {}); err != nil {
		t.Error(err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("with a stop that returns, serveAll took %s", took)
	}
}
