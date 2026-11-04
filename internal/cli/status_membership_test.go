package cli_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	nodev1 "github.com/moostackhq/moostack/api/node/v1"
	"github.com/moostackhq/moostack/internal/nodeclient"
)

// member is the node called name, at its private address in
// statusCluster, as another node's membership holds it in state.
func member(name string, i int, state nodev1.MemberState) *nodev1.Member {
	return &nodev1.Member{Node: name, Address: "10.0.0." + string(rune('0'+i)) + ":7443", State: state}
}

const (
	alive   = nodev1.MemberState_MEMBER_STATE_ALIVE
	suspect = nodev1.MemberState_MEMBER_STATE_SUSPECT
	dead    = nodev1.MemberState_MEMBER_STATE_DEAD
	left    = nodev1.MemberState_MEMBER_STATE_LEFT
)

// withMembership is a node whose agent answers, running the release that
// statusCluster pins, with membership m.
func withMembership(m *nodev1.Membership) *fakeNode {
	return &fakeNode{status: nodeclient.Status{Mode: nodev1.Mode_MODE_DIRECT, Generation: 1, Membership: m, Components: []*nodev1.Component{
		component("agent", "1.0.0", healthy, ""),
		component("updater", "1.0.0", healthy, ""),
		component("containerd", "2.0.0", healthy, ""),
	}}}
}

// membershipCluster is four nodes: node2 holds node3 dead and node1
// suspect, node3 holds node2 left and doesn't know node1, and node1
// holds node4, whose agent doesn't answer, dead.
func membershipCluster() map[string]*fakeNode {
	return map[string]*fakeNode{
		"node1": withMembership(&nodev1.Membership{Members: []*nodev1.Member{
			member("node1", 1, alive), member("node2", 2, alive), member("node3", 3, alive), member("node4", 4, dead),
		}}),
		"node2": withMembership(&nodev1.Membership{HealthScore: 3, Dropped: map[string]uint64{"queue_full": 2, "impersonated": 1}, Members: []*nodev1.Member{
			member("node1", 1, suspect), member("node2", 2, alive), member("node3", 3, dead), member("node4", 4, left),
		}}),
		"node3": withMembership(&nodev1.Membership{Members: []*nodev1.Member{member("node2", 2, left), member("node3", 3, alive)}}),
		"node4": {status: nodeclient.Status{AgentErr: errors.New("connection refused"), Components: []*nodev1.Component{
			component("agent", "", unhealthy, "the agent doesn't answer: connection refused"),
			component("updater", "1.0.0", healthy, ""),
		}}},
	}
}

func TestClusterStatusShowsEachNodesMembershipAndFlagsANodeAnotherHoldsDown(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2", "node3", "node4")
	o.nodes = membershipCluster()
	want := `Cluster prod on release 1.0.0
Operation lock: free on the nodes whose agent answered
Update: stopped partway; run mooctl cluster update again to finish it

NODE          RUNNING  PINNED  HEALTH
node1  direct
  agent       1.0.0    1.0.0   healthy
  updater     1.0.0    1.0.0   healthy
  containerd  2.0.0    2.0.0   healthy
  membership: 3 alive, 1 dead, health score 0, no message dropped
node2  direct
  agent       1.0.0    1.0.0   healthy
  updater     1.0.0    1.0.0   healthy
  containerd  2.0.0    2.0.0   healthy
  membership: 1 alive, 1 suspect, 1 dead, 1 left, health score 3, dropped 1 impersonated, 2 queue_full
node3  direct
  agent       1.0.0    1.0.0   healthy
  updater     1.0.0    1.0.0   healthy
  containerd  2.0.0    2.0.0   healthy
  membership: 1 alive, 1 left, health score 0, no message dropped
node4  mode unknown
  agent       -        1.0.0   unhealthy
  updater     1.0.0    1.0.0   healthy
  containerd  -        2.0.0   unknown

node1: node2 holds it suspect, though its agent answers mooctl, so they may not reach each other over UDP on the peer port, 7443
node1: node3 doesn't know it, though its agent answers mooctl, so they may not reach each other over UDP on the peer port, 7443
node2: node3 holds it left, though its agent answers mooctl, so they may not reach each other over UDP on the peer port, 7443
node3: node2 holds it dead, though its agent answers mooctl, so they may not reach each other over UDP on the peer port, 7443
node4: agent: the agent doesn't answer: connection refused
`
	if r := o.mustRun("--release-source", source, "cluster", "status"); r.stdout != want || r.stderr != "" {
		t.Errorf("cluster status printed\n%s\non stderr %q, want\n%s", r.stdout, r.stderr, want)
	}
	o.terminal = &scriptedTerminal{}
	if r := o.mustRun("--release-source", source, "cluster", "status"); r.stdout != want {
		t.Errorf("cluster status on a terminal printed\n%s\nwant\n%s", r.stdout, want)
	}
}

func TestClusterStatusJSONHasEachNodesMembership(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2", "node3", "node4")
	nodes := membershipCluster()
	delete(nodes, "node4")
	o.nodes = nodes
	o.nodes["node4"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	r := o.mustRun("--release-source", source, "cluster", "status", "--json")
	var view struct {
		Nodes []struct {
			Name       string `json:"name"`
			Membership *struct {
				Members []struct {
					Node, Address, State string
				} `json:"members"`
				HealthScore uint32            `json:"health_score"`
				Dropped     map[string]uint64 `json:"dropped"`
			} `json:"membership"`
			SuspectedBy []struct{ Node, State string } `json:"suspected_by"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(r.stdout), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Nodes) != 4 {
		t.Fatalf("cluster status --json has %d nodes, want 4:\n%s", len(view.Nodes), r.stdout)
	}
	node1, node2, node4 := view.Nodes[0], view.Nodes[1], view.Nodes[3]
	if m := node2.Membership; m == nil || m.HealthScore != 3 || m.Dropped["impersonated"] != 1 || m.Dropped["queue_full"] != 2 || len(m.Members) != 4 ||
		m.Members[0].Node != "node1" || m.Members[0].Address != "10.0.0.1:7443" || m.Members[0].State != "suspect" || m.Members[3].State != "left" {
		t.Errorf("node2's membership is %+v", m)
	}
	if len(node1.SuspectedBy) != 2 || node1.SuspectedBy[0].Node != "node2" || node1.SuspectedBy[0].State != "suspect" ||
		node1.SuspectedBy[1].Node != "node3" || node1.SuspectedBy[1].State != "absent" {
		t.Errorf("node1 is suspected by %+v, want node2, suspect, and node3, absent", node1.SuspectedBy)
	}
	if node4.Membership != nil || node4.SuspectedBy != nil {
		t.Errorf("the unreachable node4 has %+v and is suspected by %+v, want neither", node4.Membership, node4.SuspectedBy)
	}
	if want := `"membership": {
        "members": [
          {
            "node": "node2",
            "address": "10.0.0.2:7443",
            "state": "left"
          },
          {
            "node": "node3",
            "address": "10.0.0.3:7443",
            "state": "alive"
          }
        ],
        "health_score": 0
      },
      "suspected_by": [
        {
          "node": "node2",
          "state": "dead"
        }
      ]`; !strings.Contains(r.stdout, want) {
		t.Errorf("cluster status --json printed\n%s\nwant node3 with\n%s", r.stdout, want)
	}
}

// A member state the node doesn't say is unknown.
func TestClusterStatusShowsAMemberStateItDoesntKnowAsUnknown(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1")
	o.nodes = map[string]*fakeNode{"node1": withMembership(&nodev1.Membership{Members: []*nodev1.Member{member("node1", 1, nodev1.MemberState(9))}})}
	r := o.mustRun("--release-source", source, "cluster", "status")
	if want := "  membership: 1 unknown, health score 0, no message dropped\n"; !strings.Contains(r.stdout, want) {
		t.Errorf("cluster status printed\n%s\nwant\n%s", r.stdout, want)
	}
}

// The nodes left after a removal record each other's private addresses,
// as the cluster directory has them.
func TestNodeRemoveRecordsTheRemainingNodesPrivateAddresses(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2", "node3")
	o.nodes = map[string]*fakeNode{
		"node1": withMembership(nil), "node2": withMembership(nil),
		"node3": {err: errors.New("dial tcp: no route to host")},
	}
	o.mustRun("--release-source", source, "node", "remove", "node3", "--unreachable")
	want := "node1 at 10.0.0.1, node2 at 10.0.0.2"
	for _, name := range []string{"node1", "node2"} {
		var got []string
		for _, n := range o.nodes[name].lastRecorded() {
			got = append(got, n.Name+" at "+n.PrivateAddress)
		}
		if strings.Join(got, ", ") != want {
			t.Errorf("%s recorded %v, want %s", name, got, want)
		}
	}
}

// A node's view of itself raises no flag, and another's state mooctl
// doesn't know does.
func TestClusterStatusFlagsOnlyOtherNodesViews(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1", "node2")
	o.nodes = map[string]*fakeNode{
		"node1": withMembership(&nodev1.Membership{Members: []*nodev1.Member{member("node1", 1, suspect), member("node2", 2, alive)}}),
		"node2": withMembership(&nodev1.Membership{Members: []*nodev1.Member{member("node1", 1, nodev1.MemberState(9)), member("node2", 2, alive)}}),
	}
	r := o.mustRun("--release-source", source, "cluster", "status")
	want := "node1: node2 holds it unknown, though"
	if !strings.Contains(r.stdout, want) || strings.Contains(r.stdout, "node1: node1") || strings.Contains(r.stdout, "node2: node1") {
		t.Errorf("cluster status printed\n%s\nwant only %q", r.stdout, want)
	}
}

func TestClusterMembersShowsWhatEachNodeKnows(t *testing.T) {
	o := newOperator(t, "op")
	statusCluster(t, o, "node1", "node2", "node3", "node4", "node5")
	o.nodes = membershipCluster()
	o.nodes["node5"] = &fakeNode{err: errors.New("dial tcp: no route to host")}
	want := `node1
  node1  alive  10.0.0.1:7443
  node2  alive  10.0.0.2:7443
  node3  alive  10.0.0.3:7443
  node4  dead   10.0.0.4:7443
node2
  node1  suspect  10.0.0.1:7443
  node2  alive    10.0.0.2:7443
  node3  dead     10.0.0.3:7443
  node4  left     10.0.0.4:7443
node3
  node2  left   10.0.0.2:7443
  node3  alive  10.0.0.3:7443
node4  no view
node5  no view

node1: node2 holds it suspect, though its agent answers mooctl, so they may not reach each other over UDP on the peer port, 7443
node1: node3 doesn't know it, though its agent answers mooctl, so they may not reach each other over UDP on the peer port, 7443
node2: node3 holds it left, though its agent answers mooctl, so they may not reach each other over UDP on the peer port, 7443
node3: node2 holds it dead, though its agent answers mooctl, so they may not reach each other over UDP on the peer port, 7443
node4: its agent doesn't answer: connection refused
node5: dial tcp: no route to host
`
	if r := o.mustRun("cluster", "members"); r.stdout != want || r.stderr != "" {
		t.Errorf("cluster members printed\n%s\non stderr %q, want\n%s", r.stdout, r.stderr, want)
	}
	o.terminal = &scriptedTerminal{}
	if r := o.mustRun("cluster", "members"); r.stdout != want {
		t.Errorf("cluster members on a terminal printed\n%s\nwant\n%s", r.stdout, want)
	}
}

func TestClusterMembersPrintsJSON(t *testing.T) {
	o := newOperator(t, "op")
	statusCluster(t, o, "node1", "node2", "node3", "node4")
	o.nodes = map[string]*fakeNode{
		"node1": withMembership(&nodev1.Membership{Members: []*nodev1.Member{member("node1", 1, alive)}}),
		"node2": {err: errors.New("no route")},
		"node3": {status: nodeclient.Status{AgentErr: errors.New("refused")}},
		"node4": withMembership(nil),
	}
	want := `{
  "cluster": "prod",
  "nodes": [
    {
      "name": "node1",
      "reachable": true,
      "membership": {
        "members": [
          {
            "node": "node1",
            "address": "10.0.0.1:7443",
            "state": "alive"
          }
        ],
        "health_score": 0
      }
    },
    {
      "name": "node2",
      "reachable": false,
      "error": "no route"
    },
    {
      "name": "node3",
      "reachable": true,
      "membership_problem": "its agent doesn't answer: refused"
    },
    {
      "name": "node4",
      "reachable": true,
      "membership_problem": "its agent reports no membership"
    }
  ]
}
`
	if r := o.mustRun("cluster", "members", "--json"); r.stdout != want {
		t.Errorf("cluster members --json printed\n%s\nwant\n%s", r.stdout, want)
	}
}

func TestClusterMembersTakesNoArguments(t *testing.T) {
	o := newOperator(t, "op")
	statusCluster(t, o, "node1")
	o.nodes = map[string]*fakeNode{"node1": withMembership(nil)}
	if r := o.run("cluster", "members", "node1"); r.err == nil {
		t.Error("cluster members took an argument")
	}
}

// The reason a node can't be reached is quoted before it is shown.
func TestClusterMembersQuotesWhyANodeWasntReached(t *testing.T) {
	o := newOperator(t, "op")
	statusCluster(t, o, "node1")
	o.nodes = map[string]*fakeNode{"node1": {err: errors.New("refused\nnode9  alive")}}
	r := o.mustRun("cluster", "members")
	if strings.Contains(r.stdout, "\nnode9") {
		t.Errorf("cluster members printed a line of the node's:\n%s", r.stdout)
	}
}

// A whole cluster shows no problems, and names of any width line up.
func TestClusterMembersOfAWholeCluster(t *testing.T) {
	o := newOperator(t, "op")
	statusCluster(t, o, "node1")
	o.nodes = map[string]*fakeNode{"node1": withMembership(&nodev1.Membership{Members: []*nodev1.Member{
		{Node: "node1", Address: "10.0.0.1:7443", State: alive}, {Node: "nööööde", Address: "10.0.0.2:7443", State: suspect},
	}})}
	want := `node1
  node1    alive    10.0.0.1:7443
  nööööde  suspect  10.0.0.2:7443
`
	if r := o.mustRun("cluster", "members"); r.stdout != want {
		t.Errorf("cluster members printed\n%s\nwant\n%s", r.stdout, want)
	}
}

func TestClusterMembersSaysWhenANodeReportsNone(t *testing.T) {
	o := newOperator(t, "op")
	statusCluster(t, o, "node1")
	o.nodes = map[string]*fakeNode{"node1": withMembership(nil)}
	want := "node1  no view\n\nnode1: its agent reports no membership\n"
	if r := o.mustRun("cluster", "members"); r.stdout != want {
		t.Errorf("cluster members printed\n%s\nwant\n%s", r.stdout, want)
	}
}

func TestClusterMembersOfNoNodes(t *testing.T) {
	o := newOperator(t, "op")
	statusCluster(t, o)
	if r := o.mustRun("cluster", "members"); r.stdout != "Cluster prod has no nodes\n" {
		t.Errorf("cluster members printed %q", r.stdout)
	}
}

func TestClusterStatusOfAnEmptyView(t *testing.T) {
	o := newOperator(t, "op")
	source := statusCluster(t, o, "node1")
	o.nodes = map[string]*fakeNode{"node1": withMembership(&nodev1.Membership{})}
	r := o.mustRun("--release-source", source, "cluster", "status")
	if want := "  membership: no member, health score 0, no message dropped\n"; !strings.Contains(r.stdout, want) {
		t.Errorf("cluster status printed\n%s\nwant\n%s", r.stdout, want)
	}
}
