package node_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/health"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/node"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire"
)

var t0 = time.Unix(1_000_000, 0)

// testnet runs nodes on virtual time. Every message goes through the
// codec, after a delay, unless cut.
type testnet struct {
	t     *testing.T
	now   time.Time
	nodes map[string]*tnode
	addrs []string
	queue []delivery
	seq   int
	delay time.Duration
	cut   func(from, to string) bool
	// cutStreams drops whole-view syncs too when set, beside cut.
	cutStreams func(from, to string) bool
	// wakeFirst wakes a node before the messages that arrive at the same
	// instant.
	wakeFirst bool
	events    []string
	sent      map[string]int // packets sent, by kind and destination: "nack->n03"
	steps     map[string]int // by node
	started   map[string]int // syncs started (not answers), by node
	// others is how many gossip entries each node sent about members
	// other than itself.
	others map[string]int
}

type tnode struct {
	n      *node.Node
	wake   time.Time
	down   bool
	paused time.Time // takes no steps before then
}

type delivery struct {
	at   time.Time
	seq  int
	from string
	to   string
	data []byte
	sync bool
}

func newNet(t *testing.T, n int, edit func(i int, c *node.Config)) *testnet {
	tn := &testnet{t: t, now: t0, nodes: map[string]*tnode{}, delay: 2 * time.Millisecond, sent: map[string]int{}, steps: map[string]int{}, started: map[string]int{}, others: map[string]int{}}
	for i := range n {
		addr := fmt.Sprintf("n%02d", i)
		c := node.Defaults(member.ID(addr), addr)
		c.Seed = uint64(i + 1)
		if edit != nil {
			edit(i, &c)
		}
		tn.nodes[addr] = &tnode{n: node.New(c, wire.Sizer{}, t0), wake: t0}
		tn.addrs = append(tn.addrs, addr)
	}
	return tn
}

func (tn *testnet) joinAll() {
	for _, a := range tn.addrs[1:] {
		tn.step(a, node.Join{Seeds: []node.Seed{seed(tn.addrs[0])}})
	}
}

func (tn *testnet) step(addr string, in node.Input) {
	nd := tn.nodes[addr]
	r := nd.n.Step(tn.now, in)
	tn.steps[addr]++
	if err := nd.n.Check(); err != nil {
		tn.t.Fatalf("%s at %v: %v", addr, tn.now.Sub(t0), err)
	}
	// Everything due by now is done: the next wake-up is after now.
	if !r.Wake.IsZero() && !r.Wake.After(tn.now) {
		tn.t.Fatalf("%s at %v asked to wake at %v", addr, tn.now.Sub(t0), r.Wake.Sub(t0))
	}
	nd.wake = r.Wake
	for _, e := range r.Events {
		tn.events = append(tn.events, fmt.Sprintf("%s %s %d %s %s %d", tn.now.Sub(t0), addr, e.Kind, e.Member, e.By, e.Value))
	}
	for _, p := range r.Packets {
		tn.pinned(addr, p.To, p.Addr)
		b, err := wire.EncodePacket(p.Packet)
		if err != nil {
			tn.t.Fatal(err)
		}
		if len(b) > 1150 {
			tn.t.Fatalf("a %d-byte packet", len(b))
		}
		tn.sent[fmt.Sprintf("%T->%s", p.Packet.Body, p.Addr)]++
		for _, u := range p.Packet.Gossip {
			if string(u.Member.ID) != addr {
				tn.others[addr]++
			}
		}
		tn.send(addr, p.Addr, b, false)
	}
	for _, s := range r.Syncs {
		tn.pinned(addr, s.To, s.Addr)
		if s.Sync.Reply {
			tn.started[addr]++
		}
		b, err := wire.EncodeSync(s.Sync)
		if err != nil {
			tn.t.Fatalf("%s: %v", addr, err)
		}
		tn.send(addr, s.Addr, b, true)
	}
}

// pinned fails the test for a message that names no member, or a member
// other than the node at its address: in the testnet a node's ID is its
// address.
func (tn *testnet) pinned(from string, to member.ID, addr string) {
	if string(to) != addr {
		tn.t.Fatalf("%s at %v sent to %q meant for %q", from, tn.now.Sub(t0), addr, to)
	}
}

func (tn *testnet) send(from, to string, b []byte, sync bool) {
	if tn.cut != nil && tn.cut(from, to) || sync && tn.cutStreams != nil && tn.cutStreams(from, to) {
		return
	}
	tn.seq++
	tn.queue = append(tn.queue, delivery{at: tn.now.Add(tn.delay), seq: tn.seq, from: from, to: to, data: b, sync: sync})
}

// run advances virtual time to until, delivering messages and wake-ups in
// time order.
func (tn *testnet) run(until time.Time) {
	for {
		next, kind, idx := until, "", -1
		for i, d := range tn.queue {
			if d.at.Before(next) || d.at.Equal(next) && kind == "" {
				next, kind, idx = d.at, "msg", i
			}
		}
		for _, a := range tn.addrs {
			nd := tn.nodes[a]
			at := nd.wake
			if nd.paused.After(at) {
				at = nd.paused
			}
			if !nd.down && !at.IsZero() && (at.Before(next) || tn.wakeFirst && at.Equal(next)) {
				next, kind, idx = at, a, -1
			}
		}
		if kind == "" {
			tn.now = until
			return
		}
		tn.now = next
		if kind != "msg" {
			tn.step(kind, node.Tick{})
			continue
		}
		d := tn.queue[idx]
		tn.queue = slices.Delete(tn.queue, idx, idx+1)
		nd, ok := tn.nodes[d.to]
		if !ok || nd.down {
			continue
		}
		if nd.paused.After(tn.now) {
			// Delivered once it runs again.
			d.at = nd.paused
			tn.queue = append(tn.queue, d)
			continue
		}
		if d.sync {
			s, err := wire.DecodeSync(d.data)
			if err != nil {
				tn.t.Fatal(err)
			}
			tn.step(d.to, node.ReceiveSync{Addr: d.from, Sync: s})
		} else {
			p, err := wire.DecodePacket(d.data)
			if err != nil {
				tn.t.Fatal(err)
			}
			tn.step(d.to, node.Receive{Addr: d.from, Packet: p})
		}
	}
}

func (tn *testnet) after(d time.Duration) { tn.run(tn.now.Add(d)) }

// state is what observer thinks of id.
func (tn *testnet) state(observer, id string) (member.Member, bool) {
	for _, m := range tn.nodes[observer].n.Members() {
		if string(m.ID) == id {
			return m, true
		}
	}
	return member.Member{}, false
}

// everyone checks that every running node but except sees id in state s.
func (tn *testnet) everyone(id string, s member.State, except ...string) bool {
	for _, a := range tn.addrs {
		if tn.nodes[a].down || a == id || slices.Contains(except, a) {
			continue
		}
		if m, ok := tn.state(a, id); !ok || m.State != s {
			return false
		}
	}
	return true
}

func (tn *testnet) declaredDead() []string {
	var out []string
	for _, e := range tn.events {
		if f := strings.Fields(e); f[2] == fmt.Sprint(int(node.Declared)) {
			out = append(out, e)
		}
	}
	return out
}

func (tn *testnet) converged() bool {
	for _, a := range tn.addrs {
		if !tn.everyone(a, member.Alive) {
			return false
		}
	}
	return true
}

func TestJoinConverges(t *testing.T) {
	tn := newNet(t, 12, nil)
	tn.joinAll()
	tn.after(20 * time.Second)
	if !tn.converged() {
		t.Fatal("not every node sees every other alive after 20s")
	}
	if d := tn.declaredDead(); len(d) != 0 {
		t.Fatalf("declared dead with no failure: %v", d)
	}
}

func TestCrashIsDetected(t *testing.T) {
	tn := newNet(t, 12, nil)
	tn.joinAll()
	tn.after(20 * time.Second)
	tn.nodes["n05"].down = true
	crashed := tn.now
	for tn.now.Sub(crashed) < 2*time.Minute && !tn.everyone("n05", member.Dead) {
		tn.after(time.Second)
	}
	if !tn.everyone("n05", member.Dead) {
		t.Fatal("n05 not dead everywhere 2 minutes after it crashed")
	}
	// Lifeguard's shortest timeout among 12 is 4 × log10 12 ≈ 4.3 periods,
	// its longest 6 times that; confirmations bring it near the shortest.
	if took := tn.now.Sub(crashed); took > 40*time.Second {
		t.Errorf("detection took %v", took)
	}
	for _, e := range tn.declaredDead() {
		if f := strings.Fields(e); f[3] != "n05" {
			t.Fatalf("a live member declared dead: %s", e)
		}
	}
}

func TestSuspicionIsRefuted(t *testing.T) {
	tn := newNet(t, 6, nil)
	tn.joinAll()
	tn.after(15 * time.Second)
	m, _ := tn.state("n01", "n03")
	m.State = member.Suspect
	// A suspicion raised by n04 reaches n01 from nowhere.
	tn.step("n01", node.Receive{Addr: "n04", Packet: msg.Packet{
		From: tn.nodes["n04"].n.Self(), Body: msg.Ack{}, Gossip: []msg.Update{{Member: m, By: "n04"}},
	}})
	tn.after(20 * time.Second)
	if !tn.everyone("n03", member.Alive) {
		t.Fatal("the suspicion wasn't refuted")
	}
	if self := tn.nodes["n03"].n.Self(); self.Incarnation == 0 {
		t.Fatal("n03 refuted without raising its incarnation")
	}
	if d := tn.declaredDead(); len(d) != 0 {
		t.Fatalf("declared dead: %v", d)
	}
}

func TestLeave(t *testing.T) {
	tn := newNet(t, 6, nil)
	tn.joinAll()
	tn.after(15 * time.Second)
	tn.step("n02", node.Leave{})
	tn.after(10 * time.Second)
	if !tn.everyone("n02", member.Left) {
		t.Fatal("not everyone sees n02 left")
	}
}

// A paused node comes back, notices, and doesn't suspect the members whose
// answers queued up meanwhile.
func TestPauseIsNotAFailure(t *testing.T) {
	tn := newNet(t, 6, nil)
	tn.joinAll()
	tn.after(15 * time.Second)
	// Shorter than the shortest suspicion timeout at 6 nodes, 4 ×
	// max(1, log10 6) = 4 periods.
	tn.nodes["n02"].paused = tn.now.Add(3 * time.Second)
	tn.after(30 * time.Second)
	stalled := false
	for _, e := range tn.events {
		f := strings.Fields(e)
		if f[1] == "n02" && f[2] == fmt.Sprint(int(node.Stalled)) {
			stalled = true
		}
		if f[1] == "n02" && f[2] == fmt.Sprint(int(node.Suspected)) && f[4] == "n02" {
			t.Fatalf("n02 suspected a member after its pause: %s", e)
		}
	}
	if !stalled {
		t.Fatal("n02 didn't notice its pause")
	}
	if d := tn.declaredDead(); len(d) != 0 {
		t.Fatalf("declared dead: %v", d)
	}
}

// Each side of a partition declares the other dead; once it heals, healing
// probes and syncs bring every member back alive.
func TestPartitionHeals(t *testing.T) {
	tn := newNet(t, 8, func(_ int, c *node.Config) { c.SyncEvery = 10 })
	tn.joinAll()
	tn.after(15 * time.Second)
	side := func(a string) bool { return a < "n04" }
	tn.cut = func(from, to string) bool { return side(from) != side(to) }
	tn.after(2 * time.Minute)
	if !tn.everyone("n06", member.Dead, "n04", "n05", "n07") {
		t.Fatal("the other side of the partition isn't dead")
	}
	tn.cut = nil
	tn.after(2 * time.Minute)
	if !tn.converged() {
		for _, a := range tn.addrs {
			t.Log(a, tn.nodes[a].n.Members())
		}
		t.Fatal("not converged 2 minutes after the partition healed")
	}
}

// A member of an older version without CapNack is never sent a nack.
func TestNoNackToAnOlderVersion(t *testing.T) {
	tn := newNet(t, 6, func(i int, c *node.Config) {
		if i == 1 {
			c.Caps = 0
		}
	})
	tn.joinAll()
	tn.after(15 * time.Second)
	tn.nodes["n05"].down = true
	tn.after(time.Minute)
	if n := tn.sent["msg.Nack->n01"]; n != 0 {
		t.Fatalf("%d nacks sent to the node without CapNack", n)
	}
	nacks := 0
	for k, n := range tn.sent {
		if strings.HasPrefix(k, "msg.Nack->") {
			nacks += n
		}
	}
	if nacks == 0 {
		t.Fatal("no nacks at all: the test shows nothing")
	}
}

// The same seeds give the same run, event for event.
func TestDeterministic(t *testing.T) {
	trace := func() []string {
		tn := newNet(t, 8, nil)
		tn.joinAll()
		tn.after(10 * time.Second)
		tn.nodes["n03"].down = true
		tn.after(40 * time.Second)
		return tn.events
	}
	a, b := trace(), trace()
	if !slices.Equal(a, b) {
		t.Fatal("two runs with the same seeds differ")
	}
	if len(a) == 0 {
		t.Fatal("no events")
	}
}

// With random loss, nobody alive is declared dead and the cluster stays
// converged. The test counts what the loss dropped.
func TestLossCausesNoFalseDeaths(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	tn := newNet(t, 10, nil)
	tn.joinAll()
	tn.after(15 * time.Second)
	dropped := 0
	tn.cut = func(_, _ string) bool {
		if r.Float64() < 0.05 {
			dropped++
			return true
		}
		return false
	}
	tn.after(3 * time.Minute)
	if d := tn.declaredDead(); len(d) != 0 {
		t.Fatalf("declared dead with 5%% loss: %v", d)
	}
	if dropped < 100 {
		t.Fatalf("the loss dropped %d packets in three minutes: it shows nothing", dropped)
	}
}

// A node that gossips an entry older than the receiver's gets the newer
// one back in the ack.
func TestAStaleSenderIsCorrected(t *testing.T) {
	tn := newNet(t, 4, nil)
	tn.joinAll()
	tn.after(10 * time.Second)
	// n02 refutes a suspicion, and the refutation spreads until every
	// gossip queue has sent it as often as it may.
	suspect := tn.nodes["n02"].n.Self()
	suspect.State = member.Suspect
	tn.step("n02", node.Receive{Addr: "n03", Packet: msg.Packet{
		From: tn.nodes["n03"].n.Self(), Body: msg.Ack{}, Gossip: []msg.Update{{Member: suspect, By: "n03"}},
	}})
	tn.after(time.Minute)
	known, _ := tn.state("n01", "n02")
	if known.Incarnation == 0 || known.State != member.Alive {
		t.Fatalf("n01 has %v", known)
	}
	// n03 pings n01 with the old suspicion, as a node that missed the
	// refutation does.
	r := tn.nodes["n01"].n.Step(tn.now, node.Receive{Addr: "n03", Packet: msg.Packet{
		From: tn.nodes["n03"].n.Self(), Body: msg.Ping{Seq: 9, Target: "n01"}, Gossip: []msg.Update{{Member: suspect, By: "n03"}},
	}})
	for _, p := range r.Packets {
		if _, ok := p.Packet.Body.(msg.Ack); !ok || p.To != "n03" {
			continue
		}
		for _, u := range p.Packet.Gossip {
			if u.Member == known {
				return
			}
		}
		t.Fatalf("the ack doesn't carry n02's newer entry: %v", p.Packet.Gossip)
	}
	t.Fatalf("no ack to n03 in %v", r.Packets)
}

// A join whose sync is lost is tried again until the node hears of a
// member.
func TestALostJoinIsRetried(t *testing.T) {
	tn := newNet(t, 3, nil)
	lost := 0
	tn.cut = func(from, to string) bool {
		if from != "n00" && to == "n00" && lost < 4 {
			lost++
			return true
		}
		return false
	}
	tn.joinAll()
	tn.after(20 * time.Second)
	if lost != 4 {
		t.Fatalf("%d joins lost, want 4", lost)
	}
	if !tn.converged() {
		t.Fatal("not converged after the first joins were lost")
	}
}

// A suspicion learned from a whole-view sync isn't the sync sender's own:
// it doesn't count as a confirmation.
func TestASyncIsNotAConfirmation(t *testing.T) {
	tn := newNet(t, 6, nil)
	tn.joinAll()
	tn.after(15 * time.Second)
	suspect := tn.nodes["n03"].n.Self()
	suspect.State = member.Suspect
	tn.step("n01", node.Receive{Addr: "n02", Packet: msg.Packet{
		From: tn.nodes["n02"].n.Self(), Body: msg.Ack{}, Gossip: []msg.Update{{Member: suspect, By: "n02"}},
	}})
	r := tn.nodes["n01"].n.Step(tn.now, node.ReceiveSync{Addr: "n04", Sync: msg.Sync{
		From: tn.nodes["n04"].n.Self(), View: []member.Member{suspect},
	}})
	for _, e := range r.Events {
		if e.Kind == node.Confirmed {
			t.Fatalf("a sync counted as a confirmation: %+v", e)
		}
	}
}

// A stale entry on a packet that isn't a ping is corrected too, by a
// gossip packet back to the sender.
func TestAStaleEntryOnAnAckIsCorrected(t *testing.T) {
	tn := newNet(t, 4, nil)
	tn.joinAll()
	tn.after(10 * time.Second)
	suspect := tn.nodes["n02"].n.Self()
	suspect.State = member.Suspect
	tn.step("n02", node.Receive{Addr: "n03", Packet: msg.Packet{
		From: tn.nodes["n03"].n.Self(), Body: msg.Ack{}, Gossip: []msg.Update{{Member: suspect, By: "n03"}},
	}})
	tn.after(time.Minute)
	known, _ := tn.state("n01", "n02")
	r := tn.nodes["n01"].n.Step(tn.now, node.Receive{Addr: "n03", Packet: msg.Packet{
		From: tn.nodes["n03"].n.Self(), Body: msg.Ack{Seq: 1}, Gossip: []msg.Update{{Member: suspect, By: "n03"}},
	}})
	for _, p := range r.Packets {
		if _, ok := p.Packet.Body.(msg.Gossip); ok && p.To == "n03" && slices.ContainsFunc(p.Packet.Gossip, func(u msg.Update) bool { return u.Member == known }) {
			return
		}
	}
	t.Fatalf("no gossip packet corrects n03: %+v", r.Packets)
}

// To each member a node writes the highest wire version both read; to one
// that reads none it writes, nothing, and it says so once.
func TestVersionPerMember(t *testing.T) {
	tn := newNet(t, 4, func(i int, c *node.Config) {
		switch i {
		case 1:
			c.MinVersion, c.Version = 1, 2
		case 2:
			c.MinVersion, c.Version = 1, 2
		case 3:
			c.MinVersion, c.Version = 2, 2
		}
	})
	n1 := tn.nodes["n01"].n
	versions := map[string]uint8{}
	for _, addr := range []string{"n00", "n02", "n03"} {
		id := member.ID(addr)
		e := tn.nodes[addr].n.Self()
		n1.Step(tn.now, node.Receive{Addr: addr, Packet: msg.Packet{From: e, Body: msg.Ack{}}})
		r := n1.Step(tn.now, node.Receive{Addr: addr, Packet: msg.Packet{From: e, Body: msg.Ping{Seq: 1, Target: "n01"}}})
		for _, p := range r.Packets {
			if p.To == id {
				versions[addr] = p.Version
			}
		}
	}
	if versions["n00"] != 1 || versions["n02"] != 2 || versions["n03"] != 2 {
		t.Fatalf("versions written %v, want n00 1, n02 2, n03 2", versions)
	}
	// n00 reads only version 1, n03 only version 2: they can't talk.
	n0 := tn.nodes["n00"].n
	var events []node.Event
	events = append(events, n0.Step(tn.now, node.Receive{Addr: "n03", Packet: msg.Packet{From: tn.nodes["n03"].n.Self(), Body: msg.Ack{}}}).Events...)
	for range 2 {
		r := n0.Step(tn.now, node.Receive{Addr: "n03", Packet: msg.Packet{From: tn.nodes["n03"].n.Self(), Body: msg.Ping{Seq: 2, Target: "n00"}}})
		for _, p := range r.Packets {
			if p.To == "n03" {
				t.Fatalf("wrote version %d to a member that reads only 2", p.Version)
			}
		}
		events = append(events, r.Events...)
	}
	incompatible := 0
	for _, e := range events {
		if e.Kind == node.Incompatible && e.Member == "n03" {
			incompatible++
		}
	}
	if incompatible != 1 {
		t.Fatalf("%d Incompatible events, want 1", incompatible)
	}
}

// A node restarted with other capabilities or wire versions supersedes the
// entry its earlier run left at the same incarnation.
func TestARestartSupersedesTheOldEntry(t *testing.T) {
	for _, tc := range []struct {
		name          string
		before, after uint8 // the highest wire version, before and after
	}{
		// The newer entry outranks the old in the order at one
		// incarnation; the older one doesn't, so the node supersedes it.
		{"up", 1, 2},
		{"down", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tn := newNet(t, 4, func(i int, c *node.Config) {
				if i == 2 {
					c.Version = tc.before
				}
			})
			tn.joinAll()
			tn.after(10 * time.Second)
			c := node.Defaults("n02", "n02")
			c.MinVersion, c.Version = 1, tc.after
			c.Seed = 99
			tn.nodes["n02"].n = node.New(c, wire.Sizer{}, tn.now)
			tn.nodes["n02"].wake = tn.now
			tn.step("n02", node.Join{Seeds: []node.Seed{seed("n00")}})
			tn.after(10 * time.Second)
			for _, a := range []string{"n00", "n01", "n03"} {
				if m, _ := tn.state(a, "n02"); m.Version != tc.after {
					t.Fatalf("%s still has %+v", a, m)
				}
			}
		})
	}
}

// A message in a version the node doesn't read is answered with the node's
// own entry, once a period per address, to a member or a seed, never to a
// stranger. Every one is an event.
func TestUnreadableIsAnsweredWithTheEntry(t *testing.T) {
	tn := newNet(t, 2, nil)
	tn.joinAll()
	tn.after(5 * time.Second)
	n := tn.nodes["n00"].n
	acks := func(r node.Result, addr string) []node.Packet {
		var out []node.Packet
		for _, p := range r.Packets {
			if _, ok := p.Packet.Body.(msg.Ack); ok && p.Addr == addr {
				out = append(out, p)
			}
		}
		return out
	}
	r := n.Step(tn.now, node.Unreadable{Addr: "n01", Version: 3})
	if a := acks(r, "n01"); len(a) != 1 || a[0].Version != 1 || a[0].Packet.From != n.Self() {
		t.Fatalf("Packets = %+v", r.Packets)
	}
	if !slices.ContainsFunc(r.Events, func(e node.Event) bool { return e.Kind == node.WrongVersion && e.Value == 3 }) {
		t.Fatalf("no WrongVersion event: %+v", r.Events)
	}
	if r := n.Step(tn.now.Add(500*time.Millisecond), node.Unreadable{Addr: "n01"}); len(acks(r, "n01")) != 0 {
		t.Fatal("answered twice in a period")
	}
	if r := n.Step(tn.now.Add(time.Second), node.Unreadable{Addr: "n01"}); len(acks(r, "n01")) != 1 {
		t.Fatal("not answered after a period")
	}
	for _, p := range n.Step(tn.now.Add(2*time.Second), node.Unreadable{Addr: "stranger"}).Packets {
		if p.Addr == "stranger" {
			t.Fatal("answered a stranger")
		}
	}
	// A seed is answered before it is known as a member, named.
	m := tn.nodes["n01"].n
	m.Step(tn.now, node.Join{Seeds: []node.Seed{{ID: "s1", Addr: "seed:1"}}})
	if r := m.Step(tn.now.Add(time.Second), node.Unreadable{Addr: "seed:1"}); !slices.ContainsFunc(r.Packets, func(p node.Packet) bool { return p.Addr == "seed:1" && p.To == "s1" }) {
		t.Fatalf("a seed wasn't answered by name: %+v", r.Packets)
	}
}

// A view holds at most MaxMembers: forged entries of new members beyond it
// are ignored.
func TestMaxMembers(t *testing.T) {
	tn := newNet(t, 2, func(_ int, c *node.Config) { c.MaxMembers = 5 })
	n := tn.nodes["n00"].n
	var forged []msg.Update
	for i := range 20 {
		forged = append(forged, msg.Update{Member: member.Member{ID: member.ID(fmt.Sprintf("f%02d", i)), Addr: "x"}})
	}
	n.Step(tn.now, node.Receive{Addr: "n01", Packet: msg.Packet{From: tn.nodes["n01"].n.Self(), Body: msg.Ack{}, Gossip: forged}})
	if got := len(n.Members()); got != 5 {
		t.Fatalf("%d members, want 5", got)
	}
}

// Nodes of one seed but different IDs draw differently: nodes built from
// Defaults and started together don't sync at the same moment or probe in
// the same order.
func TestNodesOfOneSeedDrawDifferently(t *testing.T) {
	order := func(id string) []member.ID {
		c := node.Defaults(member.ID(id), id)
		n := node.New(c, wire.Sizer{}, t0)
		var ups []msg.Update
		for i := range 8 {
			ups = append(ups, msg.Update{Member: member.Member{ID: member.ID(fmt.Sprintf("m%d", i)), Addr: fmt.Sprint(i)}})
		}
		r := n.Step(t0, node.Receive{Addr: "0", Packet: msg.Packet{From: ups[0].Member, Body: msg.Ack{}, Gossip: ups}})
		var out []member.ID
		for r.Wake.Before(t0.Add(8 * time.Second)) {
			r = n.Step(r.Wake, node.Tick{})
			for _, p := range r.Packets {
				if _, ok := p.Packet.Body.(msg.Ping); ok {
					out = append(out, p.To)
				}
			}
		}
		return out
	}
	if a, b := order("node-a"), order("node-b"); slices.Equal(a, b) {
		t.Fatalf("node-a and node-b probe in the same order: %v", a)
	}
}

// A probe that failed against a member's old run suspects nothing when the
// member restarted meanwhile, at a new address and incarnation.
func TestAFailedProbeOfAnOldRunSuspectsNotTheNew(t *testing.T) {
	c := node.Defaults("a", "a")
	n := node.New(c, wire.Sizer{}, t0)
	old := member.Member{ID: "b", Addr: "b1", Incarnation: 1, MinVersion: 1, Version: 1}
	n.Step(t0, node.Receive{Addr: "b1", Packet: msg.Packet{From: old, Body: msg.Ack{}}})
	now := t0
	for range 3 {
		now = now.Add(c.Probe.Interval)
		n.Step(now, node.Tick{})
		if id, ok := n.Probing(); ok && id == "b" {
			break
		}
	}
	restarted := member.Member{ID: "b", Addr: "b2", Incarnation: 5000, MinVersion: 1, Version: 1}
	n.Step(now.Add(10*time.Millisecond), node.Receive{Addr: "b2", Packet: msg.Packet{From: restarted, Body: msg.Ack{}}})
	for range 3 {
		now = now.Add(c.Probe.Interval)
		n.Step(now, node.Tick{})
	}
	if m, _ := n.Member("b"); m.State != member.Alive || m.Addr != "b2" {
		t.Fatalf("the new run of b is %v", m)
	}
}

// A sender first heard in this packet gets an ack without the gossip
// queue's updates.
func TestAStrangerSpendsNoGossip(t *testing.T) {
	tn := newNet(t, 3, nil)
	tn.joinAll()
	tn.after(5 * time.Second)
	n := tn.nodes["n00"].n
	fresh := msg.Update{Member: member.Member{ID: "fresh", Addr: "fresh"}}
	n.Step(tn.now, node.Receive{Addr: "n01", Packet: msg.Packet{From: tn.nodes["n01"].n.Self(), Body: msg.Ack{}, Gossip: []msg.Update{fresh}}})
	for i := range 20 {
		stranger := member.Member{ID: member.ID(fmt.Sprintf("s%d", i)), Addr: "victim"}
		r := n.Step(tn.now, node.Receive{Addr: "victim", Packet: msg.Packet{From: stranger, Body: msg.Ping{Seq: 1, Target: "n00"}}})
		for _, p := range r.Packets {
			if p.Addr == "victim" && slices.ContainsFunc(p.Packet.Gossip, func(u msg.Update) bool { return u.Member.ID == "fresh" }) {
				t.Fatalf("an ack to a sender first heard carried the queue's %v", p.Packet.Gossip)
			}
		}
	}
}

// A node left alone doesn't wake for its gossip timer: with nobody to send
// to, it sleeps until its probes.
func TestALoneNodeSleeps(t *testing.T) {
	tn := newNet(t, 2, nil)
	tn.joinAll()
	tn.after(5 * time.Second)
	tn.nodes["n01"].down = true
	tn.after(2 * time.Hour)
	steps := 0
	n := tn.nodes["n00"]
	now := tn.now
	for now.Before(tn.now.Add(time.Minute)) {
		now = n.wake
		r := n.n.Step(now, node.Tick{})
		n.wake = r.Wake
		steps++
	}
	if steps > 70 {
		t.Fatalf("%d wake-ups in a minute alone", steps)
	}
}

// A node that left joins again at a new incarnation, and is alive to the
// others.
func TestRejoinAfterLeave(t *testing.T) {
	tn := newNet(t, 4, nil)
	tn.joinAll()
	tn.after(10 * time.Second)
	tn.step("n02", node.Leave{})
	tn.after(10 * time.Second)
	tn.step("n02", node.Join{Seeds: []node.Seed{seed("n00")}})
	tn.after(20 * time.Second)
	if !tn.everyone("n02", member.Alive) {
		t.Fatal("n02 isn't alive again after rejoining")
	}
}

// MaxMembers is a hard cap: a full view with nobody gone refuses new
// members' own entries as well as third parties', and says so. A member
// kept out gets in once a gone member makes room.
func TestAFullViewIsAHardCap(t *testing.T) {
	tn := newNet(t, 2, func(_ int, c *node.Config) { c.MaxMembers = 5 })
	n := tn.nodes["n00"].n
	var forged []msg.Update
	for i := range 3 {
		forged = append(forged, msg.Update{Member: member.Member{ID: member.ID(fmt.Sprintf("f%02d", i)), Addr: "x"}})
	}
	n.Step(tn.now, node.Receive{Addr: "n01", Packet: msg.Packet{From: tn.nodes["n01"].n.Self(), Body: msg.Ack{}, Gossip: forged}})
	for i := range 50 {
		sender := member.Member{ID: member.ID(fmt.Sprintf("s%02d", i)), Addr: fmt.Sprint(i), MinVersion: 1, Version: 1}
		r := n.Step(tn.now, node.Receive{Addr: sender.Addr, Packet: msg.Packet{From: sender, Body: msg.Ping{Target: "n00"}}})
		if !slices.ContainsFunc(r.Events, func(e node.Event) bool { return e.Kind == node.Refused && e.Member == sender.ID }) {
			t.Fatalf("sender %d: refused without an event", i)
		}
		n.Step(tn.now, node.ReceiveSync{Addr: sender.Addr, Sync: msg.Sync{From: sender, View: []member.Member{sender}, Reply: true}})
	}
	if got := len(n.Members()); got != 5 {
		t.Fatalf("%d members, want 5", got)
	}
	dead := forged[0]
	dead.Member.State = member.Dead
	n.Step(tn.now, node.Receive{Addr: "n01", Packet: msg.Packet{From: tn.nodes["n01"].n.Self(), Body: msg.Ack{}, Gossip: []msg.Update{dead}}})
	joiner := member.Member{ID: "joiner", Addr: "j", MinVersion: 1, Version: 1}
	n.Step(tn.now, node.Receive{Addr: "j", Packet: msg.Packet{From: joiner, Body: msg.Ping{Target: "n00"}}})
	if _, ok := n.Member("joiner"); !ok {
		t.Fatal("a member was kept out after a gone member made room")
	}
	if got := len(n.Members()); got != 5 {
		t.Fatalf("%d members, want 5", got)
	}
}

func TestValidate(t *testing.T) {
	good := node.Defaults("a", "a")
	if err := good.Validate(wire.Sizer{}); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*node.Config){
		"no ID":            func(c *node.Config) { c.ID = "" },
		"a long ID":        func(c *node.Config) { c.ID = member.ID(strings.Repeat("x", 256)) },
		"a long address":   func(c *node.Config) { c.Addr = strings.Repeat("x", 256) },
		"version 0":        func(c *node.Config) { c.MinVersion = 0 },
		"versions reverse": func(c *node.Config) { c.MinVersion, c.Version = 2, 1 },
		"no interval":      func(c *node.Config) { c.Probe.Interval = 0 },
		"negative sync":    func(c *node.Config) { c.SyncEvery = -1 },
	} {
		c := good
		edit(&c)
		if err := c.Validate(wire.Sizer{}); err == nil {
			t.Errorf("%s: valid", name)
		}
	}
	if err := good.Validate(nil); err == nil {
		t.Error("no sizer: valid")
	}
}

// A pause that ends on the node's own timer, before it reads the acks
// that queued up, is still noticed: its deadlines move on, and nobody
// whose ack waits is suspected.
func TestAPauseEndingOnTheTimer(t *testing.T) {
	tn := newNet(t, 6, nil)
	tn.wakeFirst = true
	tn.joinAll()
	tn.after(15 * time.Second)
	for range 20 {
		tn.nodes["n02"].paused = tn.now.Add(3 * time.Second)
		tn.after(10 * time.Second)
	}
	for _, e := range tn.events {
		if f := strings.Fields(e); f[1] == "n02" && f[2] == fmt.Sprint(int(node.Suspected)) && f[4] == "n02" {
			t.Fatalf("n02 suspected a member after a pause: %s", e)
		}
	}
}

// A node restarted with its clock behind its earlier run, and rolled back
// to a lower wire version, still gets back in: members answer its own,
// older entry with the earlier run's, in a version it reads, and it
// supersedes that.
func TestARestartBehindAndRolledBack(t *testing.T) {
	tn := newNet(t, 4, func(i int, c *node.Config) {
		if i == 2 {
			c.Incarnation, c.Version = 1_000_000, 2
		}
	})
	tn.joinAll()
	tn.after(10 * time.Second)
	c := node.Defaults("n02", "n02")
	c.Incarnation, c.Seed = 500_000, 7
	tn.nodes["n02"].n = node.New(c, wire.Sizer{}, tn.now)
	tn.nodes["n02"].wake = tn.now
	tn.step("n02", node.Join{Seeds: []node.Seed{seed("n00")}})
	tn.after(30 * time.Second)
	for _, a := range []string{"n00", "n01", "n03"} {
		if m, _ := tn.state(a, "n02"); m.State != member.Alive || m.Version != 1 || m.Incarnation <= 1_000_000 {
			t.Fatalf("%s has %v", a, m)
		}
	}
}

// A leave whose whole-view syncs are all lost still reaches everyone, on
// the gossip packets the node sends with them: it reads as a leave, not a
// death.
func TestALeaveWithItsSyncsLost(t *testing.T) {
	tn := newNet(t, 6, nil)
	tn.joinAll()
	tn.after(15 * time.Second)
	tn.cutStreams = func(from, _ string) bool { return from == "n02" }
	tn.step("n02", node.Leave{})
	tn.nodes["n02"].down = true
	tn.after(20 * time.Second)
	if !tn.everyone("n02", member.Left) {
		t.Fatal("not everyone sees n02 left")
	}
}

// A sync to a seed the node knows reads no version it writes is said, not
// silent.
func TestASyncToAnIncompatibleSeedIsReported(t *testing.T) {
	c := node.Defaults("a", "a")
	n := node.New(c, wire.Sizer{}, t0)
	newer := member.Member{ID: "s", Addr: "s", MinVersion: 2, Version: 2}
	if r := hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: newer, By: "b"}); hasEvent(r, node.Incompatible) {
		t.Fatalf("a member heard of raised %v", r.Events)
	}
	r := n.Step(t0, node.Join{Seeds: []node.Seed{{ID: "s", Addr: "s"}}})
	if len(r.Syncs) != 0 || !slices.ContainsFunc(r.Events, func(e node.Event) bool { return e.Kind == node.Incompatible && e.Member == "s" }) {
		t.Fatalf("syncs %v, events %v", r.Syncs, r.Events)
	}
}

// In a cluster of three, one confirmation is all there can be, and it
// brings the suspicion to its shortest: a crash is found in about that.
func TestASmallClusterFindsACrashAtTheShortestTimeout(t *testing.T) {
	tn := newNet(t, 3, nil)
	tn.joinAll()
	tn.after(10 * time.Second)
	tn.nodes["n02"].down = true
	crashed := tn.now
	for tn.now.Sub(crashed) < time.Minute && !tn.everyone("n02", member.Dead) {
		tn.after(100 * time.Millisecond)
	}
	// Shortest 4 periods, plus up to two rounds to find it and a period
	// for the probe to fail: well under the longest, 24.
	if took := tn.now.Sub(crashed); took > 12*time.Second {
		t.Fatalf("detection took %v", took)
	}
}

// Entries a sync brings about members the node didn't know aren't
// gossiped on.
func TestASyncsNewMembersArentGossiped(t *testing.T) {
	c := node.Defaults("a", "a")
	n := node.New(c, wire.Sizer{}, t0)
	seed := member.Member{ID: "seed", Addr: "seed", MinVersion: 1, Version: 1, Caps: c.Caps}
	view := []member.Member{seed}
	for i := range 5 {
		view = append(view, member.Member{ID: member.ID(fmt.Sprintf("m%d", i)), Addr: fmt.Sprint(i), MinVersion: 1, Version: 1})
	}
	n.Step(t0, node.ReceiveSync{Addr: "seed", Sync: msg.Sync{From: seed, View: view}})
	r := n.Step(t0, node.Receive{Addr: "seed", Packet: msg.Packet{From: seed, Body: msg.Ping{Seq: 1, Target: "a"}}})
	for _, p := range r.Packets {
		for _, u := range p.Packet.Gossip {
			if strings.HasPrefix(string(u.Member.ID), "m") {
				t.Fatalf("gossiped %v, learned from a sync", u.Member)
			}
		}
	}
}

// A quiet cluster sends no gossip packets: the timer only runs while there
// are updates.
func TestAQuietClusterSendsNoGossipPackets(t *testing.T) {
	tn := newNet(t, 5, nil)
	tn.joinAll()
	tn.after(time.Minute)
	before := 0
	for k, v := range tn.sent {
		if strings.HasPrefix(k, "msg.Gossip->") {
			before += v
		}
	}
	tn.after(time.Minute)
	after := 0
	for k, v := range tn.sent {
		if strings.HasPrefix(k, "msg.Gossip->") {
			after += v
		}
	}
	if after != before {
		t.Fatalf("%d gossip packets in a quiet minute", after-before)
	}
}

// A node that left relays no ping-req, sends nothing on a second Leave,
// answers a ping where it came from, and needs no wake-up once nothing
// it times is left.
func TestWhatALeftNodeDoes(t *testing.T) {
	tn := newNet(t, 3, nil)
	tn.joinAll()
	tn.after(10 * time.Second)
	n := tn.nodes["n01"].n
	n.Step(tn.now, node.Leave{})
	r := n.Step(tn.now, node.Leave{})
	if len(r.Packets)+len(r.Syncs) != 0 {
		t.Fatalf("a second Leave sent %v %v", r.Packets, r.Syncs)
	}
	r = n.Step(tn.now, node.Receive{Addr: "n00", Packet: msg.Packet{From: tn.nodes["n00"].n.Self(), Body: msg.PingReq{Seq: 3, Target: "n02"}}})
	for _, p := range r.Packets {
		if _, ok := p.Packet.Body.(msg.Ping); ok {
			t.Fatal("a node that left relayed a ping-req")
		}
	}
	r = n.Step(tn.now, node.Receive{Addr: "elsewhere:9", Packet: msg.Packet{From: tn.nodes["n00"].n.Self(), Body: msg.Ping{Seq: 4, Target: "n01"}}})
	if len(r.Packets) != 1 || r.Packets[0].Addr != "elsewhere:9" {
		t.Fatalf("the ack went to %v, want where the ping came from", r.Packets)
	}
	if !r.Wake.IsZero() {
		t.Fatalf("a node that left with nothing to time asks to wake at %v", r.Wake)
	}
}

// An entry with an incarnation past the most is ignored.
func TestAnIncarnationPastTheMostIsIgnored(t *testing.T) {
	tn := newNet(t, 3, nil)
	tn.joinAll()
	tn.after(5 * time.Second)
	forged := tn.nodes["n02"].n.Self()
	forged.State, forged.Incarnation = member.Dead, member.MaxIncarnation+1
	for _, a := range []string{"n00", "n02"} {
		tn.step(a, node.Receive{Addr: "n01", Packet: msg.Packet{From: tn.nodes["n01"].n.Self(), Body: msg.Ack{}, Gossip: []msg.Update{{Member: forged}}}})
	}
	if m, _ := tn.state("n00", "n02"); m.State != member.Alive {
		t.Fatalf("n00 took the forged entry: %v", m)
	}
	if self := tn.nodes["n02"].n.Self(); self.Incarnation > member.MaxIncarnation {
		t.Fatalf("n02 refuted to %d", self.Incarnation)
	}
}

// A flood of unreadable messages from spoofed addresses is answered to
// none of them, and costs little.
func TestAnUnreadableFloodIsCheap(t *testing.T) {
	n := node.New(node.Defaults("a", "a"), wire.Sizer{}, t0)
	start := time.Now()
	for i := range 20000 {
		r := n.Step(t0.Add(time.Duration(i)*time.Microsecond), node.Unreadable{Addr: fmt.Sprintf("spoof:%d", i)})
		for _, p := range r.Packets {
			if strings.HasPrefix(p.Addr, "spoof:") {
				t.Fatal("answered a spoofed address")
			}
		}
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("20,000 unreadable messages took %v", took)
	}
}

// A node that hears an entry of its own incarnation another run left
// supersedes it, and says so.
func TestSupersedingIsAnEvent(t *testing.T) {
	n := node.New(node.Defaults("a", "a"), wire.Sizer{}, t0)
	twin := n.Self()
	twin.Addr = "elsewhere"
	r := n.Step(t0, node.Receive{Addr: "b", Packet: msg.Packet{From: member.Member{ID: "b", Addr: "b", MinVersion: 1, Version: 1}, Body: msg.Ack{}, Gossip: []msg.Update{{Member: twin}}}})
	if !slices.ContainsFunc(r.Events, func(e node.Event) bool { return e.Kind == node.Superseded && e.Incarnation == 1 }) {
		t.Fatalf("events %v", r.Events)
	}
}

// An entry at the incarnation bound, which moves with the clock, can be
// refuted: the member's answer is within the bound a moment later, and the
// member keeps sending. One past it is ignored.
func TestAnEntryAtTheBoundIsRefuted(t *testing.T) {
	tn := newNet(t, 3, nil)
	tn.joinAll()
	tn.after(5 * time.Second)
	bound := uint64(tn.now.UnixMilli()) + 1<<32
	forged := tn.nodes["n02"].n.Self()
	forged.State, forged.Incarnation = member.Suspect, bound
	tn.step("n00", node.Receive{Addr: "n01", Packet: msg.Packet{From: tn.nodes["n01"].n.Self(), Body: msg.Ack{}, Gossip: []msg.Update{{Member: forged}}}})
	tn.after(10 * time.Second)
	if m, _ := tn.state("n00", "n02"); m.State != member.Alive || m.Incarnation <= bound {
		t.Fatalf("n00 has %v", m)
	}
	past := forged
	past.Incarnation = uint64(tn.now.UnixMilli()) + 1<<32 + 1000
	r := tn.nodes["n00"].n.Step(tn.now, node.Receive{Addr: "n01", Packet: msg.Packet{From: tn.nodes["n01"].n.Self(), Body: msg.Ack{}, Gossip: []msg.Update{{Member: past}}}})
	if !slices.ContainsFunc(r.Events, func(e node.Event) bool { return e.Kind == node.Refused && e.Member == "n02" }) {
		t.Fatalf("an entry past the bound wasn't refused: %v", r.Events)
	}
}

// Dead entries in a full view make room for a live member.
func TestAFullViewMakesRoomFromTheDead(t *testing.T) {
	tn := newNet(t, 2, func(_ int, c *node.Config) { c.MaxMembers = 5 })
	n := tn.nodes["n00"].n
	var forged []msg.Update
	for i := range 3 {
		forged = append(forged, msg.Update{Member: member.Member{ID: member.ID(fmt.Sprintf("f%02d", i)), Addr: "x"}})
	}
	n.Step(tn.now, node.Receive{Addr: "n01", Packet: msg.Packet{From: tn.nodes["n01"].n.Self(), Body: msg.Ack{}, Gossip: forged}})
	for i := range forged {
		forged[i].Member.State = member.Dead
	}
	n.Step(tn.now, node.Receive{Addr: "n01", Packet: msg.Packet{From: tn.nodes["n01"].n.Self(), Body: msg.Ack{}, Gossip: forged}})
	third := msg.Update{Member: member.Member{ID: "real", Addr: "r", MinVersion: 1, Version: 1}}
	n.Step(tn.now, node.Receive{Addr: "n01", Packet: msg.Packet{From: tn.nodes["n01"].n.Self(), Body: msg.Ack{}, Gossip: []msg.Update{third}}})
	if _, ok := n.Member("real"); !ok {
		t.Fatal("dead entries kept a live member out of a full view")
	}
	if len(n.Members()) > 5 {
		t.Fatalf("%d members, over the cap", len(n.Members()))
	}
}

// A member forgotten to make room is reported and cleaned up as one
// forgotten at DeadTimeout is.
func TestMakingRoomReportsTheForgotten(t *testing.T) {
	n := soloWith(func(c *node.Config) { c.MaxMembers = 4 }, "b", "c", "d")
	dead := peer("d")
	dead.State = member.Dead
	hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: dead})
	r := hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: peer("e")})
	if _, ok := n.Member("e"); !ok {
		t.Fatal("e wasn't taken")
	}
	if len(r.Forgotten) != 1 || r.Forgotten[0].ID != "d" || r.Forgotten[0].State != member.Dead {
		t.Fatalf("Forgotten = %v, want d's entry", r.Forgotten)
	}
}

// An entry a full view wouldn't take, as one no newer than its member's
// grave, makes no room: it forgets no gone member for nothing.
func TestAStaleEntryMakesNoRoom(t *testing.T) {
	n := soloWith(func(c *node.Config) { c.MaxMembers = 5 }, "b", "c", "d", "g")
	g, d := peer("g"), peer("d")
	g.Incarnation, g.State = 2, member.Dead
	d.State = member.Dead
	hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: g})
	hear(n, t0.Add(time.Second), peer("b"), msg.Ack{}, msg.Update{Member: d})
	// e takes g's room, g gone longest; g leaves a grave at incarnation 2.
	if r := hear(n, t0.Add(2*time.Second), peer("b"), msg.Ack{}, msg.Update{Member: peer("e")}); len(r.Forgotten) != 1 || r.Forgotten[0].ID != "g" {
		t.Fatalf("Forgotten = %v, want g", r.Forgotten)
	}
	stale := peer("g")
	stale.Incarnation = 1
	r := hear(n, t0.Add(3*time.Second), peer("b"), msg.Ack{}, msg.Update{Member: stale})
	if len(r.Forgotten) != 0 {
		t.Fatalf("a stale entry forgot %v", r.Forgotten)
	}
	if _, ok := n.Member("d"); !ok {
		t.Fatal("a stale entry made room by forgetting d")
	}
	if _, ok := n.Member("g"); ok {
		t.Fatal("a stale entry of g was taken")
	}
}

// A packet carries at most msg.MaxUpdates, however large MaxPacket: every
// packet a node builds encodes.
func TestEveryPacketEncodes(t *testing.T) {
	n := solo(func(c *node.Config) { c.MaxMembers, c.MaxPacket, c.SuspicionMult = 0, msg.MaxPacketBytes, 1000 })
	at := t0
	for i := range 3 {
		var us []msg.Update
		for j := range 1000 {
			us = append(us, msg.Update{Member: member.Member{ID: member.ID(fmt.Sprintf("m%d-%d", i, j)), Addr: "x"}})
		}
		hear(n, at, peer(fmt.Sprint("s", i)), msg.Ack{}, us...)
	}
	most := 0
	for range 200 {
		r := n.Step(at, node.Tick{})
		for _, p := range r.Packets {
			if _, err := wire.EncodePacket(p.Packet); err != nil {
				t.Fatal(err)
			}
			most = max(most, len(p.Packet.Gossip))
		}
		at = r.Wake
	}
	if most != msg.MaxUpdates {
		t.Fatalf("at most %d updates in a packet, want %d", most, msg.MaxUpdates)
	}
}

// No packet is larger than MaxPacket, at MinPacket: a buddy entry that
// doesn't fit stays out, and the longest ping-req still fits.
func TestNoPacketPastMaxPacket(t *testing.T) {
	long := func(s string) string { return s + strings.Repeat("x", 255-len(s)) }
	n := solo(func(c *node.Config) {
		c.ID, c.Addr, c.Incarnation = member.ID(long("n")), long("n"), member.MaxIncarnation-10
		c.MaxPacket = c.MinPacket(wire.Sizer{})
	})
	var peers []msg.Update
	for i := range 6 {
		m := peer(long(fmt.Sprint("p", i)))
		m.Addr = string(m.ID)
		if i%2 == 0 {
			m.State = member.Suspect
		}
		peers = append(peers, msg.Update{Member: m, By: member.ID(long("by"))})
	}
	r := hear(n, t0, peers[1].Member, msg.Ack{}, peers...)
	at, max := t0, n.Config().MaxPacket
	for range 100 {
		for _, p := range r.Packets {
			b, err := wire.EncodePacket(p.Packet)
			if err != nil {
				t.Fatal(err)
			}
			if len(b) > max {
				t.Fatalf("a %d-byte %T, MaxPacket %d", len(b), p.Packet.Body, max)
			}
		}
		r = n.Step(at, node.Tick{})
		at = r.Wake
	}
}

// A member forgotten to make room takes its queued updates with it: no
// packet to another member carries them after. A healing probe of d
// carries d's own grave entry; that isn't gossip.
func TestMakingRoomDropsTheForgottensGossip(t *testing.T) {
	n := soloWith(func(c *node.Config) { c.MaxMembers = 4 }, "b", "c", "d")
	dead := peer("d")
	dead.State = member.Dead
	hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: dead})
	hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: peer("e")})
	at := t0
	for range 50 {
		r := n.Step(at, node.Tick{})
		for _, p := range r.Packets {
			for _, u := range p.Packet.Gossip {
				if u.Member.ID == "d" && p.To != "d" {
					t.Fatalf("%T to %s carried forgotten d", p.Packet.Body, p.To)
				}
			}
		}
		at = r.Wake
	}
}

// A newer tombstone of a member the node forgot is passed on, as news,
// whether it came by gossip or in a sync.
func TestANewerTombstoneOfAForgottenMemberIsPassedOn(t *testing.T) {
	for _, bySync := range []bool{false, true} {
		n := soloWith(func(c *node.Config) { c.DeadTimeout = time.Second }, "b", "c", "g")
		dead := peer("g")
		dead.State = member.Dead
		hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: dead})
		at := t0
		for at.Before(t0.Add(2 * time.Second)) {
			at = n.Step(at, node.Tick{}).Wake
		}
		if _, ok := n.Member("g"); ok {
			t.Fatal("g wasn't forgotten")
		}
		left := peer("g")
		left.Incarnation, left.State = 3, member.Left
		var r node.Result
		if bySync {
			r = n.Step(at, node.ReceiveSync{Addr: "b", Sync: msg.Sync{From: peer("b"), View: []member.Member{peer("b"), left}}})
		} else {
			r = hear(n, at, peer("b"), msg.Ack{}, msg.Update{Member: left})
		}
		passed := false
		for range 50 {
			for _, p := range r.Packets {
				for _, u := range p.Packet.Gossip {
					passed = passed || u.Member == left
				}
			}
			r = n.Step(r.Wake, node.Tick{})
		}
		if !passed {
			t.Errorf("by sync %v: the newer tombstone wasn't passed on", bySync)
		}
	}
}

// A tombstone queued for a member whose old grave went over the cap, and
// who was buried again in the same step, stays queued: it belongs to the
// new grave.
func TestATombstoneOfANewGraveStaysQueued(t *testing.T) {
	n := soloWith(func(c *node.Config) { c.MaxMembers, c.GraveTimeout = 3, 24*time.Hour }, "b")
	up := func(id string, inc uint64, s member.State) msg.Update {
		m := peer(id)
		m.Incarnation, m.State = inc, s
		return msg.Update{Member: m}
	}
	// g is buried first, then 10,000 more graves push its grave out; g
	// comes back, dies and is buried again, and a newer tombstone of it
	// comes, all in one packet.
	us := []msg.Update{up("g", 0, member.Alive), up("g", 0, member.Dead)}
	for i := range 10001 {
		id := fmt.Sprintf("m%05d", i)
		us = append(us, up(id, 0, member.Alive), up(id, 0, member.Dead))
	}
	us = append(us, up("g", 1, member.Alive), up("g", 1, member.Dead), up("y", 0, member.Alive), up("g", 2, member.Dead))
	r := hear(n, t0, peer("b"), msg.Ack{}, us...)
	for range 50 {
		for _, p := range r.Packets {
			for _, u := range p.Packet.Gossip {
				if u.Member.ID == "g" && u.Member.Incarnation == 2 {
					return
				}
			}
		}
		r = n.Step(r.Wake, node.Tick{})
	}
	t.Fatal("the new grave's tombstone was dropped")
}

// A newer tombstone queued for a forgotten member goes when its grave
// does: the queue keeps nothing of a member the node no longer remembers.
func TestATombstoneGoesWithItsGrave(t *testing.T) {
	n := solo(func(c *node.Config) { c.DeadTimeout, c.GraveTimeout = time.Second, 10*time.Second })
	// b reads no version n writes, so nothing queued is sent to it.
	b := peer("b")
	b.MinVersion, b.Version = 5, 5
	hear(n, t0, b, msg.Ack{}, msg.Update{Member: peer("g")})
	dead := peer("g")
	dead.State = member.Dead
	hear(n, t0, b, msg.Ack{}, msg.Update{Member: dead})
	at := t0
	for at.Before(t0.Add(2 * time.Second)) {
		at = n.Step(at, node.Tick{}).Wake
	}
	left := peer("g")
	left.Incarnation, left.State = 3, member.Left
	hear(n, at, b, msg.Ack{}, msg.Update{Member: left})
	for at.Before(t0.Add(15 * time.Second)) {
		at = n.Step(at, node.Tick{}).Wake
	}
	// b restarts in a version n writes: nothing about g reaches it.
	b = peer("b")
	b.Incarnation = 1
	r := hear(n, at, b, msg.Ack{})
	for range 50 {
		for _, p := range r.Packets {
			for _, u := range p.Packet.Gossip {
				if u.Member.ID == "g" {
					t.Fatalf("%T to %s carried g after its grave went", p.Packet.Body, p.To)
				}
			}
		}
		r = n.Step(r.Wake, node.Tick{})
	}
}

// A stranger the node answers but doesn't take in, as a full view
// refuses it, leaves nothing behind when it reads no version the node
// writes.
func TestARefusedIncompatibleStrangerLeavesNothing(t *testing.T) {
	n := soloWith(func(c *node.Config) { c.MaxMembers = 3 }, "b", "c")
	for i := range 20 {
		s := peer(fmt.Sprint("s", i))
		s.MinVersion, s.Version = 5, 5
		hear(n, t0, s, msg.Ping{Target: "n"})
		if err := n.Check(); err != nil {
			t.Fatal(err)
		}
	}
}

// A forgotten member answering a healing probe leaves its round-trip time
// only while its grave lasts.
func TestAHealedGravesRTTGoesWithIt(t *testing.T) {
	n := soloWith(func(c *node.Config) {
		c.DeadTimeout, c.GraveTimeout, c.Probe.HealEvery = time.Second, 30*time.Second, 1
	}, "b", "g")
	dead := peer("g")
	dead.State = member.Dead
	hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: dead})
	r := n.Step(t0, node.Tick{})
	// g answers healing probes from its old run, an entry no newer than its
	// grave, until one is measured while g is buried.
	for n.RTT("g") == 0 || isMember(n, "g") {
		if r.Wake.After(t0.Add(20 * time.Second)) {
			t.Fatal("no healing probe of buried g was answered")
		}
		at := r.Wake
		r = n.Step(at, node.Tick{})
		for _, p := range r.Packets {
			if ping, ok := p.Packet.Body.(msg.Ping); ok && p.To == "g" && !isMember(n, "g") {
				hear(n, at.Add(time.Millisecond), peer("g"), msg.Ack{Seq: ping.Seq})
			}
		}
	}
	// Then g is silent, and its grave expires.
	for r.Wake.Before(t0.Add(time.Minute)) {
		r = n.Step(r.Wake, node.Tick{})
		if err := n.Check(); err != nil {
			t.Fatal(err)
		}
	}
	if n.RTT("g") != 0 {
		t.Fatalf("g's round-trip time %v outlived its grave", n.RTT("g"))
	}
}

// A stranger the view refuses gets no correction.
func TestARefusedStrangerGetsNoCorrection(t *testing.T) {
	n := soloWith(func(c *node.Config) { c.MaxMembers = 3 }, "b", "c")
	b2 := peer("b")
	b2.Incarnation = 2
	hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: b2})
	// s, refused by the full view, gossips an older entry of b.
	r := hear(n, t0, peer("s"), msg.Gossip{}, msg.Update{Member: peer("b")})
	for _, p := range r.Packets {
		if p.To == "s" {
			t.Fatalf("a %T correction to refused s", p.Packet.Body)
		}
	}
	// A member that gossips the older entry is corrected.
	r = hear(n, t0, peer("c"), msg.Gossip{}, msg.Update{Member: peer("b")})
	if !slices.ContainsFunc(r.Packets, func(p node.Packet) bool { return p.To == "c" }) {
		t.Fatal("member c wasn't corrected")
	}
}

// An ack that arrives after the grave of the member probed went, from a
// member a full view refuses, leaves no round-trip time behind.
func TestALateAckFromAnUnkeptMemberLeavesNoRTT(t *testing.T) {
	n := soloWith(func(c *node.Config) {
		c.MaxMembers, c.DeadTimeout, c.GraveTimeout, c.Probe.HealEvery = 3, time.Second, 10*time.Second, 1
	}, "b", "g")
	dead := peer("g")
	dead.State = member.Dead
	hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: dead})
	r := n.Step(t0, node.Tick{})
	var buried time.Time
	for buried.IsZero() {
		at := r.Wake
		r = n.Step(at, node.Tick{})
		if slices.ContainsFunc(r.Forgotten, func(m member.Member) bool { return m.ID == "g" }) {
			buried = at
		}
	}
	hear(n, buried, peer("b"), msg.Ack{}, msg.Update{Member: peer("c")})
	expires := buried.Add(10 * time.Second)
	// The last healing probe of g before its grave expires.
	var seq uint32
	for r.Wake.Before(expires) {
		r = n.Step(r.Wake, node.Tick{})
		for _, p := range r.Packets {
			if ping, ok := p.Packet.Body.(msg.Ping); ok && p.To == "g" {
				seq = ping.Seq
			}
		}
	}
	n.Step(expires.Add(time.Millisecond), node.Tick{})
	g := peer("g")
	g.Incarnation = 1
	hear(n, expires.Add(2*time.Millisecond), g, msg.Ack{Seq: seq})
	if isMember(n, "g") {
		t.Fatal("full view took g")
	}
	if err := n.Check(); err != nil {
		t.Fatal(err)
	}
}

// A message whose sender entry names a member other than the peer the
// transport proved sent it is dropped whole, and reported: an admitted
// member can't refute for another or say it left. One from the peer
// itself, or with no proven peer, is taken.
func TestAnImpersonatedMessageIsDropped(t *testing.T) {
	n := soloWith(nil, "b", "c")
	sb := peer("b")
	sb.State = member.Suspect
	hear(n, t0, peer("c"), msg.Ack{}, msg.Update{Member: sb, By: "c"})
	// mallory, proven by the transport, speaks as b: a refutation, and
	// news of a new member, in a packet and in a sync.
	b5 := peer("b")
	b5.Incarnation = 5
	for _, in := range []node.Input{
		node.Receive{Addr: "b", Peer: "mallory", Packet: msg.Packet{From: b5, Body: msg.Ack{}, Gossip: []msg.Update{{Member: peer("x")}}}},
		node.ReceiveSync{Addr: "b", Peer: "mallory", Sync: msg.Sync{From: b5, View: []member.Member{b5, peer("x")}, Reply: true}},
	} {
		r := n.Step(t0, in)
		if !slices.ContainsFunc(r.Events, func(e node.Event) bool {
			return e.Kind == node.Impersonated && e.Member == "b" && e.By == "mallory"
		}) {
			t.Fatalf("%T: no Impersonated event: %v", in, r.Events)
		}
		if len(r.Packets)+len(r.Syncs) != 0 {
			t.Fatalf("%T: answered an impersonation", in)
		}
		if m, _ := n.Member("b"); m.State != member.Suspect || m.Incarnation != 0 {
			t.Fatalf("%T: b is %v at %d, want still suspect at 0", in, m.State, m.Incarnation)
		}
		if isMember(n, "x") {
			t.Fatalf("%T: took x from an impersonation", in)
		}
	}
	// From the peer itself, and with no peer proven, the refutation holds.
	for _, peerID := range []member.ID{"b", ""} {
		m := soloWith(nil, "b", "c")
		hear(m, t0, peer("c"), msg.Ack{}, msg.Update{Member: sb, By: "c"})
		m.Step(t0, node.Receive{Addr: "b", Peer: peerID, Packet: msg.Packet{From: b5, Body: msg.Ack{}}})
		if got, _ := m.Member("b"); got.State != member.Alive || got.Incarnation != 5 {
			t.Fatalf("peer %q: b is %v at %d, want alive at 5", peerID, got.State, got.Incarnation)
		}
	}
}

// A running node has the last word on its own entry: a claim that it is
// dead or left, at its incarnation or above, makes it refute at a higher
// one, as a suspicion does.
func TestAClaimThatTheNodeDiedOrLeftIsRefuted(t *testing.T) {
	for _, st := range []member.State{member.Dead, member.Left} {
		n := soloWith(nil, "b")
		self := n.Self()
		claim := self
		claim.State, claim.Incarnation = st, self.Incarnation+3
		r := hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: claim})
		if !hasEvent(r, node.Refuted) {
			t.Fatalf("%v: not refuted: %v", st, r.Events)
		}
		if got := n.Self(); got.State != member.Alive || got.Incarnation != claim.Incarnation+1 {
			t.Fatalf("%v: self %v at %d, want alive at %d", st, got.State, got.Incarnation, claim.Incarnation+1)
		}
	}
}

// A sync names the member it is meant for: the seed it joins through, the
// sender of a sync it answers, and the member drawn for a periodic sync.
func TestASyncNamesTheMemberItIsMeantFor(t *testing.T) {
	n := solo(nil)
	r := n.Step(t0, node.Join{Seeds: []node.Seed{{ID: "seed", Addr: "seed-addr"}}})
	if len(r.Syncs) != 1 || r.Syncs[0].To != "seed" || r.Syncs[0].Addr != "seed-addr" {
		t.Fatalf("join syncs %+v, want one to seed at seed-addr", r.Syncs)
	}
	b := peer("b")
	b.Addr = "b-addr"
	r = n.Step(t0, node.ReceiveSync{Addr: "b-addr", Sync: msg.Sync{From: b, View: []member.Member{b}, Reply: true}})
	if len(r.Syncs) != 1 || r.Syncs[0].To != "b" || r.Syncs[0].Addr != "b-addr" {
		t.Fatalf("answer %+v, want one to b at b-addr", r.Syncs)
	}
	// Periodic syncs go to b, the only member.
	at := t0
	for range 200 {
		r = n.Step(at, node.Tick{})
		for _, s := range r.Syncs {
			if s.Addr == "b-addr" {
				if s.To != "b" {
					t.Fatalf("periodic sync %+v, want it meant for b", s)
				}
				return
			}
		}
		at = r.Wake
	}
	t.Fatal("no periodic sync to b")
}

// A message in a version the node doesn't read is told apart by its proven
// peer, if the transport proves who sends: answered as that member, once a
// period whatever address it comes from, and reported with it.
func TestAnUnreadableMessageIsKnownByItsPeer(t *testing.T) {
	n := soloWith(nil, "b")
	r := n.Step(t0, node.Unreadable{Addr: "elsewhere", Peer: "b", Version: 9})
	if len(r.Packets) != 1 || r.Packets[0].To != "b" || r.Packets[0].Addr != "elsewhere" {
		t.Fatalf("answer %+v, want one to b at the address it came from", r.Packets)
	}
	if !slices.ContainsFunc(r.Events, func(e node.Event) bool { return e.Kind == node.WrongVersion && e.Member == "b" }) {
		t.Fatalf("events %v, want WrongVersion from b", r.Events)
	}
	if r := n.Step(t0, node.Unreadable{Addr: "yet-elsewhere", Peer: "b", Version: 9}); len(r.Packets) != 0 {
		t.Fatal("answered b twice in a period from another address")
	}
	// A stranger, proven but not a member, at an address that is no seed,
	// gets no answer.
	if r := n.Step(t0, node.Unreadable{Addr: "b", Peer: "stranger", Version: 9}); len(r.Packets) != 0 {
		t.Fatalf("answered a stranger at b's address: %+v", r.Packets)
	}
}

func isMember(n *node.Node, id member.ID) bool {
	_, ok := n.Member(id)
	return ok
}

// solo is a node named n, at t0, built from Defaults changed by edit.
func solo(edit func(c *node.Config)) *node.Node {
	c := node.Defaults("n", "n")
	c.Seed = 1
	if edit != nil {
		edit(&c)
	}
	return node.New(c, wire.Sizer{}, t0)
}

// seed is the seed of the member id, at the address id.
func seed(id string) node.Seed { return node.Seed{ID: member.ID(id), Addr: id} }

// peer is a member that reads version 1 and has every capability.
func peer(id string) member.Member {
	return member.Member{ID: member.ID(id), Addr: id, MinVersion: 1, Version: 1, Caps: member.Of(member.CapNack, member.CapGossip)}
}

// hear steps n with a packet from sender at at.
func hear(n *node.Node, at time.Time, sender member.Member, body msg.Body, gossip ...msg.Update) node.Result {
	return n.Step(at, node.Receive{Addr: sender.Addr, Packet: msg.Packet{From: sender, Body: body, Gossip: gossip}})
}

func hasEvent(r node.Result, kind node.EventKind) bool {
	return slices.ContainsFunc(r.Events, func(e node.Event) bool { return e.Kind == kind })
}

// A configuration at every limit is valid; one past any is not.
func TestValidateLimits(t *testing.T) {
	good := node.Defaults("a", "a")
	for name, edit := range map[string]func(*node.Config){
		"an ID of 255 bytes":      func(c *node.Config) { c.ID = member.ID(strings.Repeat("x", 255)) },
		"an address of 255 bytes": func(c *node.Config) { c.Addr = strings.Repeat("x", 255) },
		"one version":             func(c *node.Config) { c.MinVersion, c.Version = 2, 2 },
		"the highest incarnation": func(c *node.Config) { c.Incarnation = member.MaxIncarnation },
		"intervals and counts of 0": func(c *node.Config) {
			c.SyncEvery, c.SeedSyncEvery, c.RetryJoinEvery, c.GossipInterval, c.GossipNodes = 0, 0, 0, 0, 0
		},
		"the smallest probe timings": func(c *node.Config) { c.Probe.Interval, c.Probe.Timeout = 1, 1 },
		"the smallest MaxPacket":     func(c *node.Config) { c.MaxPacket = c.MinPacket(wire.Sizer{}) },
		"every version":              func(c *node.Config) { c.MinVersion, c.Version = 1, 255; c.MaxPacket = c.MinPacket(wire.Sizer{}) },
		"the Lifeguard health rule":  func(c *node.Config) { c.HealthRule = health.RuleLifeguard },
		"the smallest MaxPacket, the longest ID": func(c *node.Config) {
			c.ID = member.ID(strings.Repeat("x", 255))
			c.MaxPacket = c.MinPacket(wire.Sizer{})
		},
	} {
		c := good
		edit(&c)
		if err := c.Validate(wire.Sizer{}); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, edit := range map[string]func(*node.Config){
		"no timeout":                     func(c *node.Config) { c.Probe.Timeout = 0 },
		"no MaxPacket":                   func(c *node.Config) { c.MaxPacket = 0 },
		"a MaxPacket past a datagram":    func(c *node.Config) { c.MaxPacket = msg.MaxPacketBytes + 1 },
		"a MaxPacket under MinPacket":    func(c *node.Config) { c.MaxPacket = c.MinPacket(wire.Sizer{}) - 1 },
		"a negative seed sync count":     func(c *node.Config) { c.SeedSyncEvery = -1 },
		"no suspicion timeout":           func(c *node.Config) { c.SuspicionMult = 0 },
		"an undefined health rule":       func(c *node.Config) { c.HealthRule = health.RuleLifeguard + 1 },
		"a negative helper count":        func(c *node.Config) { c.Probe.Helpers = -1 },
		"a negative RTT multiple":        func(c *node.Config) { c.Probe.RTTMult = -1 },
		"a negative healing interval":    func(c *node.Config) { c.Probe.HealEvery = -1 },
		"a negative HealthMax":           func(c *node.Config) { c.HealthMax = -1 },
		"a negative SuspicionMaxMult":    func(c *node.Config) { c.SuspicionMaxMult = -1 },
		"negative confirmations":         func(c *node.Config) { c.Confirmations = -1 },
		"a negative RetransmitMult":      func(c *node.Config) { c.RetransmitMult = -1 },
		"a negative sync interval":       func(c *node.Config) { c.SyncEvery = -1 },
		"a negative DeadTimeout":         func(c *node.Config) { c.DeadTimeout = -1 },
		"a negative GraveTimeout":        func(c *node.Config) { c.GraveTimeout = -1 },
		"a negative MaxMembers":          func(c *node.Config) { c.MaxMembers = -1 },
		"a negative LateLimit":           func(c *node.Config) { c.LateLimit = -1 },
		"a negative join retry":          func(c *node.Config) { c.RetryJoinEvery = -1 },
		"a negative gossip interval":     func(c *node.Config) { c.GossipInterval = -1 },
		"a negative gossip fanout":       func(c *node.Config) { c.GossipNodes = -1 },
		"an incarnation over the most":   func(c *node.Config) { c.Incarnation = member.MaxIncarnation + 1 },
		"a version below the lowest one": func(c *node.Config) { c.MinVersion, c.Version = 3, 2 },
	} {
		c := good
		edit(&c)
		if err := c.Validate(wire.Sizer{}); err == nil {
			t.Errorf("%s: valid", name)
		}
	}
}

// MinPacket ends for any version range, a malformed one included.
func TestMinPacketEndsForAnyVersions(t *testing.T) {
	c := node.Defaults("a", "a")
	done := make(chan struct{})
	go func() {
		for _, r := range [][2]uint8{{0, 255}, {255, 255}, {3, 2}, {0, 0}} {
			c.MinVersion, c.Version = r[0], r[1]
			c.MinPacket(wire.Sizer{})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("MinPacket didn't end")
	}
}

// Defaults gossips on a timer, more often than it probes, and keeps the
// graves of forgotten members longer than the dead.
func TestDefaultsGossipAndKeepGraves(t *testing.T) {
	c := node.Defaults("a", "a")
	if c.GossipInterval <= 0 || c.GossipInterval >= c.Probe.Interval {
		t.Errorf("GossipInterval %v, probe interval %v", c.GossipInterval, c.Probe.Interval)
	}
	if c.GraveTimeout <= c.DeadTimeout {
		t.Errorf("GraveTimeout %v, DeadTimeout %v", c.GraveTimeout, c.DeadTimeout)
	}
}

// A step that changes the health score says so with its new value; a step
// that doesn't, doesn't.
func TestHealthChangedIsAnEvent(t *testing.T) {
	n := solo(nil)
	if r := n.Step(t0, node.Tick{}); hasEvent(r, node.HealthChanged) {
		t.Fatalf("a step that changed no health: %v", r.Events)
	}
	accused := n.Self()
	accused.State = member.Suspect
	r := hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: accused, By: "b"})
	if !slices.ContainsFunc(r.Events, func(e node.Event) bool { return e.Kind == node.HealthChanged && e.Value == n.Health() && e.Value > 0 }) {
		t.Fatalf("refuting raised the health to %d without an event: %v", n.Health(), r.Events)
	}
}

// A join whose seeds don't answer is tried again RetryJoinEvery periods
// later, give or take half.
func TestJoinRetriesAfterRetryJoinEvery(t *testing.T) {
	n := solo(func(c *node.Config) { c.SyncEvery = 0 })
	n.Step(t0, node.Join{Seeds: []node.Seed{seed("seed")}})
	now := t0
	for range 100 {
		r := n.Step(now, node.Tick{})
		if slices.ContainsFunc(r.Syncs, func(s node.Sync) bool { return s.Addr == "seed" }) && now.After(t0) {
			if d := now.Sub(t0); d < time.Second || d > 3*time.Second {
				t.Fatalf("retried %v after joining, want 2s give or take half", d)
			}
			return
		}
		now = r.Wake
	}
	t.Fatal("never retried")
}

// With MaxMembers 0 a view has no cap: third parties' entries are taken.
func TestNoMaxMembers(t *testing.T) {
	n := solo(func(c *node.Config) { c.MaxMembers = 0 })
	var others []msg.Update
	for i := range 5 {
		others = append(others, msg.Update{Member: peer(fmt.Sprint("o", i))})
	}
	hear(n, t0, peer("b"), msg.Ack{}, others...)
	if got := len(n.Members()); got != 7 {
		t.Fatalf("%d members, want 7", got)
	}
}

// soloWith is a solo node that knows the given peers, alive.
func soloWith(edit func(c *node.Config), ids ...string) *node.Node {
	n := solo(edit)
	var us []msg.Update
	for _, id := range ids[1:] {
		us = append(us, msg.Update{Member: peer(id)})
	}
	hear(n, t0, peer(ids[0]), msg.Ack{}, us...)
	return n
}

// The member that raised a suspicion doesn't confirm it by saying it
// again; another member does.
func TestTheRaiserIsNoConfirmer(t *testing.T) {
	n := soloWith(nil, "b", "c", "d", "x")
	sx := peer("x")
	sx.State = member.Suspect
	if r := hear(n, t0, peer("c"), msg.Ack{}, msg.Update{Member: sx, By: "c"}); !hasEvent(r, node.Suspected) {
		t.Fatalf("no suspicion: %v", r.Events)
	}
	if r := hear(n, t0, peer("c"), msg.Ack{}, msg.Update{Member: sx, By: "c"}); hasEvent(r, node.Confirmed) {
		t.Fatalf("the raiser confirmed its own suspicion: %v", r.Events)
	}
	if r := hear(n, t0, peer("d"), msg.Ack{}, msg.Update{Member: sx, By: "d"}); !hasEvent(r, node.Confirmed) {
		t.Fatalf("another member's suspicion didn't confirm: %v", r.Events)
	}
}

// ackGossip is the gossip on n's ack to a ping from sender.
func ackGossip(t *testing.T, n *node.Node, at time.Time, sender member.Member, gossip ...msg.Update) []msg.Update {
	t.Helper()
	r := hear(n, at, sender, msg.Ping{Seq: 1, Target: n.Self().ID}, gossip...)
	for _, p := range r.Packets {
		if _, ok := p.Packet.Body.(msg.Ack); ok && p.To == sender.ID {
			return p.Packet.Gossip
		}
	}
	t.Fatalf("no ack to %s in %v", sender.ID, r.Packets)
	return nil
}

// With Dogpile, the first Confirmations confirmations are gossiped on.
func TestConfirmationsAreGossipedOn(t *testing.T) {
	n := soloWith(func(c *node.Config) { c.Confirmations = 1 }, "b", "c", "d", "x")
	sx := peer("x")
	sx.State = member.Suspect
	hear(n, t0, peer("c"), msg.Ack{}, msg.Update{Member: sx, By: "c"})
	hear(n, t0, peer("d"), msg.Ack{}, msg.Update{Member: sx, By: "d"})
	for _, u := range ackGossip(t, n, t0, peer("b")) {
		if u.Member == sx && u.By == "d" {
			return
		}
	}
	t.Fatal("the confirmation wasn't gossiped on")
}

// A leaving node syncs with Helpers members and sends its goodbye in
// gossip packets to λ log n + GossipNodes more.
func TestALeaveReachesItsFanout(t *testing.T) {
	ids := []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8"}
	n := soloWith(nil, ids...)
	r := n.Step(t0, node.Leave{})
	gossips := 0
	for _, p := range r.Packets {
		if _, ok := p.Packet.Body.(msg.Gossip); ok {
			gossips++
		}
	}
	// 3 syncs, then Limit(1, 10) = 2 and GossipNodes 3 more: 8 of the 9.
	if len(r.Syncs) != 3 || gossips != 8 {
		t.Fatalf("%d syncs and %d gossip packets, want 3 and 8", len(r.Syncs), gossips)
	}
}

// A gossip round sends the queue to other members, never to the node
// itself.
func TestAGossipRoundGoesToOthers(t *testing.T) {
	n := soloWith(nil, "b", "c")
	r := n.Step(t0.Add(200*time.Millisecond), node.Tick{})
	sent := 0
	for _, p := range r.Packets {
		if _, ok := p.Packet.Body.(msg.Gossip); ok {
			if p.To == "n" {
				t.Fatal("a gossip packet to the node itself")
			}
			sent++
		}
	}
	if sent == 0 {
		t.Fatalf("no gossip packet in the round: %v", r.Packets)
	}
}

// A reply goes in the highest version the sender says it reads, even when
// the view's entry of it, newer, says otherwise: the sender restarted on a
// newer release behind its earlier run.
func TestAReplyFollowsTheSendersOwnVersions(t *testing.T) {
	n := solo(func(c *node.Config) { c.Version = 2 })
	old := peer("b")
	old.Incarnation = 5
	hear(n, t0, old, msg.Ack{})
	restarted := peer("b")
	restarted.Incarnation, restarted.MinVersion, restarted.Version = 3, 2, 2
	r := hear(n, t0, restarted, msg.Ping{Seq: 1, Target: "n"})
	for _, p := range r.Packets {
		if _, ok := p.Packet.Body.(msg.Ack); ok {
			if p.Version != 2 {
				t.Fatalf("ack in version %d, want 2", p.Version)
			}
			return
		}
	}
	t.Fatalf("no ack in %v", r.Packets)
}

// With Buddy, a probe of a suspect member carries its suspicion first,
// without its raiser.
func TestBuddyTellsTheSuspectFirst(t *testing.T) {
	n := soloWith(func(c *node.Config) { c.LateLimit = 0 }, "b", "c")
	sb := peer("b")
	sb.State = member.Suspect
	r := hear(n, t0, peer("c"), msg.Ack{}, msg.Update{Member: sb, By: "c"})
	for range 10 {
		r = n.Step(r.Wake, node.Tick{})
		for _, p := range r.Packets {
			if _, ok := p.Packet.Body.(msg.Ping); ok && p.To == "b" {
				if g := p.Packet.Gossip; len(g) == 0 || g[0] != (msg.Update{Member: sb}) {
					t.Fatalf("the probe of a suspect starts %v", g)
				}
				return
			}
		}
	}
	t.Fatal("b wasn't probed")
}

// The entries a node corrects a sender with go on its ack as far as they
// fit MaxPacket, exactly.
func TestCorrectionsFitMaxPacket(t *testing.T) {
	stale := peer("c")
	newer := stale
	newer.Incarnation = 2
	// The node learns c from a sync: c isn't in its gossip queue.
	base := msg.Packet{From: peer("n"), Body: msg.Ack{Seq: 1}}
	var sz wire.Sizer
	fit := sz.PacketSize(base, 1) + sz.UpdateSize(msg.Update{Member: newer}, 1)
	for _, tc := range []struct {
		max  int
		want bool
	}{{fit, true}, {fit - 1, false}} {
		n := solo(func(c *node.Config) { c.MaxPacket = tc.max })
		n.Step(t0, node.ReceiveSync{Addr: "b", Sync: msg.Sync{From: peer("b"), View: []member.Member{newer}}})
		got := slices.ContainsFunc(ackGossip(t, n, t0, peer("b"), msg.Update{Member: stale}), func(u msg.Update) bool { return u.Member == newer })
		if got != tc.want {
			t.Errorf("MaxPacket %d, the correction needing %d: sent %v", tc.max, fit, got)
		}
	}
}

// However much gossip is queued, a packet stays within MaxPacket.
func TestQueuedGossipStaysWithinMaxPacket(t *testing.T) {
	var fresh []msg.Update
	for i := range 100 {
		fresh = append(fresh, msg.Update{Member: peer(fmt.Sprintf("member-%03d", i))})
	}
	n := soloWith(nil, "b")
	hear(n, t0, peer("b"), msg.Ack{}, fresh...)
	r := hear(n, t0, peer("b"), msg.Ping{Seq: 1, Target: "n"})
	for _, p := range r.Packets {
		b, err := wire.EncodePacket(p.Packet)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > n.Config().MaxPacket || len(p.Packet.Gossip) == 0 {
			t.Fatalf("a %d-byte packet with %d updates, MaxPacket %d", len(b), len(p.Packet.Gossip), n.Config().MaxPacket)
		}
	}
}

// A node with updates queued and someone to send them to wakes for its
// gossip timer; with none queued, it doesn't.
func TestTheGossipTimerWakesOnlyForUpdates(t *testing.T) {
	n := solo(nil)
	if r := hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: peer("c")}); r.Wake.After(t0.Add(200 * time.Millisecond)) {
		t.Fatalf("woke at %v with updates queued, gossip due at 200ms", r.Wake.Sub(t0))
	}
	tn := newNet(t, 2, nil)
	tn.joinAll()
	tn.after(time.Minute)
	before := tn.steps["n00"]
	tn.after(time.Minute)
	// Some three a second for probes and acks both ways.
	if steps := tn.steps["n00"] - before; steps > 250 {
		t.Fatalf("%d steps in a quiet minute", steps)
	}
}

// A member that had a wrong-version answer within a period gets no other,
// even after the node dropped the answers older than a period.
func TestAnsweringUnreadablesKeepsRecentOnes(t *testing.T) {
	n := soloWith(nil, "a", "b", "c")
	answered := func(at time.Duration, addr string) bool {
		r := n.Step(t0.Add(at), node.Unreadable{Addr: addr, Version: 9})
		return slices.ContainsFunc(r.Packets, func(p node.Packet) bool { return p.Addr == addr })
	}
	if !answered(0, "a") || !answered(900*time.Millisecond, "b") || !answered(time.Second, "c") {
		t.Fatal("a first wrong-version message wasn't answered")
	}
	if answered(1500*time.Millisecond, "b") {
		t.Fatal("b answered twice in a period")
	}
}

// A suspicion nobody confirms lasts SuspicionMaxMult times its shortest
// timeout, SuspicionMult × log10(live members) periods.
func TestASuspicionsLongestTimeout(t *testing.T) {
	ids := []string{"b"}
	for i := range 98 {
		ids = append(ids, fmt.Sprintf("m%02d", i))
	}
	// 100 live members: the shortest timeout is 4 × 2 periods, and the
	// longest six times that.
	n := soloWith(func(c *node.Config) { c.LateLimit = 0 }, ids...)
	sx := peer("m00")
	sx.State = member.Suspect
	hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: sx, By: "b"})
	n.Step(t0.Add(48*time.Second-time.Millisecond), node.Tick{})
	if m, _ := n.Member("m00"); m.State != member.Suspect {
		t.Fatalf("m00 is %v before its timeout", m.State)
	}
	n.Step(t0.Add(48*time.Second), node.Tick{})
	if m, _ := n.Member("m00"); m.State != member.Dead {
		t.Fatalf("m00 is %v at its timeout", m.State)
	}
}

// A joined node catches up with a sync soon after its join, and with more
// only while syncs teach it of members it didn't know: of two nodes, the
// joiner learns all there is from its seed's answer, so it syncs twice,
// the join and one catch-up, and then not again before its periodic sync
// is due, 30 periods on.
func TestCatchUpOnlyWhileSyncsTeach(t *testing.T) {
	tn := newNet(t, 2, nil)
	tn.joinAll()
	tn.after(20 * time.Second)
	if got := tn.started["n01"]; got != 2 {
		t.Fatalf("the joiner started %d syncs in 20 periods, want 2: its join and one catch-up", got)
	}
}

// An entry taken from a sync, of a member the node didn't know, isn't
// gossiped on; the same entry coming again in a packet is news going
// round, and is.
func TestSyncEntriesComingRoundAgainAreGossiped(t *testing.T) {
	caps := member.Of(member.CapNack, member.CapGossip)
	b := member.Member{ID: "b", Addr: "b", Incarnation: 5, Caps: caps, MinVersion: 1, Version: 1}
	x := member.Member{ID: "x", Addr: "x", Incarnation: 7, Caps: caps, MinVersion: 1, Version: 1}
	peers := map[string]member.Member{"b": b, "x": x}
	c := node.Defaults("a", "a")
	c.Seed = 1
	n := node.New(c, wire.Sizer{}, t0)
	now := t0
	// step steps the node, answers its pings as b and x, and reports
	// whether it gossiped x's entry.
	var step func(in node.Input) bool
	step = func(in node.Input) bool {
		carried := false
		for _, p := range n.Step(now, in).Packets {
			for _, u := range p.Packet.Gossip {
				carried = carried || u.Member == x
			}
			if ping, ok := p.Packet.Body.(msg.Ping); ok {
				carried = step(node.Receive{Addr: p.Addr, Packet: msg.Packet{From: peers[p.Addr], Body: msg.Ack{Seq: ping.Seq}}}) || carried
			}
		}
		return carried
	}
	ticks := func() bool {
		carried := false
		for range 30 {
			now = now.Add(100 * time.Millisecond)
			carried = step(node.Tick{}) || carried
		}
		return carried
	}
	step(node.ReceiveSync{Addr: "b", Sync: msg.Sync{From: b, View: []member.Member{x}}})
	if ticks() {
		t.Fatal("an entry taken from a sync was gossiped on")
	}
	// An older entry of x, or the same one from another sync, isn't news.
	old := x
	old.Incarnation--
	step(node.Receive{Addr: "b", Packet: msg.Packet{From: b, Body: msg.Gossip{}, Gossip: []msg.Update{{Member: old}}}})
	step(node.ReceiveSync{Addr: "b", Sync: msg.Sync{From: b, View: []member.Member{x}}})
	if ticks() {
		t.Fatal("x gossiped after an older entry or another sync")
	}
	step(node.Receive{Addr: "b", Packet: msg.Packet{From: b, Body: msg.Gossip{}, Gossip: []msg.Update{{Member: x}}}})
	if !ticks() {
		t.Fatal("x came round again by gossip and wasn't gossiped on")
	}
}

// A node that joins a settled cluster learns it all from its seed's
// answer, and none of it comes round again as news: it gossips its own
// entry, not the others', though every packet from them carries theirs.
// It sends 8 entries about others here.
func TestALateJoinerDoesNotGossipTheCluster(t *testing.T) {
	tn := newNet(t, 12, nil)
	for _, a := range tn.addrs[1:11] {
		tn.step(a, node.Join{Seeds: []node.Seed{seed(tn.addrs[0])}})
	}
	tn.after(time.Minute)
	before := tn.others["n11"]
	tn.step("n11", node.Join{Seeds: []node.Seed{seed(tn.addrs[0])}})
	tn.after(30 * time.Second)
	if got := tn.others["n11"] - before; got > 12 {
		t.Fatalf("the late joiner sent %d gossip entries about others in 30 periods, want at most 12", got)
	}
}

// A seed with no ID or no address, or with the node's own ID or address,
// is skipped: only the named seed of another member is synced with, and
// retried.
func TestAJoinSkipsSeedsWithNoIDAndItself(t *testing.T) {
	n := solo(func(c *node.Config) { c.SyncEvery = 0 })
	r := n.Step(t0, node.Join{Seeds: []node.Seed{{Addr: "anon"}, {ID: "nowhere"}, {ID: "n", Addr: "elsewhere"}, {ID: "other", Addr: "n"}, {ID: "s", Addr: "s-addr"}}})
	if len(r.Syncs) != 1 || r.Syncs[0].To != "s" || r.Syncs[0].Addr != "s-addr" {
		t.Fatalf("join syncs %+v, want one to s at s-addr", r.Syncs)
	}
	now := t0
	for range 20 {
		r = n.Step(now, node.Tick{})
		for _, s := range r.Syncs {
			if s.To != "s" {
				t.Fatalf("retried %+v", s)
			}
		}
		now = r.Wake
	}
	if !n.Joining() {
		t.Fatal("not joining")
	}
	// With only seeds it skips, there is nothing to join.
	if r := n.Step(now, node.Join{Seeds: []node.Seed{{Addr: "anon"}}}); len(r.Syncs) != 0 || n.Joining() {
		t.Fatalf("joined through an unnamed seed: %+v", r.Syncs)
	}
}

// A node with another live member keeps seeds and contacts none, whatever
// it holds of them, and brings no sync forward; a kept seed gets its turn
// in the periodic seed syncs.
func TestANodeWithLiveMembersKeepsSeedsAndContactsNone(t *testing.T) {
	n := soloWith(func(c *node.Config) { c.SyncEvery, c.SeedSyncEvery = 2, 1 }, "b", "c")
	b := peer("b")
	b.State, b.Incarnation = member.Dead, b.Incarnation+1
	now := joined(n, seed("c"))
	hear(n, now, peer("c"), msg.Ack{}, msg.Update{Member: b, By: "c"})
	r := n.Step(now, node.Join{Seeds: []node.Seed{
		{ID: "b", Addr: "b"},
		{ID: "c", Addr: "c-moved"},
		{ID: "new", Addr: "new-addr"},
	}})
	if len(r.Syncs) != 0 || n.Joining() {
		t.Fatalf("join syncs %+v, joining %v", r.Syncs, n.Joining())
	}
	for range 2000 {
		r := n.Step(now, node.Tick{})
		if slices.ContainsFunc(r.Syncs, func(s node.Sync) bool { return s.Addr == "new-addr" }) {
			return
		}
		now = r.Wake
	}
	t.Fatal("no seed sync reached the seed kept")
}

// A node alone syncs with a few seeds, not all, and each attempt takes the
// next few.
func TestANodeAloneJoinsThroughAFewSeeds(t *testing.T) {
	n := solo(nil)
	var seeds []node.Seed
	for i := range 20 {
		seeds = append(seeds, seed(fmt.Sprint("s", i)))
	}
	r := n.Step(t0, node.Join{Seeds: seeds})
	if len(r.Syncs) != 3 || !n.Joining() {
		t.Fatalf("join syncs %+v, joining %v", r.Syncs, n.Joining())
	}
	tried := map[string]bool{}
	for _, s := range r.Syncs {
		tried[s.Addr] = true
	}
	now := t0
	for range 2000 {
		r := n.Step(now, node.Tick{})
		for _, s := range r.Syncs {
			tried[s.Addr] = true
		}
		now = r.Wake
	}
	if len(tried) != len(seeds) {
		t.Fatalf("attempts tried only %v", tried)
	}
}

// joined gives n its first seeds, and steps it until its join is done,
// returning when that was.
func joined(n *node.Node, seeds ...node.Seed) time.Time {
	r := n.Step(t0, node.Join{Seeds: seeds})
	for n.Joining() {
		r = n.Step(r.Wake, node.Tick{})
	}
	return r.Wake
}

// A node's first seeds are always reached, though it has heard a member.
func TestAFirstJoinReachesSeedsWithAMemberHeard(t *testing.T) {
	n := solo(nil)
	hear(n, t0, peer("b"), msg.Ack{})
	var seeds []node.Seed
	for i := range 20 {
		seeds = append(seeds, seed(fmt.Sprint("s", i)))
	}
	if r := n.Step(t0, node.Join{Seeds: seeds}); len(r.Syncs) != 3 {
		t.Fatalf("first join syncs %+v", r.Syncs)
	}
	if r := n.Step(t0, node.Join{Seeds: seeds}); len(r.Syncs) != 0 {
		t.Fatalf("second join with a member heard syncs %+v", r.Syncs)
	}
}

// A first join with no seeds doesn't count as the first.
func TestAJoinWithNoSeedsLeavesTheFirstToCome(t *testing.T) {
	n := solo(nil)
	n.Step(t0, node.Join{})
	hear(n, t0, peer("b"), msg.Ack{})
	if r := n.Step(t0, node.Join{Seeds: []node.Seed{seed("s")}}); len(r.Syncs) != 1 {
		t.Fatalf("join syncs %+v", r.Syncs)
	}
}

// A running node that loses its last other member tries its seeds at once
// and goes on trying.
func TestANodeLeftAloneReachesForItsSeeds(t *testing.T) {
	n := soloWith(nil, "b")
	now := joined(n, seed("b"), seed("s"))
	gone := peer("b")
	gone.State, gone.Incarnation = member.Left, gone.Incarnation+1
	r := hear(n, now, peer("b"), msg.Ack{}, msg.Update{Member: gone, By: "b"})
	if len(r.Syncs) == 0 || !n.Joining() {
		t.Fatalf("left alone: syncs %+v, joining %v", r.Syncs, n.Joining())
	}
}

// A node alone, in the middle of trying its seeds, reaches new seeds at
// once.
func TestANodeAloneGivenSeedsReachesThemAtOnce(t *testing.T) {
	n := solo(nil)
	n.Step(t0, node.Join{Seeds: []node.Seed{seed("a")}})
	r := n.Step(t0.Add(time.Millisecond), node.Join{Seeds: []node.Seed{seed("a"), seed("b"), seed("c"), seed("d")}})
	if len(r.Syncs) != 3 {
		t.Fatalf("join syncs %+v, want 3", r.Syncs)
	}
}

// A node that left reaches for no seeds, though it is alone.
func TestANodeThatLeftReachesForNoSeeds(t *testing.T) {
	n := soloWith(nil, "b")
	now := joined(n, seed("b"), seed("c"))
	for _, s := range n.Step(now, node.Leave{}).Syncs {
		if s.Sync.Reply {
			t.Fatalf("leaving, it reached for a seed: %+v", s)
		}
	}
	if r := n.Step(now, node.Forget{ID: "b"}); len(r.Syncs) != 0 {
		t.Fatalf("alone, syncs %+v", r.Syncs)
	}
	for i := range 20 {
		if r := n.Step(now.Add(time.Duration(i)*time.Minute), node.Tick{}); len(r.Syncs) != 0 {
			t.Fatalf("after %d minutes, syncs %+v", i, r.Syncs)
		}
	}
}

// A node that left joins again through a few seeds, though it still holds
// live members.
func TestANodeThatLeftJoinsAgainThroughAFewSeeds(t *testing.T) {
	n := soloWith(nil, "b", "c")
	now := joined(n, seed("b"))
	n.Step(now, node.Leave{})
	r := n.Step(now.Add(time.Second), node.Join{Seeds: []node.Seed{seed("b"), seed("c"), seed("d"), seed("e")}})
	if len(r.Syncs) < 3 || len(r.Syncs) > 4 {
		t.Fatalf("join syncs %+v, want 3, and a periodic one if it was due", r.Syncs)
	}
}

// allSeeds names every node of tn but except, as a recording does.
func allSeeds(tn *testnet, except string) []node.Seed {
	var s []node.Seed
	for _, a := range tn.addrs {
		if a != except {
			s = append(s, seed(a))
		}
	}
	return s
}

// A seed a join didn't contact stays a seed: after a partition long enough
// for each side to forget the other, only seed syncs bring them back
// together.
func TestAKeptSeedStillHealsAPartition(t *testing.T) {
	tn := newNet(t, 6, func(_ int, c *node.Config) {
		c.SyncEvery = 2
		c.SeedSyncEvery = 1
		c.DeadTimeout = 30 * time.Second
		c.GraveTimeout = 30 * time.Second
	})
	tn.joinAll()
	tn.after(20 * time.Second)
	for _, a := range tn.addrs {
		tn.step(a, node.Join{Seeds: allSeeds(tn, a)})
	}
	tn.after(20 * time.Second)
	for _, a := range tn.addrs {
		if r := tn.nodes[a].n.Step(tn.now, node.Join{Seeds: allSeeds(tn, a)}); len(r.Syncs) != 0 {
			t.Fatalf("%s holds every seed alive and syncs %+v", a, r.Syncs)
		}
	}
	tn.after(5 * time.Second)
	// The node all joined through stays cut off.
	first := tn.addrs[0]
	side := func(a string) bool { return a < "n03" }
	tn.cut = func(from, to string) bool { return from == first || to == first || side(from) != side(to) }
	tn.after(3 * time.Minute)
	rest := tn.addrs[1:]
	for _, a := range rest {
		want := 3
		if side(a) {
			want = 2
		}
		if got := len(tn.nodes[a].n.Members()); got != want {
			t.Fatalf("%s holds %d members, want only its own side of %d", a, got, want)
		}
	}
	tn.cut = func(from, to string) bool { return from == first || to == first }
	tn.after(time.Minute)
	for _, a := range rest {
		if got := len(tn.nodes[a].n.Members()); got != len(rest) {
			t.Fatalf("%s holds %d members a minute after the partition healed, want %d", a, got, len(rest))
		}
	}
}

// A node added to a cluster costs a few syncs whatever the cluster's size,
// whichever of it and the others records the generation first: the new
// node syncs with a few seeds, and the others hear of it by gossip.
func TestANodeAddCostsAFewSyncsAtAnySize(t *testing.T) {
	for _, c := range []struct {
		n        int
		newFirst bool
	}{{25, false}, {50, false}, {100, false}, {25, true}, {50, true}, {100, true}} {
		n := c.n
		tn := newNet(t, n+1, nil)
		added := tn.addrs[n]
		tn.addrs = tn.addrs[:n]
		tn.joinAll()
		for _, a := range tn.addrs {
			tn.step(a, node.Join{Seeds: allSeeds(tn, a)})
		}
		tn.after(time.Minute)
		count := func() int {
			c := 0
			for _, v := range tn.started {
				c += v
			}
			return c
		}
		before := count()
		tn.after(20 * time.Second)
		quiet := count() - before
		tn.addrs = append(tn.addrs, added)
		before = count()
		if c.newFirst {
			tn.step(added, node.Join{Seeds: allSeeds(tn, added)})
		}
		for _, a := range tn.addrs[:n] {
			tn.step(a, node.Join{Seeds: allSeeds(tn, a)})
		}
		if !c.newFirst {
			tn.step(added, node.Join{Seeds: allSeeds(tn, added)})
		}
		tn.after(20 * time.Second)
		if got := count() - before - quiet; got > 20 {
			t.Errorf("adding a node to %d started %d syncs, want at most 20", n, got)
		}
		if !tn.converged() {
			t.Fatalf("%d nodes and the one added didn't converge", n)
		}
	}
}

// Forget takes a member out of the view at once, whatever its state, and
// reports it. From then on the node neither probes it nor gossips it, and
// ignores every entry of it, at any incarnation, from the member itself or
// gossiped by another, past GraveTimeout, until a Join names it; then it
// takes it back.
func TestForgetRemovesAMemberAndKeepsItOutUntilAJoinNamesIt(t *testing.T) {
	for _, st := range []member.State{member.Alive, member.Suspect, member.Dead, member.Left} {
		n := soloWith(func(c *node.Config) { c.GraveTimeout = time.Hour }, "b", "c", "x")
		if st != member.Alive {
			x := peer("x")
			x.State = st
			hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: x, By: "b"})
		}
		r := n.Step(t0, node.Forget{ID: "x"})
		if len(r.Forgotten) != 1 || r.Forgotten[0].ID != "x" || r.Forgotten[0].State != st {
			t.Fatalf("%v: forgotten %v", st, r.Forgotten)
		}
		if _, ok := n.Member("x"); ok || n.Suspected("x") {
			t.Fatalf("%v: x still in the view, or suspected", st)
		}
		if err := n.Check(); err != nil {
			t.Fatalf("%v: %v", st, err)
		}
		alive := peer("x")
		alive.Incarnation = uint64(t0.UnixMilli())
		at := t0
		for at.Before(t0.Add(59 * time.Minute)) {
			for _, r := range []node.Result{
				hear(n, at, peer("b"), msg.Ping{Target: "n"}, msg.Update{Member: alive}),
				hear(n, at, alive, msg.Ping{Target: "n"}),
				n.Step(at, node.ReceiveSync{Addr: "c", Sync: msg.Sync{From: peer("c"), View: []member.Member{alive}}}),
				n.Step(at, node.ReceiveSync{Addr: "x", Peer: "x", Sync: msg.Sync{From: alive, View: []member.Member{alive}, Reply: true}}),
				hear(n, at, peer("b"), msg.PingReq{Seq: 7, Target: "x", TargetAddr: "x"}),
				n.Step(at, node.Tick{}),
			} {
				if len(r.Changes) != 0 {
					t.Fatalf("%v: at %v changed %v", st, at.Sub(t0), r.Changes)
				}
				for _, p := range r.Packets {
					if p.To != "x" && slices.ContainsFunc(p.Packet.Gossip, func(u msg.Update) bool { return u.Member.ID == "x" }) {
						t.Fatalf("%v: at %v gossiped x to %s", st, at.Sub(t0), p.To)
					}
					if b, ok := p.Packet.Body.(msg.PingReq); ok && b.Target == "x" || p.To == "x" && !isAck(p) {
						t.Fatalf("%v: at %v probed x: %+v", st, at.Sub(t0), p)
					}
				}
				for _, s := range r.Syncs {
					if slices.ContainsFunc(s.Sync.View, func(m member.Member) bool { return m.ID == "x" }) {
						t.Fatalf("%v: at %v synced an entry of x: %+v", st, at.Sub(t0), s)
					}
				}
			}
			if _, ok := n.Member("x"); ok {
				t.Fatalf("%v: x back at %v", st, at.Sub(t0))
			}
			alive.Incarnation++
			at = at.Add(time.Second)
		}
		at = t0.Add(100 * time.Hour)
		n.Step(at, node.Tick{})
		if r := hear(n, at, alive, msg.Ping{Target: "n"}); len(r.Changes) != 0 {
			t.Fatalf("%v: x taken back after GraveTimeout: %v", st, r.Changes)
		}
		n.Step(at, node.Join{Seeds: []node.Seed{{ID: "x", Addr: "x"}}})
		if r := hear(n, at, alive, msg.Ping{Target: "n"}); len(r.Changes) != 1 {
			t.Fatalf("%v: x not taken back after a Join named it: %v", st, r.Changes)
		}
	}
}

// The first entry of a buried member to arrive at GraveTimeout takes it
// back, with no tick before it, and one a nanosecond before is ignored:
// from itself, from another's gossip, and in a sync, for the grave of a
// member forgotten at DeadTimeout, whose entry is no newer than its
// tombstone.
func TestABuriedMemberIsTakenBackByTheFirstEntryAtGraveTimeout(t *testing.T) {
	const g = 100 * time.Millisecond
	for _, grave := range []struct {
		name string
		dig  func(n *node.Node) time.Time
		x    member.Member
	}{
		{"of the dead", func(n *node.Node) time.Time {
			dead := peer("x")
			dead.State = member.Dead
			hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: dead, By: "b"})
			n.Step(t0.Add(time.Minute), node.Tick{})
			if _, ok := n.Member("x"); ok {
				t.Fatal("x not forgotten at DeadTimeout")
			}
			return t0.Add(time.Minute)
		}, peer("x")},
	} {
		x := grave.x
		for name, receive := range map[string]func(n *node.Node, at time.Time) node.Result{
			"its own ping": func(n *node.Node, at time.Time) node.Result { return hear(n, at, x, msg.Ping{Target: "n"}) },
			"gossip": func(n *node.Node, at time.Time) node.Result {
				return hear(n, at, peer("b"), msg.Ping{Target: "n"}, msg.Update{Member: x})
			},
			"a sync": func(n *node.Node, at time.Time) node.Result {
				return n.Step(at, node.ReceiveSync{Addr: "c", Sync: msg.Sync{From: peer("c"), View: []member.Member{x}}})
			},
		} {
			n := soloWith(func(c *node.Config) { c.GraveTimeout = g; c.DeadTimeout = time.Minute }, "b", "c", "x")
			dug := grave.dig(n)
			if r := receive(n, dug.Add(g-time.Nanosecond)); slices.ContainsFunc(r.Changes, func(m member.Member) bool { return m.ID == "x" }) {
				t.Fatalf("%s grave, %s a nanosecond before GraveTimeout: changes %v, want x still out", grave.name, name, r.Changes)
			}
			if r := receive(n, dug.Add(g)); !slices.ContainsFunc(r.Changes, func(m member.Member) bool { return m.ID == "x" }) {
				t.Fatalf("%s grave, %s at GraveTimeout: changes %v, want x taken back", grave.name, name, r.Changes)
			}
			if _, ok := n.Member("x"); !ok {
				t.Fatalf("%s grave, %s at GraveTimeout: x not in the view", grave.name, name)
			}
		}
	}
}

// A second Forget keeps the hold, which a Join naming the member lifts
// once.
func TestASecondForgetKeepsTheHold(t *testing.T) {
	const g = 100 * time.Millisecond
	n := soloWith(func(c *node.Config) { c.GraveTimeout = g }, "b", "c", "x")
	n.Step(t0, node.Forget{ID: "x"})
	n.Step(t0.Add(g), node.Forget{ID: "x"})
	alive := peer("x")
	alive.Incarnation = 5
	if r := hear(n, t0.Add(10*g), alive, msg.Ping{Target: "n"}); len(r.Changes) != 0 {
		t.Fatalf("x taken back with its hold on: %v", r.Changes)
	}
	n.Step(t0.Add(10*g), node.Join{Seeds: []node.Seed{{ID: "x", Addr: "x"}}})
	if r := hear(n, t0.Add(10*g), alive, msg.Ping{Target: "n"}); len(r.Changes) != 1 {
		t.Fatalf("x not taken back after the Join: %v", r.Changes)
	}
}

// A gone member whose newer alive entry arrives at DeadTimeout is taken
// back, not forgotten: the step reports the change alone, whether the
// member is dead or left.
func TestAGoneMemberBackAtDeadTimeoutIsNotForgotten(t *testing.T) {
	for _, st := range []member.State{member.Dead, member.Left} {
		n := soloWith(func(c *node.Config) { c.DeadTimeout = time.Minute }, "b", "x")
		gone := peer("x")
		gone.State = st
		hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: gone, By: "b"})
		alive := peer("x")
		alive.Incarnation = 1
		r := hear(n, t0.Add(time.Minute), alive, msg.Ping{Target: "n"})
		if len(r.Forgotten) != 0 || !slices.ContainsFunc(r.Changes, func(m member.Member) bool { return m.ID == "x" && m.State == member.Alive }) {
			t.Fatalf("%v: forgotten %v, changes %v; want x alive and nothing forgotten", st, r.Forgotten, r.Changes)
		}
		if m, ok := n.Member("x"); !ok || m.State != member.Alive {
			t.Fatalf("%v: x %v, %v; want alive", st, m, ok)
		}
	}
}

// A step's Forgotten lists the members out of the view at its end, each
// once, with its last entry: one forgotten to make room and taken back by a
// later entry of the same packet is reported changed alone, one changed and
// then forgotten is reported in both, and one forgotten, taken back and
// forgotten again is reported forgotten once.
func TestAStepReportsForgottenOnlyWhatStaysForgotten(t *testing.T) {
	has := func(ms []member.Member, id member.ID, st member.State) bool {
		return slices.ContainsFunc(ms, func(m member.Member) bool { return m.ID == id && m.State == st })
	}
	n := soloWith(func(c *node.Config) { c.MaxMembers = 5 }, "b", "c", "d", "z")
	d, z := peer("d"), peer("z")
	d.State, z.State = member.Dead, member.Dead
	hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: d})
	hear(n, t0.Add(time.Second), peer("b"), msg.Ack{}, msg.Update{Member: z})
	back := peer("d")
	back.Incarnation = 1
	r := hear(n, t0.Add(2*time.Second), peer("b"), msg.Ack{}, msg.Update{Member: peer("e")}, msg.Update{Member: back})
	if m, ok := n.Member("d"); !ok || m.State != member.Alive {
		t.Fatalf("d %v, %v; want taken back alive", m, ok)
	}
	if !has(r.Changes, "d", member.Alive) || slices.ContainsFunc(r.Forgotten, func(m member.Member) bool { return m.ID == "d" }) {
		t.Fatalf("forgotten and taken back: changes %v, forgotten %v; want d alive changed and not forgotten", r.Changes, r.Forgotten)
	}

	n = soloWith(func(c *node.Config) { c.MaxMembers = 5 }, "b", "c", "d", "z")
	r = hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: d}, msg.Update{Member: peer("e")})
	if _, ok := n.Member("d"); ok {
		t.Fatal("d still in the view after it died and made room")
	}
	if !has(r.Changes, "d", member.Dead) || !has(r.Forgotten, "d", member.Dead) {
		t.Fatalf("changed and then forgotten: changes %v, forgotten %v; want d dead in both", r.Changes, r.Forgotten)
	}
	n = soloWith(func(c *node.Config) { c.MaxMembers = 5 }, "b", "c", "d", "z")
	hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: d})
	hear(n, t0.Add(time.Second), peer("b"), msg.Ack{}, msg.Update{Member: z})
	again := peer("d")
	again.Incarnation, again.State = 2, member.Dead
	r = hear(n, t0.Add(2*time.Second), peer("b"), msg.Ack{}, msg.Update{Member: peer("e")}, msg.Update{Member: back}, msg.Update{Member: again}, msg.Update{Member: peer("f")})
	var ds []member.Member
	for _, m := range r.Forgotten {
		if m.ID == "d" {
			ds = append(ds, m)
		}
	}
	if _, ok := n.Member("d"); ok || len(ds) != 1 || ds[0] != again {
		t.Fatalf("forgotten, taken back and forgotten again: forgotten %v; want d once, as %v", r.Forgotten, again)
	}
}

// Forgetting the node itself, or the empty ID, does nothing. A member the
// node doesn't know yet is reported nowhere but held out all the same: its
// entry arriving after the Forget, from itself or from another, is
// ignored.
func TestForgetOfSelfOrAStranger(t *testing.T) {
	n := soloWith(nil, "b")
	self := n.Self()
	for _, id := range []member.ID{"n", "", "x"} {
		if r := n.Step(t0, node.Forget{ID: id}); len(r.Forgotten) != 0 || len(r.Changes) != 0 {
			t.Fatalf("Forget(%q) = %+v", id, r)
		}
	}
	if n.Self() != self || len(n.Members()) != 2 {
		t.Fatalf("self %+v, members %v", n.Self(), n.Members())
	}
	// The node's refutation of a suspicion of it stays queued.
	sn := n.Self()
	sn.State = member.Suspect
	hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: sn, By: "b"})
	refuted := n.Self()
	n.Step(t0, node.Forget{ID: "n"})
	n.Step(t0, node.Forget{ID: ""})
	if r := hear(n, t0, peer("b"), msg.Ping{Seq: 1, Target: "n"}); !slices.ContainsFunc(r.Packets, func(p node.Packet) bool {
		return slices.ContainsFunc(p.Packet.Gossip, func(u msg.Update) bool { return u.Member == refuted })
	}) {
		t.Fatalf("the refutation %+v didn't go out after forgetting the node itself: %+v", refuted, r.Packets)
	}
	x := peer("x")
	x.Incarnation = uint64(t0.UnixMilli())
	if r := hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: x}); len(r.Changes) != 0 {
		t.Fatalf("took x from b: %v", r.Changes)
	}
	if r := n.Step(t0, node.ReceiveSync{Addr: "x", Peer: "x", Sync: msg.Sync{From: x, View: []member.Member{x}}}); len(r.Changes) != 0 {
		t.Fatalf("took x from its own sync: %v", r.Changes)
	}
	if err := n.Check(); err != nil {
		t.Fatal(err)
	}
}

// A Join that names a forgotten member as a seed lifts its hold: its next
// entry is taken at once, whether or not the node knew it before the
// Forget. A seed the join skips lifts nothing.
func TestJoinTakesBackAForgottenSeed(t *testing.T) {
	for _, known := range []bool{true, false} {
		ids := []string{"b"}
		if known {
			ids = append(ids, "x")
		}
		n := soloWith(nil, ids...)
		n.Step(t0, node.Forget{ID: "x"})
		// A seed the join skips, at the node's own address, lifts nothing.
		n.Step(t0, node.Join{Seeds: []node.Seed{{ID: "x", Addr: "n"}}})
		early := peer("x")
		early.Incarnation = uint64(t0.UnixMilli())
		if r := n.Step(t0, node.ReceiveSync{Addr: "x", Peer: "x", Sync: msg.Sync{From: early, View: []member.Member{early}}}); len(r.Changes) != 0 {
			t.Fatalf("known %v: a skipped seed lifted the hold", known)
		}
		n.Step(t0, node.Join{Seeds: []node.Seed{seed("x")}})
		x := peer("x")
		x.Incarnation = uint64(t0.UnixMilli())
		if r := n.Step(t0, node.ReceiveSync{Addr: "x", Peer: "x", Sync: msg.Sync{From: x, View: []member.Member{x}}}); len(r.Changes) == 0 {
			t.Fatalf("known %v: x not taken back after Join named it", known)
		}
		if err := n.Check(); err != nil {
			t.Fatal(err)
		}
	}
}

// A forgotten seed is no seed: a joining node stops retrying it.
func TestAForgottenSeedIsNotRetried(t *testing.T) {
	n := solo(func(c *node.Config) { c.SyncEvery = 0 })
	n.Step(t0, node.Join{Seeds: []node.Seed{seed("s"), seed("t")}})
	n.Step(t0, node.Forget{ID: "s"})
	now := t0
	for range 20 {
		r := n.Step(now, node.Tick{})
		for _, s := range r.Syncs {
			if s.To == "s" {
				t.Fatalf("synced with a forgotten seed at %v", now.Sub(t0))
			}
		}
		now = r.Wake
	}
	n.Step(now, node.Forget{ID: "t"})
	if n.Joining() {
		t.Fatal("still joining with every seed forgotten")
	}
}

// In a cluster, a node that forgets a member keeps it out while the
// member runs on and every other node still gossips it alive.
func TestAForgottenMemberStaysOutWhileOthersGossipIt(t *testing.T) {
	tn := newNet(t, 5, func(_ int, c *node.Config) { c.GraveTimeout = 2 * time.Minute })
	tn.joinAll()
	tn.after(10 * time.Second)
	if !tn.everyone("n04", member.Alive) {
		t.Fatal("not converged")
	}
	tn.step("n00", node.Forget{ID: "n04"})
	forgot := tn.now
	for tn.now.Sub(forgot) < 2*time.Minute-time.Second {
		tn.after(500 * time.Millisecond)
		if m, ok := tn.state("n00", "n04"); ok {
			t.Fatalf("n04 back at n00 after %v: %+v", tn.now.Sub(forgot), m)
		}
		if !tn.everyone("n04", member.Alive, "n00") {
			t.Fatalf("the others lost n04 at %v", tn.now.Sub(forgot))
		}
	}
	tn.after(2 * time.Minute)
	if m, ok := tn.state("n00", "n04"); ok {
		t.Fatalf("n04 back at n00 after GraveTimeout: %+v", m)
	}
}

// A message from another member at a seed's address is no message from the
// seed: it isn't answered as the seed.
func TestAnUnreadableMessageFromAnotherAtASeedsAddressIsNotAnswered(t *testing.T) {
	n := solo(nil)
	n.Step(t0, node.Join{Seeds: []node.Seed{{ID: "s", Addr: "s-addr"}}})
	if r := n.Step(t0, node.Unreadable{Addr: "s-addr", Peer: "other"}); len(r.Packets) != 0 {
		t.Fatalf("answered %+v", r.Packets)
	}
	if r := n.Step(t0, node.Unreadable{Addr: "s-addr", Peer: "s"}); len(r.Packets) != 1 || r.Packets[0].To != "s" {
		t.Fatalf("the seed answered %+v", r.Packets)
	}
}

// A probe of a member under way when it is forgotten ends with no verdict:
// no indirect probes about it, no failed probe and no change to the
// node's health.
func TestForgetEndsAProbeUnderWayWithNoVerdict(t *testing.T) {
	for _, rule := range []health.Rule{health.RuleNoNack, health.RuleLifeguard} {
		n := soloWith(func(c *node.Config) { c.HealthRule = rule }, "b", "c", "d", "x")
		at := t0
		for i := 0; ; i++ {
			if id, ok := n.Probing(); ok && id == "x" {
				break
			}
			if i > 100 {
				t.Fatalf("%v: never probed x", rule)
			}
			at = n.Step(at, node.Tick{}).Wake
		}
		health := n.Health()
		n.Step(at, node.Forget{ID: "x"})
		for range 20 {
			r := n.Step(at, node.Tick{})
			for _, p := range r.Packets {
				if b, ok := p.Packet.Body.(msg.PingReq); ok && b.Target == "x" {
					t.Fatalf("%v: asked %s to ping a forgotten member", rule, p.To)
				}
			}
			for _, e := range r.Events {
				if e.Kind == node.ProbeFailed && e.Member == "x" {
					t.Fatalf("%v: a failed probe of a forgotten member", rule)
				}
			}
			if id, ok := n.Probing(); !ok || id != "x" {
				break
			}
			at = r.Wake
		}
		if n.Health() != health {
			t.Fatalf("%v: health %d, was %d", rule, n.Health(), health)
		}
	}
}

// A sync goes in the highest wire version the node and the member both
// read, by the member's entry.
func TestASyncGoesInTheHighestCommonVersion(t *testing.T) {
	n := solo(func(c *node.Config) { c.MinVersion, c.Version, c.SyncEvery = 1, 3, 2 })
	b := peer("b")
	b.MinVersion, b.Version = 2, 2
	r := n.Step(t0, node.ReceiveSync{Addr: "b", Sync: msg.Sync{From: b, View: []member.Member{b}, Reply: true}})
	if len(r.Syncs) != 1 || r.Syncs[0].To != "b" || r.Syncs[0].Version != 2 {
		t.Fatalf("answer %+v, want one to b in version 2", r.Syncs)
	}
	now := t0
	for range 2000 {
		r := n.Step(now, node.Tick{})
		if len(r.Syncs) > 0 {
			if r.Syncs[0].Version != 2 {
				t.Fatalf("periodic sync %+v, want version 2", r.Syncs)
			}
			return
		}
		now = r.Wake
	}
	t.Fatal("no periodic sync")
}

func isAck(p node.Packet) bool { _, ok := p.Packet.Body.(msg.Ack); return ok }

// Generated sequences of Forget, Join, gossip of one or two entries, a
// member's own packets and ticks, half of them in a view capped at five
// members, some landing at a hold's end or a nanosecond before it,
// against a model of which members are held out: after every step a member
// Forget held, which no Join has named since and whose GraveTimeout hasn't
// passed, is not in the view, whatever was said of it; with no cap on the
// view, one whose hold ended or was lifted, and that nobody gossiped since,
// is taken back by its own ping; Forget reports exactly the members that
// were in the view, and a step reports a member forgotten at most once,
// and none in the view.
func TestHeldMembersAgainstAModel(t *testing.T) {
	r := rand.New(rand.NewPCG(17, 18))
	ids := []string{"b", "c", "x", "y", "z"}
	const graveTimeout = 2 * time.Minute
	for run := range 200 {
		n := soloWith(func(c *node.Config) {
			c.GraveTimeout = graveTimeout
			if run%2 == 1 {
				c.MaxMembers = 5
			}
		}, "b", "c")
		held := map[string]time.Time{}
		free := map[string]bool{}
		now := t0
		for step := range 150 {
			now = now.Add(time.Duration(r.IntN(4000)) * time.Millisecond)
			id := ids[r.IntN(len(ids))]
			var what string
			var res node.Result
			switch r.IntN(6) {
			case 0:
				what = "Forget " + id
				_, was := n.Member(member.ID(id))
				res = n.Step(now, node.Forget{ID: member.ID(id)})
				if got := len(res.Forgotten) == 1 && string(res.Forgotten[0].ID) == id; got != was || len(res.Forgotten) > 1 {
					t.Fatalf("run %d step %d: %s reported %v, in the view before %v", run, step, what, res.Forgotten, was)
				}
				held[id] = now
				delete(free, id)
			case 1:
				what = "Join " + id
				res = n.Step(now, node.Join{Seeds: []node.Seed{seed(id)}})
				if _, ok := held[id]; ok {
					free[id] = true
				}
				delete(held, id)
			case 2, 3:
				from := peer(ids[r.IntN(2)])
				var us []msg.Update
				for k := range 1 + r.IntN(2) {
					g := id
					if k > 0 {
						g = ids[2+r.IntN(len(ids)-2)]
					}
					m := peer(g)
					m.Incarnation = uint64(t0.UnixMilli()) + r.Uint64N(1<<20)
					m.State = member.State(r.IntN(4))
					us = append(us, msg.Update{Member: m, By: from.ID})
					delete(free, g)
				}
				what = fmt.Sprintf("%s gossips %v", from.ID, us)
				res = hear(n, now, from, msg.Ack{}, us...)
			case 4:
				m := peer(id)
				m.Incarnation = uint64(t0.UnixMilli()) + r.Uint64N(1<<20)
				what = fmt.Sprintf("%s pings at %d", id, m.Incarnation)
				res = hear(n, now, m, msg.Ping{Seq: 1, Target: "n"})
				if _, ok := n.Member(m.ID); free[id] && !ok && run%2 == 0 {
					t.Fatalf("run %d step %d, after %s: %s, no longer held, not taken back", run, step, what, id)
				}
				delete(free, id)
			default:
				what = "Tick"
				res = n.Step(now, node.Tick{})
			}
			if err := n.Check(); err != nil {
				t.Fatalf("run %d step %d, after %s: %v", run, step, what, err)
			}
			for i, f := range res.Forgotten {
				if m, ok := n.Member(f.ID); ok {
					t.Fatalf("run %d step %d, after %s: %s reported forgotten, in the view %+v", run, step, what, f.ID, m)
				}
				if slices.ContainsFunc(res.Forgotten[i+1:], func(o member.Member) bool { return o.ID == f.ID }) {
					t.Fatalf("run %d step %d, after %s: %s reported forgotten twice: %v", run, step, what, f.ID, res.Forgotten)
				}
			}
			for h, at := range held {
				if m, ok := n.Member(member.ID(h)); ok {
					t.Fatalf("run %d step %d, after %s: %s held since %v, in the view %+v", run, step, what, h, at.Sub(t0), m)
				}
				for _, c := range res.Changes {
					if string(c.ID) == h {
						t.Fatalf("run %d step %d, after %s: a change of %s, held", run, step, what, h)
					}
				}
			}
		}
	}
}

// Whatever address, peer and version an unreadable message comes with, the
// node answers with at most one packet, at the address it came from,
// naming the proven peer, if there is one, as a member it knows or as the
// seed at that address; with no peer, the member it knows at that address
// or the seed there.
func FuzzUnreadableIsAnsweredOnlyByName(f *testing.F) {
	for _, s := range [][2]string{{"b", "b"}, {"b", ""}, {"s-addr", "s"}, {"s-addr", ""}, {"s-addr", "other"}, {"x", "x"}, {"", ""}, {"n", "n"}} {
		f.Add(s[0], s[1], uint64(9))
	}
	f.Fuzz(func(t *testing.T, addr, peer string, version uint64) {
		n := soloWith(nil, "b")
		n.Step(t0, node.Join{Seeds: []node.Seed{{ID: "s", Addr: "s-addr"}}})
		n.Step(t0, node.Forget{ID: "x"})
		r := n.Step(t0, node.Unreadable{Addr: addr, Peer: member.ID(peer), Version: version})
		if err := n.Check(); err != nil {
			t.Fatal(err)
		}
		if len(r.Packets) > 1 {
			t.Fatalf("%d answers", len(r.Packets))
		}
		for _, p := range r.Packets {
			m, known := n.Member(p.To)
			seed := p.To == "s" && addr == "s-addr"
			if p.Addr != addr || p.To == "" || p.To == "n" || peer != "" && string(p.To) != peer || !seed && (!known || peer == "" && m.Addr != addr) {
				t.Fatalf("Unreadable{%q, %q} answered %+v", addr, peer, p)
			}
		}
	})
}

// Forget of a member already buried drops what the node still has queued
// of it: a newer tombstone the grave took is gossiped no more.
func TestForgetOfABuriedMemberDropsItsQueuedGossip(t *testing.T) {
	n := soloWith(func(c *node.Config) { c.DeadTimeout = time.Second }, "b", "x")
	dead := peer("x")
	dead.State = member.Dead
	hear(n, t0, peer("b"), msg.Ack{}, msg.Update{Member: dead, By: "b"})
	at := t0.Add(2 * time.Second)
	n.Step(at, node.Tick{})
	if _, ok := n.Member("x"); ok {
		t.Fatal("x not forgotten at DeadTimeout")
	}
	newer := dead
	newer.Incarnation++
	hear(n, at, peer("b"), msg.Ack{}, msg.Update{Member: newer, By: "b"})
	n.Step(at, node.Forget{ID: "x"})
	for range 50 {
		r := n.Step(at, node.Tick{})
		for _, p := range r.Packets {
			if slices.ContainsFunc(p.Packet.Gossip, func(u msg.Update) bool { return u.Member.ID == "x" }) {
				t.Fatalf("gossiped x to %s after it was forgotten", p.To)
			}
		}
		at = r.Wake
	}
}

// A Join that lifts a hold drops what the hold kept: the round-trip time a
// direct ack of the abandoned probe left.
func TestALiftedHoldKeepsNothing(t *testing.T) {
	n := soloWith(nil, "b", "c", "x")
	at := t0
	for i := 0; ; i++ {
		if id, ok := n.Probing(); ok && id == "x" {
			break
		}
		if i > 100 {
			t.Fatal("never probed x")
		}
		at = n.Step(at, node.Tick{}).Wake
	}
	n.Step(at, node.Forget{ID: "x"})
	var seq uint32
	for s := uint32(1); s < 200; s++ {
		before := n.RTT("x")
		hear(n, at.Add(5*time.Millisecond), peer("x"), msg.Ack{Seq: s})
		if n.RTT("x") != before {
			seq = s
			break
		}
	}
	if seq == 0 {
		t.Fatal("no round-trip time from the abandoned probe's ack")
	}
	n.Step(at.Add(10*time.Millisecond), node.Join{Seeds: []node.Seed{seed("x")}})
	if rtt := n.RTT("x"); rtt != 0 {
		t.Fatalf("a round-trip time of %v kept after the hold was lifted", rtt)
	}
	if err := n.Check(); err != nil {
		t.Fatal(err)
	}
}

// Forget of a member the node learned of from a sync and hasn't gossiped
// on leaves no unspread entry of it.
func TestForgetOfAnUnspreadMember(t *testing.T) {
	n := solo(nil)
	b, x := peer("b"), peer("x")
	n.Step(t0, node.ReceiveSync{Addr: "b", Sync: msg.Sync{From: b, View: []member.Member{b, x}}})
	n.Step(t0, node.Forget{ID: "x"})
	if err := n.Check(); err != nil {
		t.Fatal(err)
	}
}
