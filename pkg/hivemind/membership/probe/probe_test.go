package probe

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/view"
)

var t0 = time.Unix(1000, 0)

var cfg = Config{Interval: time.Second, Timeout: 200 * time.Millisecond, Helpers: 3, RTTMult: 3, Nacks: true}

func cluster(n int) *view.View {
	v := view.New(member.Member{ID: "self", Caps: member.Of(member.CapNack)})
	for i := range n {
		id := member.ID(fmt.Sprintf("m%02d", i))
		v.Apply(t0, msg.Update{Member: member.Member{ID: id, Addr: string(id), Caps: member.Of(member.CapNack)}})
	}
	return v
}

func pings(o Out) []Send {
	var out []Send
	for _, s := range o.Sends {
		if _, ok := s.Body.(msg.Ping); ok {
			out = append(out, s)
		}
	}
	return out
}

// Every live member is probed exactly once per round, in an order that
// changes between rounds (SWIM 4.3).
func TestEachMemberOncePerRound(t *testing.T) {
	r := rand.New(rand.NewPCG(9, 10))
	for range 50 {
		n := r.IntN(20) + 1
		v := cluster(n)
		d := New(cfg, rand.New(rand.NewPCG(r.Uint64(), 0)), t0)
		var rounds [3][]member.ID
		now := t0
		for i := range 3 * n {
			o := d.Tick(now, v, 1)
			p := pings(o)
			if len(p) != 1 {
				t.Fatalf("period %d: %d pings", i, len(p))
			}
			rounds[i/n] = append(rounds[i/n], p[0].To.ID)
			d.Ack(now, p[0].To.ID, p[0].Body.(msg.Ping).Seq)
			now = now.Add(time.Second)
		}
		for _, round := range rounds {
			seen := map[member.ID]bool{}
			for _, id := range round {
				if seen[id] || id == "self" {
					t.Fatalf("round %v probes %s twice or probes self", round, id)
				}
				seen[id] = true
			}
			if len(seen) != n {
				t.Fatalf("round probed %d of %d members", len(seen), n)
			}
		}
	}
}

func TestOrderIsReshuffled(t *testing.T) {
	v := cluster(10)
	d := New(cfg, rand.New(rand.NewPCG(1, 1)), t0)
	var order [2]string
	now := t0
	for i := range 20 {
		p := pings(d.Tick(now, v, 1))[0]
		order[i/10] += string(p.To.ID)
		d.Ack(now, p.To.ID, p.Body.(msg.Ping).Seq)
		now = now.Add(time.Second)
	}
	if order[0] == order[1] {
		t.Fatalf("two rounds in the same order: %s", order[0])
	}
}

// A probe without an ack goes through k helpers at the timeout, and fails
// at the end of the period with the helpers' nacks counted (SWIM 3.1,
// Lifeguard 4.1).
func TestFailedProbe(t *testing.T) {
	v := cluster(5)
	d := New(cfg, rand.New(rand.NewPCG(2, 2)), t0)
	p := pings(d.Tick(t0, v, 1))[0]
	target, seq := p.To.ID, p.Body.(msg.Ping).Seq
	if due := d.Due(); !due.Equal(t0.Add(200 * time.Millisecond)) {
		t.Fatalf("Due = %v, want the timeout", due)
	}
	o := d.Tick(t0.Add(200*time.Millisecond), v, 1)
	if len(o.Sends) != 3 {
		t.Fatalf("%d sends at the timeout, want 3 ping-reqs", len(o.Sends))
	}
	for _, s := range o.Sends {
		req, ok := s.Body.(msg.PingReq)
		if !ok || req.Target != target || req.Seq != seq || s.To.ID == target {
			t.Fatalf("send %+v", s)
		}
	}
	d.Nack(o.Sends[0].To.ID, seq)
	d.Nack("stranger", seq) // a member not asked doesn't count
	o = d.Tick(t0.Add(time.Second), v, 1)
	if len(o.Failed) != 1 || o.Failed[0].Target.ID != target || o.Failed[0].Asked != 3 || o.Failed[0].Nacks != 1 {
		t.Fatalf("Failed = %+v", o.Failed)
	}
	if len(pings(o)) != 1 {
		t.Fatal("the next probe didn't start at the end of the period")
	}
}

func TestDirectAck(t *testing.T) {
	v := cluster(5)
	d := New(cfg, rand.New(rand.NewPCG(3, 3)), t0)
	p := pings(d.Tick(t0, v, 1))[0]
	o := d.Ack(t0.Add(50*time.Millisecond), p.To.ID, p.Body.(msg.Ping).Seq)
	if o.Succeeded != 1 {
		t.Fatal("ack not counted")
	}
	if d.RTT(p.To.ID) != 50*time.Millisecond {
		t.Fatalf("RTT = %v", d.RTT(p.To.ID))
	}
	if o := d.Tick(t0.Add(200*time.Millisecond), v, 1); len(o.Sends) != 0 {
		t.Fatalf("probed indirectly after an ack: %+v", o.Sends)
	}
	if o := d.Tick(t0.Add(time.Second), v, 1); len(o.Failed) != 0 {
		t.Fatal("an acked probe failed")
	}
}

func TestForwardedAckCountsWithoutRTT(t *testing.T) {
	v := cluster(5)
	d := New(cfg, rand.New(rand.NewPCG(4, 4)), t0)
	p := pings(d.Tick(t0, v, 1))[0]
	o := d.Tick(t0.Add(200*time.Millisecond), v, 1)
	helper := o.Sends[0].To.ID
	if o := d.Ack(t0.Add(600*time.Millisecond), helper, p.Body.(msg.Ping).Seq); o.Succeeded != 1 {
		t.Fatal("a forwarded ack didn't count")
	}
	if d.RTT(p.To.ID) != 0 || d.RTT(helper) != 0 {
		t.Fatal("a forwarded ack gave a round-trip sample")
	}
}

// Only the target or a helper asked acks a probe: an ack with the right
// sequence number from anyone else, before the helpers are asked or after,
// counts for nothing, and the probe still fails.
func TestAnAckFromAnyoneElseCountsForNothing(t *testing.T) {
	v := cluster(5)
	d := New(cfg, rand.New(rand.NewPCG(4, 4)), t0)
	p := pings(d.Tick(t0, v, 1))[0]
	seq := p.Body.(msg.Ping).Seq
	other := func(asked []member.ID) member.ID {
		for _, id := range v.IDs() {
			if id != "self" && id != p.To.ID && !slices.Contains(asked, id) {
				return id
			}
		}
		t.Fatal("no member left out")
		return ""
	}
	if o := d.Ack(t0.Add(50*time.Millisecond), other(nil), seq); o.Succeeded != 0 {
		t.Fatal("an ack from a member not probed counted")
	}
	o := d.Tick(t0.Add(200*time.Millisecond), v, 1)
	var asked []member.ID
	for _, s := range o.Sends {
		asked = append(asked, s.To.ID)
	}
	if len(asked) != cfg.Helpers {
		t.Fatalf("asked %v", asked)
	}
	if o := d.Ack(t0.Add(300*time.Millisecond), other(asked), seq); o.Succeeded != 0 {
		t.Fatal("an ack from a member not asked counted")
	}
	if o := d.Tick(t0.Add(time.Second), v, 1); len(o.Failed) != 1 {
		t.Fatal("acks from others kept the probe from failing")
	}
}

func TestTimeoutFollowsRTTUpToHalfAPeriod(t *testing.T) {
	d := New(cfg, rand.New(rand.NewPCG(5, 5)), t0)
	d.rtt["a"] = 100 * time.Millisecond
	if got := d.timeout("a", 1); got != 300*time.Millisecond {
		t.Fatalf("timeout = %v, want 3 × RTT", got)
	}
	d.rtt["a"] = time.Second
	if got := d.timeout("a", 1); got != 500*time.Millisecond {
		t.Fatalf("timeout = %v, want half a period", got)
	}
	if got := d.timeout("b", 4); got != 800*time.Millisecond {
		t.Fatalf("timeout at scale 4 = %v", got)
	}
}

// helperView is a view with an asker and a target, both live members.
func helperView(askerCaps member.Caps) *view.View {
	v := view.New(member.Member{ID: "self"})
	v.Apply(t0, msg.Update{Member: member.Member{ID: "asker", Addr: "asker", Caps: askerCaps}})
	v.Apply(t0, msg.Update{Member: member.Member{ID: "t", Addr: "t-addr"}})
	return v
}

func TestHelper(t *testing.T) {
	asker := member.Member{ID: "asker", Addr: "asker", Caps: member.Of(member.CapNack)}
	req := msg.PingReq{Seq: 77, Target: "t", TargetAddr: "elsewhere"}
	v := helperView(asker.Caps)

	d := New(cfg, rand.New(rand.NewPCG(6, 6)), t0.Add(time.Hour))
	o := d.PingReq(t0, v, asker, req, 1)
	ping := o.Sends[0]
	// The view's address, not the one the request names.
	if ping.To.Addr != "t-addr" || ping.Body.(msg.Ping).Target != "t" {
		t.Fatalf("helper sent %+v", ping)
	}
	o = d.Ack(t0.Add(10*time.Millisecond), "t", ping.Body.(msg.Ping).Seq)
	if len(o.Sends) != 1 || o.Sends[0].To.ID != "asker" || o.Sends[0].Body != (msg.Ack{Seq: 77}) {
		t.Fatalf("ack not forwarded: %+v", o.Sends)
	}
	if o := d.Tick(t0.Add(time.Second), v, 1); len(o.Sends) != 0 {
		t.Fatalf("nacked after forwarding an ack: %+v", o.Sends)
	}

	// No ack: a nack at the base timeout, to an asker that has CapNack,
	// whatever this node's health.
	d = New(cfg, rand.New(rand.NewPCG(6, 6)), t0.Add(time.Hour))
	d.PingReq(t0, v, asker, req, 4)
	if due := d.Due(); !due.Equal(t0.Add(cfg.Timeout)) {
		t.Fatalf("nack due %v after the ping-req, want the base timeout %v", due.Sub(t0), cfg.Timeout)
	}
	o = d.Tick(d.Due(), v, 4)
	if len(o.Sends) != 1 || o.Sends[0].Body != (msg.Nack{Seq: 77}) {
		t.Fatalf("Sends = %+v", o.Sends)
	}

	// An asker of an older version gets no nack.
	d = New(cfg, rand.New(rand.NewPCG(6, 6)), t0.Add(time.Hour))
	old := member.Member{ID: "asker", Addr: "asker"}
	d.PingReq(t0, helperView(0), old, req, 1)
	if o := d.Tick(t0.Add(time.Second), v, 1); len(o.Sends) != 0 {
		t.Fatalf("nacked an asker without CapNack: %+v", o.Sends)
	}
}

// A helper pings no address a stranger names: not for a sender that isn't
// a live member, not a target that isn't, and a copy of a request once.
func TestHelperRefuses(t *testing.T) {
	v := helperView(member.Of(member.CapNack))
	asker := member.Member{ID: "asker", Addr: "asker", Caps: member.Of(member.CapNack)}
	for name, tc := range map[string]struct {
		asker member.Member
		req   msg.PingReq
	}{
		"a stranger asks":       {member.Member{ID: "stranger", Addr: "x"}, msg.PingReq{Seq: 1, Target: "t"}},
		"for a stranger":        {asker, msg.PingReq{Seq: 1, Target: "victim", TargetAddr: "victim:53"}},
		"for the helper itself": {asker, msg.PingReq{Seq: 1, Target: "self"}},
	} {
		d := New(cfg, rand.New(rand.NewPCG(7, 7)), t0.Add(time.Hour))
		if o := d.PingReq(t0, v, tc.asker, tc.req, 1); len(o.Sends) != 0 {
			t.Errorf("%s: sent %+v", name, o.Sends)
		}
	}
	d := New(cfg, rand.New(rand.NewPCG(7, 7)), t0.Add(time.Hour))
	d.PingReq(t0, v, asker, msg.PingReq{Seq: 5, Target: "t"}, 1)
	if o := d.PingReq(t0, v, asker, msg.PingReq{Seq: 5, Target: "t"}, 1); len(o.Sends) != 0 {
		t.Fatal("a duplicated ping-req was relayed twice")
	}
	if n := len(d.Tick(d.Due(), v, 1).Sends); n != 1 {
		t.Fatalf("%d nacks for one relayed request", n)
	}
}

// A helper's nack counts once, however many copies of it arrive.
func TestANackCountsOnce(t *testing.T) {
	v := cluster(5)
	d := New(cfg, rand.New(rand.NewPCG(2, 2)), t0)
	p := pings(d.Tick(t0, v, 1))[0]
	seq := p.Body.(msg.Ping).Seq
	o := d.Tick(t0.Add(200*time.Millisecond), v, 1)
	helper := o.Sends[0].To.ID
	d.Nack(helper, seq)
	d.Nack(helper, seq)
	o = d.Tick(t0.Add(time.Second), v, 1)
	if f := o.Failed[0]; f.Nacks != 1 || f.Nackers != 3 || f.Asked != 3 {
		t.Fatalf("Failed = %+v", f)
	}
}

// A relay across a pause gives no round-trip sample.
func TestARelayAcrossAPauseGivesNoSample(t *testing.T) {
	v := helperView(0)
	d := New(cfg, rand.New(rand.NewPCG(8, 8)), t0.Add(time.Hour))
	o := d.PingReq(t0, v, member.Member{ID: "asker", Addr: "asker"}, msg.PingReq{Seq: 1, Target: "t"}, 1)
	d.Shift(5 * time.Second)
	d.Ack(t0.Add(5*time.Second), "t", o.Sends[0].Body.(msg.Ping).Seq)
	if d.RTT("t") != 0 {
		t.Fatalf("RTT %v across a pause", d.RTT("t"))
	}
}

// Helpers are alive members.
func TestHelpersAreAlive(t *testing.T) {
	v := cluster(4)
	for _, id := range []member.ID{"m01", "m02"} {
		v.Apply(t0, msg.Update{Member: member.Member{ID: id, Addr: string(id), State: member.Suspect}})
	}
	d := New(cfg, rand.New(rand.NewPCG(3, 3)), t0)
	target := pings(d.Tick(t0, v, 1))[0].To.ID
	for _, s := range d.Tick(t0.Add(200*time.Millisecond), v, 1).Sends {
		if m, _ := v.Get(s.To.ID); m.State != member.Alive || s.To.ID == target {
			t.Fatalf("asked %v", m)
		}
	}
}

func TestForgetDropsTheRTT(t *testing.T) {
	d := New(cfg, rand.New(rand.NewPCG(1, 1)), t0)
	d.sample("x", time.Millisecond)
	d.Forget("x")
	if d.RTT("x") != 0 {
		t.Fatal("kept a forgotten member's RTT")
	}
}

func TestScaleStretchesThePeriod(t *testing.T) {
	v := cluster(3)
	d := New(cfg, rand.New(rand.NewPCG(7, 7)), t0)
	d.Tick(t0, v, 3)
	o := d.Tick(t0.Add(time.Second), v, 3)
	if len(pings(o)) != 0 {
		t.Fatal("a new probe started before the stretched period ended")
	}
	if due := d.Due(); !due.Equal(t0.Add(600*time.Millisecond)) && !due.Equal(t0.Add(3*time.Second)) {
		t.Fatalf("Due = %v", due)
	}
}

func TestHealingProbesAGoneMember(t *testing.T) {
	v := cluster(3)
	v.Apply(t0, msg.Update{Member: member.Member{ID: "m01", Addr: "m01", State: member.Dead}})
	c := cfg
	c.HealEvery = 2
	d := New(c, rand.New(rand.NewPCG(8, 8)), t0)
	now := t0
	var probed []member.ID
	for range 4 {
		o := d.Tick(now, v, 1)
		if len(o.Failed) != 0 {
			t.Fatalf("an unanswered probe failed: %+v", o.Failed)
		}
		p := pings(o)[0]
		probed = append(probed, p.To.ID)
		if p.To.ID != "m01" {
			d.Ack(now, p.To.ID, p.Body.(msg.Ping).Seq)
		}
		now = now.Add(time.Second)
	}
	if probed[1] != "m01" || probed[3] != "m01" {
		t.Fatalf("probed %v, want m01 every second probe", probed)
	}
}

func TestShift(t *testing.T) {
	v := cluster(3)
	d := New(cfg, rand.New(rand.NewPCG(9, 9)), t0)
	p := pings(d.Tick(t0, v, 1))[0]
	d.Shift(5 * time.Second)
	if due := d.Due(); !due.Equal(t0.Add(5200 * time.Millisecond)) {
		t.Fatalf("Due after Shift = %v", due)
	}
	d.Ack(t0.Add(5*time.Second), p.To.ID, p.Body.(msg.Ping).Seq)
	if d.RTT(p.To.ID) != 0 {
		t.Fatal("a probe across a pause gave a round-trip sample")
	}
}

// A helper holds at most maxRelays indirect probes for others at once.
func TestRelaysAreCapped(t *testing.T) {
	v := cluster(3)
	d := New(cfg, rand.New(rand.NewPCG(9, 9)), t0.Add(time.Hour))
	asker := member.Member{ID: "m00", Addr: "m00"}
	for i := range maxRelays + 50 {
		d.PingReq(t0, v, asker, msg.PingReq{Seq: uint32(i), Target: "m01"}, 1)
	}
	if len(d.relays) != maxRelays {
		t.Fatalf("%d relays held", len(d.relays))
	}
}

// Healing probes go to dead members, not to members that left.
func TestHealingSkipsMembersThatLeft(t *testing.T) {
	v := cluster(3)
	v.Apply(t0, msg.Update{Member: member.Member{ID: "m01", Addr: "m01", State: member.Left}})
	c := cfg
	c.HealEvery = 1
	d := New(c, rand.New(rand.NewPCG(8, 8)), t0)
	now := t0
	for range 10 {
		for _, s := range pings(d.Tick(now, v, 1)) {
			if s.To.ID == "m01" {
				t.Fatal("probed a member that left")
			}
		}
		now = now.Add(time.Second)
	}
}

// A member added mid-round is probed in what is left of that round, and
// every other member still once.
func TestAddedMemberIsProbedThisRound(t *testing.T) {
	for seed := range uint64(50) {
		v := cluster(6)
		d := New(cfg, rand.New(rand.NewPCG(seed, 3)), t0)
		now := t0
		probe := func() member.ID {
			p := pings(d.Tick(now, v, 1))[0]
			d.Ack(now, p.To.ID, p.Body.(msg.Ping).Seq)
			now = now.Add(time.Second)
			return p.To.ID
		}
		seen := map[member.ID]int{}
		for range 3 {
			seen[probe()]++
		}
		v.Apply(now, msg.Update{Member: member.Member{ID: "new", Addr: "new"}})
		d.Add("new")
		d.Add("new") // a second Add of the same member changes nothing
		for range 4 {
			seen[probe()]++
		}
		if len(seen) != 7 || seen["new"] != 1 {
			t.Fatalf("seed %d: the round probed %v", seed, seen)
		}
	}
}

// Probing names the target of the probe in flight, and nothing between
// probes.
func TestProbing(t *testing.T) {
	v := cluster(3)
	d := New(cfg, rand.New(rand.NewPCG(1, 2)), t0)
	if id, ok := d.Probing(); ok {
		t.Fatalf("Probing = %s before the first probe", id)
	}
	p := pings(d.Tick(t0, v, 1))[0]
	if id, ok := d.Probing(); !ok || id != p.To.ID {
		t.Fatalf("Probing = %s, %v, want %s", id, ok, p.To.ID)
	}
}

// Healing probes go to forgotten members that died, never to forgotten
// members that left.
func TestHealingProbesDeadGravesNotLeftOnes(t *testing.T) {
	for _, tc := range []struct {
		state member.State
		want  bool
	}{{member.Dead, true}, {member.Left, false}} {
		v := cluster(3)
		v.Apply(t0, msg.Update{Member: member.Member{ID: "m01", Addr: "m01", State: tc.state}})
		v.Forget(t0.Add(time.Hour), time.Minute, 24*time.Hour)
		c := cfg
		c.HealEvery = 1
		d := New(c, rand.New(rand.NewPCG(8, 8)), t0.Add(time.Hour))
		now, probed := t0.Add(time.Hour), false
		for range 10 {
			for _, s := range pings(d.Tick(now, v, 1)) {
				probed = probed || s.To.ID == "m01"
			}
			now = now.Add(time.Second)
		}
		if probed != tc.want {
			t.Errorf("forgotten %v member: probed %v, want %v", tc.state, probed, tc.want)
		}
	}
}

// The round-trip time is smoothed: each new sample counts an eighth.
func TestRTTIsSmoothed(t *testing.T) {
	d := New(cfg, rand.New(rand.NewPCG(1, 1)), t0)
	d.sample("x", 80*time.Millisecond)
	d.sample("x", 160*time.Millisecond)
	if got := d.RTT("x"); got != 90*time.Millisecond {
		t.Fatalf("RTT = %v, want (7 × 80ms + 160ms) / 8 = 90ms", got)
	}
}

// A relay lasts the helper's period stretched by its health: an ack that
// comes late in a stretched period is still forwarded.
func TestARelayLastsTheStretchedPeriod(t *testing.T) {
	v := helperView(0)
	d := New(cfg, rand.New(rand.NewPCG(6, 6)), t0.Add(time.Hour))
	o := d.PingReq(t0, v, member.Member{ID: "asker", Addr: "asker"}, msg.PingReq{Seq: 9, Target: "t"}, 2)
	d.Tick(t0.Add(time.Second), v, 2)
	if o := d.Ack(t0.Add(time.Second), "t", o.Sends[0].Body.(msg.Ping).Seq); len(o.Sends) != 1 {
		t.Fatal("an ack within the stretched period wasn't forwarded")
	}
}

// The nack goes out within the asker's period: by two fifths of a period
// when the timeout is longer than that.
func TestTheNackComesWithinThePeriod(t *testing.T) {
	c := cfg
	c.Timeout = 600 * time.Millisecond
	v := helperView(member.Of(member.CapNack))
	d := New(c, rand.New(rand.NewPCG(6, 6)), t0.Add(time.Hour))
	d.PingReq(t0, v, member.Member{ID: "asker", Addr: "asker", Caps: member.Of(member.CapNack)}, msg.PingReq{Seq: 9, Target: "t"}, 1)
	if due := d.Due(); !due.Equal(t0.Add(400 * time.Millisecond)) {
		t.Fatalf("nack due %v after the ping-req, want 400ms", due.Sub(t0))
	}
}
