package runtime

import (
	"fmt"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/node"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/wire"
)

func testNode(t *testing.T) (*Runtime, *node.Node) {
	t.Helper()
	codec, _ := wire.NewCodec(1, 1)
	c := node.Defaults("a", "a")
	return &Runtime{codec: codec, hooks: newQueue()}, node.New(c, codec, time.Now())
}

// A wake-up in the past sets the timer a millisecond out, not to fire at
// once over and over.
func TestAWakeInThePastDoesntSpin(t *testing.T) {
	r, _ := testNode(t)
	timer := time.NewTimer(time.Hour)
	r.reset(timer, time.Now().Add(-time.Hour))
	select {
	case <-timer.C:
		t.Fatal("fired at once")
	default:
	}
	select {
	case <-timer.C:
	case <-time.After(time.Second):
		t.Fatal("never fired")
	}
}

func TestWaitingMessagesGoBeforeTheTimer(t *testing.T) {
	r, n := testNode(t)
	ch := make(chan transport.Message, 8)
	for i := range 3 {
		m := member.Member{ID: member.ID(fmt.Sprint("m", i)), Addr: fmt.Sprint(i), MinVersion: 1, Version: 1}
		b, _ := wire.EncodePacket(msg.Packet{From: m, Body: msg.Ack{}})
		ch <- transport.Message{From: m.Addr, Peer: string(m.ID), Data: b}
	}
	r.tr = nopTransport{}
	r.drain(n, ch)
	if len(n.Members()) != 4 {
		t.Fatalf("%d members after draining 3 senders' messages", len(n.Members()))
	}
}

func TestTheViewIsCopiedOnlyOnAChange(t *testing.T) {
	r, n := testNode(t)
	r.tr = nopTransport{}
	r.snapshot(n)
	before := &r.members[0]
	r.carryOut(n, node.Result{})
	if &r.members[0] != before {
		t.Fatal("a step that changed nothing copied the view")
	}
	r.carryOut(n, node.Result{Changes: []member.Member{{ID: "x"}}})
	if &r.members[0] == before {
		t.Fatal("a change didn't copy the view")
	}
}

type nopTransport struct{}

func (nopTransport) Addr() string                      { return "a" }
func (nopTransport) SendPacket(transport.Dest, []byte) {}
func (nopTransport) SendStream(transport.Dest, []byte) {}
func (nopTransport) Receive() <-chan transport.Message { return nil }
func (nopTransport) Close() error                      { return nil }

// A message that doesn't decode is dropped and counted by why: of a kind
// this version doesn't know, or malformed.
func TestDroppedMessagesAreCounted(t *testing.T) {
	r, _ := testNode(t)
	for _, data := range [][]byte{{1, 99}, {1, 99}, {}} {
		if in := r.input(transport.Message{From: "x", Peer: "x", Data: data}); in != nil {
			t.Fatalf("input(%v) = %#v, want it dropped", data, in)
		}
	}
	if got := r.Counts(); got != (Counts{Unknown: 2, Malformed: 1}) {
		t.Fatalf("Counts = %+v", got)
	}
}

// A message with no proven peer is dropped and counted, unless the runtime
// was told its transport proves nobody.
func TestAMessageWithNoProvenPeerIsDroppedByDefault(t *testing.T) {
	r, _ := testNode(t)
	ack, _ := wire.EncodePacket(msg.Packet{From: member.Member{ID: "x"}, Body: msg.Ack{}})
	if in := r.input(transport.Message{From: "x", Data: ack}); in != nil {
		t.Fatalf("input = %#v, want it dropped", in)
	}
	if got := r.Counts(); got != (Counts{Unauthenticated: 1}) {
		t.Fatalf("Counts = %+v", got)
	}
	r.opts.Unauthenticated = true
	if in := r.input(transport.Message{From: "x", Data: ack}); in == nil {
		t.Fatal("dropped with Unauthenticated set")
	}
}

// A packet or sync that doesn't encode is not sent, and counted.
func TestUnencodableMessagesAreCounted(t *testing.T) {
	r, n := testNode(t)
	r.tr = nopTransport{}
	r.carryOut(n, node.Result{
		Packets: []node.Packet{{Addr: "b", Version: 9, Packet: msg.Packet{Body: msg.Ack{}}}},
		Syncs:   []node.Sync{{Addr: "b", Version: 9}, {Addr: "c", Version: 9}},
	})
	if got := r.Counts().Unsent; got != 3 {
		t.Fatalf("Unsent = %d, want 3", got)
	}
}
