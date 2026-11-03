package probe

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/msg"
)

// The detector over random ticks, acks, nacks, ping-reqs and pauses keeps
// its invariants: at most one probe in flight, nacks never more than its
// nackers, Due never before the last call, and every live member probed
// within the bound a round gives.
func TestDetectorInvariants(t *testing.T) {
	r := rand.New(rand.NewPCG(15, 16))
	for run := range 100 {
		n := 2 + r.IntN(8)
		v := cluster(n)
		d := New(cfg, rand.New(rand.NewPCG(uint64(run), 1)), t0)
		now := t0
		lastProbed := map[member.ID]time.Time{}
		for id := range n {
			lastProbed[member.ID(fmt.Sprintf("m%02d", id))] = t0
		}
		var seqs []uint32
		var shifted time.Duration
		for step := range 400 {
			var o Out
			switch r.IntN(6) {
			case 0, 1:
				now = d.Due()
				o = d.Tick(now, v, 1+r.IntN(2))
			case 2:
				if len(seqs) > 0 {
					o = d.Ack(now, member.ID(fmt.Sprintf("m%02d", r.IntN(n))), seqs[r.IntN(len(seqs))])
				}
			case 3:
				if len(seqs) > 0 {
					d.Nack(member.ID(fmt.Sprintf("m%02d", r.IntN(n))), seqs[r.IntN(len(seqs))])
				}
			case 4:
				o = d.PingReq(now, v, member.Member{ID: member.ID(fmt.Sprintf("m%02d", r.IntN(n)))}, msg.PingReq{Seq: r.Uint32(), Target: member.ID(fmt.Sprintf("m%02d", r.IntN(n)))}, 1)
			case 5:
				by := time.Duration(r.IntN(500)) * time.Millisecond
				d.Shift(by)
				shifted += by
			}
			for _, s := range o.Sends {
				if p, ok := s.Body.(msg.Ping); ok {
					seqs = append(seqs, p.Seq)
					lastProbed[s.To.ID] = now
				}
			}
			for _, f := range o.Failed {
				if f.Nacks > f.Nackers || f.Nackers > f.Asked {
					t.Fatalf("run %d step %d: %+v", run, step, f)
				}
			}
			if due := d.Due(); due.Before(now) {
				t.Fatalf("run %d step %d: Due %v before now %v", run, step, due, now)
			}
			for id, at := range lastProbed {
				// Two rounds of n probes at the longest stretched period, plus
				// what the pauses shifted.
				if now.Sub(at) > time.Duration(2*n+2)*2*cfg.Interval+shifted {
					t.Fatalf("run %d step %d: %s unprobed since %v", run, step, id, now.Sub(at))
				}
			}
		}
	}
}
