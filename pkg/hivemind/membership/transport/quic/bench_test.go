package quic_test

import (
	"flag"
	"fmt"
	goruntime "runtime"
	"syscall"
	"testing"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/node"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/runtime"
	"github.com/moostackhq/moostack/pkg/hivemind/membership/transport"
	. "github.com/moostackhq/moostack/pkg/hivemind/membership/transport/quic"
)

var measure = flag.Duration("measure", time.Minute, "how long BenchmarkMembership measures a converged cluster")

// namedSeeds sets the Peer of a destination that has none to the member
// named at its address.
type namedSeeds struct {
	*Transport
	names map[string]string
}

func (s namedSeeds) SendPacket(to transport.Dest, b []byte) { s.Transport.SendPacket(s.pin(to), b) }
func (s namedSeeds) SendStream(to transport.Dest, b []byte) { s.Transport.SendStream(s.pin(to), b) }

func (s namedSeeds) pin(to transport.Dest) transport.Dest {
	if to.Peer == "" {
		to.Peer = s.names[to.Addr]
	}
	return to
}

type benchCluster struct {
	ids   []ident
	nodes []*runtime.Runtime
	trs   []*Transport
}

// startCluster starts n membership runtimes over transports on loopback,
// each joining the first, with the options opts gives the ith.
func startCluster(b testing.TB, n int, opts func(i int) runtime.Options) *benchCluster {
	b.Helper()
	ca := newCluster(b)
	c := &benchCluster{}
	seed := map[string]string{}
	for i := range n {
		id := ca.Node(fmt.Sprintf("node-%03d", i))
		tr, err := Listen(id.Config("127.0.0.1:0"))
		if err != nil {
			b.Fatal(err)
		}
		if i == 0 {
			seed[tr.Addr()] = id.Name
		}
		var o runtime.Options
		if opts != nil {
			o = opts(i)
		}
		rt, err := runtime.Start(b.Context(), node.Defaults(member.ID(id.Name), ""), namedSeeds{tr, seed}, o)
		if err != nil {
			b.Fatal(err)
		}
		if i > 0 {
			if err := rt.Join(c.trs[0].Addr()); err != nil {
				b.Fatal(err)
			}
		}
		c.ids = append(c.ids, id)
		c.nodes = append(c.nodes, rt)
		c.trs = append(c.trs, tr)
	}
	b.Cleanup(func() {
		for _, rt := range c.nodes {
			rt.Close()
		}
		for _, tr := range c.trs {
			tr.Close()
		}
	})
	return c
}

func alive(rt *runtime.Runtime) int {
	n := 0
	for _, m := range rt.Members() {
		if m.State == member.Alive {
			n++
		}
	}
	return n
}

func (c *benchCluster) converged(want int) bool {
	for _, rt := range c.nodes {
		if alive(rt) != want {
			return false
		}
	}
	return true
}

type sample struct {
	at                time.Time
	handshakes, fails uint64
	conns             int
	cpu               time.Duration
}

func (c *benchCluster) sample() sample {
	s := sample{at: time.Now(), cpu: cpu()}
	for _, tr := range c.trs {
		st := tr.Stats()
		s.handshakes += st.Dials + st.Accepts
		s.fails += st.DialFails
		s.conns += st.Conns
	}
	return s
}

func cpu() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// BenchmarkMembership runs real membership runtimes with node.Defaults
// over the transport on loopback, measures a converged cluster for
// -measure, then crashes a node. It reports per node: connections open,
// handshakes a second, the share of a core and the heap; and how long
// the crash took to be known everywhere.
func BenchmarkMembership(b *testing.B) {
	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			for range b.N {
				benchMembership(b, n)
			}
		})
	}
}

func benchMembership(b *testing.B, n int) {
	var before goruntime.MemStats
	goruntime.GC()
	goruntime.ReadMemStats(&before)
	began := time.Now()
	c := startCluster(b, n, nil)
	for !c.converged(n) {
		if time.Since(began) > 2*time.Minute {
			b.Fatal("not converged after 2 minutes")
		}
		time.Sleep(100 * time.Millisecond)
	}
	first := c.sample()
	time.Sleep(*measure)
	last := c.sample()
	secs := last.at.Sub(first.at).Seconds()
	var after goruntime.MemStats
	goruntime.GC()
	goruntime.ReadMemStats(&after)
	b.ReportMetric(float64(last.conns)/float64(n), "conns/node")
	b.ReportMetric(float64(last.handshakes-first.handshakes)/secs/float64(n), "handshakes/s/node")
	b.ReportMetric(100*(last.cpu-first.cpu).Seconds()/secs/float64(n), "%core/node")
	b.ReportMetric(float64(after.HeapInuse-min(after.HeapInuse, before.HeapInuse))/float64(n)/1e6, "MBheap/node")
	if f := last.fails - first.fails; f > 0 {
		b.Errorf("%d dials failed in a converged cluster", f)
	}

	// A crash: the node stops without leaving, and its transport says nothing.
	crashed := n - 1
	c.nodes[crashed].Close()
	c.trs[crashed].Crash()
	at := time.Now()
	gone := member.ID(fmt.Sprintf("node-%03d", crashed))
	for !c.knownGone(gone, crashed) {
		if time.Since(at) > 2*time.Minute {
			b.Fatal("the crash was not known everywhere within 2 minutes")
		}
		time.Sleep(100 * time.Millisecond)
	}
	b.ReportMetric(time.Since(at).Seconds(), "s-crash-known")
	for i, rt := range c.nodes[:crashed] {
		if a := alive(rt); a != n-1 {
			b.Errorf("node %d has %d alive, want %d: a false death", i, a, n-1)
		}
	}
}

// knownGone says whether each of the first upto nodes knows id is gone.
func (c *benchCluster) knownGone(id member.ID, upto int) bool {
	for _, rt := range c.nodes[:upto] {
		for _, m := range rt.Members() {
			if m.ID == id && !m.State.Gone() {
				return false
			}
		}
	}
	return true
}
