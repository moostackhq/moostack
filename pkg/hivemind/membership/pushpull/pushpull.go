// Package pushpull schedules the whole-view syncs, in which two members
// swap every entry they have. A node joins by syncing with its seeds, and
// retries until it hears of another member.
package pushpull

import (
	"math/rand/v2"
	"slices"
	"time"

	"github.com/moostackhq/moostack/pkg/hivemind/membership/member"
)

// Config is when syncs happen.
type Config struct {
	// Every is the time between periodic syncs with a random live member;
	// every SeedEvery-th goes to a random seed instead. 0 turns seed syncs
	// off.
	Every     time.Duration
	SeedEvery int
	// RetryEvery is the time between join attempts while the node knows
	// no other live member.
	RetryEvery time.Duration
}

// Scheduler decides when to sync, and with whom.
type Scheduler struct {
	cfg     Config
	rng     *rand.Rand
	seeds   []string
	next    time.Time // the next periodic sync
	retryAt time.Time // the next join attempt; zero when not joining
	backoff time.Duration
	count   int
	catchUp int // syncs soon still allowed after a join
}

// maxCatchUp is how many syncs past the first a joined node brings
// forward, one for each sync that taught it of members it didn't know.
const maxCatchUp = 3

// maxBackoff is how many times RetryEvery join attempts back off to while
// the seeds don't answer.
const maxBackoff = 8

// New returns a scheduler whose first periodic sync is at a random time
// within Every after now.
func New(cfg Config, rng *rand.Rand, now time.Time) *Scheduler {
	first := cfg.Every
	if cfg.Every > 0 {
		first = time.Duration(rng.Int64N(int64(cfg.Every))) + 1
	}
	return &Scheduler{cfg: cfg, rng: rng, next: now.Add(first)}
}

// Join keeps the seeds, other than self, and returns the addresses to sync
// with now. With none, there is nothing to join: a join still trying
// earlier seeds ends.
func (s *Scheduler) Join(now time.Time, self string, seeds []string) []string {
	s.seeds = slices.DeleteFunc(slices.Clone(seeds), func(a string) bool { return a == self })
	if len(s.seeds) == 0 {
		s.retryAt, s.backoff, s.catchUp = time.Time{}, 0, 0
		return nil
	}
	s.backoff = s.cfg.RetryEvery
	s.retryAt = now.Add(s.jitter(s.backoff))
	return slices.Clone(s.seeds)
}

// jitter is d give or take half.
func (s *Scheduler) jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	return d/2 + time.Duration(s.rng.Int64N(int64(d)))
}

// Tick returns the addresses to sync with at now. others says whether the
// node knows another live member, and pick draws one at random. Without
// any, a joining node tries its seeds again. Once it knows one, its join
// is done, and it catches up: its next sync comes within RetryEvery or so
// instead of Every, and so does the one after each sync that taught it of
// members it didn't know (see Learned), maxCatchUp times at most.
func (s *Scheduler) Tick(now time.Time, others bool, pick func() (member.Member, bool)) []string {
	var out []string
	if !s.retryAt.IsZero() {
		if others {
			s.retryAt = time.Time{}
			s.catchUp = maxCatchUp
			s.soon(now)
		} else if !now.Before(s.retryAt) {
			s.backoff = min(2*s.backoff, maxBackoff*s.cfg.RetryEvery)
			s.retryAt = now.Add(s.jitter(s.backoff))
			out = append(out, s.seeds...)
		}
	}
	if s.cfg.Every <= 0 || now.Before(s.next) {
		return out
	}
	s.next = now.Add(s.cfg.Every)
	s.count++
	if len(s.seeds) > 0 && s.cfg.SeedEvery > 0 && s.count%s.cfg.SeedEvery == 0 {
		return append(out, s.seeds[s.rng.IntN(len(s.seeds))])
	}
	if m, ok := pick(); ok {
		out = append(out, m.Addr)
	}
	return out
}

// soon brings the next sync forward to within RetryEvery or so. With
// periodic syncs off, next is never read.
func (s *Scheduler) soon(now time.Time) {
	if at := now.Add(s.jitter(s.cfg.RetryEvery)); at.Before(s.next) {
		s.next = at
	}
}

// Learned tells the scheduler that a sync taught the node of members it
// didn't know. Soon after a join, that brings the next sync forward.
func (s *Scheduler) Learned(now time.Time) {
	if s.catchUp > 0 && s.retryAt.IsZero() {
		s.catchUp--
		s.soon(now)
	}
}

// Due returns when Tick next has something to do.
// With periodic syncs off (Every 0) and no join in progress, that is the
// zero time: nothing to do.
func (s *Scheduler) Due() time.Time {
	next := s.next
	if s.cfg.Every <= 0 {
		next = time.Time{}
	}
	if !s.retryAt.IsZero() && (next.IsZero() || s.retryAt.Before(next)) {
		return s.retryAt
	}
	return next
}

// Shift moves the schedule later by d, as after the node was paused.
func (s *Scheduler) Shift(d time.Duration) {
	s.next = s.next.Add(d)
	if !s.retryAt.IsZero() {
		s.retryAt = s.retryAt.Add(d)
	}
}

func (s *Scheduler) IsSeed(addr string) bool { return slices.Contains(s.seeds, addr) }

// Joining reports whether the node is still trying its seeds.
func (s *Scheduler) Joining() bool { return !s.retryAt.IsZero() }
