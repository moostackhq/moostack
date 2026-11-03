// Package health is Lifeguard's local health score: a node that misses
// what it expects to hear probes more slowly and waits longer for answers.
package health

// Rule decides which failed probes raise the score.
type Rule uint8

const (
	// RuleNoNack raises the score for a failed probe only when no helper of
	// the indirect probe answered at all.
	RuleNoNack Rule = iota
	// RuleLifeguard raises the score by one for a failed probe and by one
	// for each helper that sent no nack.
	RuleLifeguard
)

// Score is a node's local health score, from 0 (healthy) to Max-1.
type Score struct {
	v, max int
	rule   Rule
}

// New returns a healthy score that saturates at max-1. A max of 1 or less
// keeps it at 0, which turns local health awareness off.
func New(max int, rule Rule) *Score {
	return &Score{max: max, rule: rule}
}

func (s *Score) add(d int) {
	s.v = min(max(s.v+d, 0), max(s.max-1, 0))
}

func (s *Score) Value() int { return s.v }

// Scale is the factor that stretches the probe interval and timeout:
// score + 1.
func (s *Score) Scale() int { return s.v + 1 }

// ProbeSucceeded lowers the score: the node got an ack in time.
func (s *Score) ProbeSucceeded() { s.add(-1) }

// ProbeFailed records a probe that got no ack, after asking helpers of
// which nackers can send a nack and nacks did. Helpers that can't nack
// count as no missed nack.
func (s *Score) ProbeFailed(nackers, nacks int) {
	nacks = min(nacks, nackers)
	switch s.rule {
	case RuleNoNack:
		if nacks == 0 {
			s.add(1)
		}
	case RuleLifeguard:
		s.add(1 + nackers - nacks)
	}
}

// Refuted raises the score: the node refuted a suspicion of itself.
func (s *Score) Refuted() { s.add(1) }
