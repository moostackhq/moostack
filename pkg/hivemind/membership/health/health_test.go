package health

import "testing"

func TestScoreSaturates(t *testing.T) {
	s := New(3, RuleNoNack)
	s.ProbeSucceeded()
	if s.Value() != 0 {
		t.Fatalf("went below 0: %d", s.Value())
	}
	for range 5 {
		s.Refuted()
	}
	if s.Value() != 2 || s.Scale() != 3 {
		t.Fatalf("Value %d, Scale %d; want 2, 3", s.Value(), s.Scale())
	}
}

func TestOff(t *testing.T) {
	s := New(1, RuleLifeguard)
	s.ProbeFailed(3, 0)
	if s.Scale() != 1 {
		t.Fatalf("Scale = %d with health awareness off", s.Scale())
	}
}

func TestRules(t *testing.T) {
	for _, tc := range []struct {
		rule         Rule
		asked, nacks int
		want         int
	}{
		{RuleNoNack, 3, 0, 1},
		{RuleNoNack, 3, 1, 0}, // a helper answered: the prober is reachable
		{RuleNoNack, 0, 0, 1},
		{RuleLifeguard, 3, 0, 4},
		{RuleLifeguard, 3, 2, 2},
		{RuleLifeguard, 3, 3, 1},
		{RuleLifeguard, 3, 5, 1}, // more nacks than nackers counts no more
		{RuleNoNack, 0, 2, 1},
	} {
		s := New(8, tc.rule)
		s.ProbeFailed(tc.asked, tc.nacks)
		if s.Value() != tc.want {
			t.Errorf("rule %d, asked %d, nacks %d: score %d, want %d", tc.rule, tc.asked, tc.nacks, s.Value(), tc.want)
		}
	}
}
