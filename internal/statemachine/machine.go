package statemachine

import (
	"fmt"
	"math/rand"

	"event-sim/internal/model"
)

// weightedRule is a precomputed transition with a cumulative probability
// threshold.
type weightedRule struct {
	To        model.State
	Threshold float64 // cumulative, not individual
}

// StateMachine evaluates state transitions via weighted random selection.
type StateMachine struct {
	rules map[model.State][]weightedRule
}

// Validates and precomputes cumulative thresholds from the given
// transition rules.
// Returns an error if any state's probabilities exceed 1.0.
func New(transitions []model.TransitionRule) (*StateMachine, error) {
	rules := make(map[model.State][]weightedRule, len(transitions))

	for _, t := range transitions {
		rules[t.From] = append(rules[t.From], weightedRule{
			To:        t.To,
			Threshold: t.Probability,
		})
	}

	for state, rs := range rules {
		var cum float64
		for i, r := range rs {
			cum += r.Threshold
			rules[state][i].Threshold = cum
		}
		if cum > 1.0+1e-9 {
			return nil, fmt.Errorf(
				"state %q: transition probabilities sum to %.6f, must be <= 1.0",
				state, cum,
			)
		}
	}

	return &StateMachine{rules: rules}, nil
}

// Next draws a random transition from current using rng.
// Returns ended=true when current is terminal, has no outgoing transitions,
// or the random draw falls in the implicit drop-off region (sum of
// probabilities < 1.0 and draw exceeds the last threshold).
func (sm *StateMachine) Next(rng *rand.Rand, current model.State) (next model.State, ended bool) {
	if model.TerminalStates[current] {
		return current, true
	}

	rs, ok := sm.rules[current]
	if !ok || len(rs) == 0 {
		return current, true
	}

	p := rng.Float64()
	for _, r := range rs {
		if p <= r.Threshold {
			return r.To, false
		}
	}

	//Draw exceeded sum of all probabilities — implicit session drop-off.
	// This is intentional when transitions intentionally sum to < 1.0.
	return current, true
}
