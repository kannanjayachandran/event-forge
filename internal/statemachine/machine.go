package statemachine

import (
	"fmt"
	"math/rand"

	"event-sim/internal/model"
)

type weightedRule struct {
	To          model.State
	Probability float64
	Threshold   float64
}

type StateMachine struct {
	rng   *rand.Rand
	rules map[model.State][]weightedRule
}

func New(rng *rand.Rand, transitions []model.TransitionRule) (*StateMachine, error) {
	rules := make(map[model.State][]weightedRule)

	for _, t := range transitions {
		rules[t.From] = append(rules[t.From], weightedRule{
			To:          t.To,
			Probability: t.Probability,
		})
	}

	for state, rs := range rules {
		var cum float64
		for i, r := range rs {
			cum += r.Probability
			rules[state][i].Threshold = cum
		}
		if cum > 1.0+1e-9 {
			return nil, fmt.Errorf("total probability for state %s exceeds 1.0: %.2f", state, cum)
		}
	}

	return &StateMachine{rng: rng, rules: rules}, nil
}

func (sm *StateMachine) Next(current model.State) (next model.State, ended bool) {
	if model.TerminalStates[current] {
		return current, true
	}

	rs, ok := sm.rules[current]
	if !ok || len(rs) == 0 {
		return current, true
	}

	p := sm.rng.Float64()
	for _, r := range rs {
		if p <= r.Threshold {
			return r.To, false
		}
	}

	return current, true
}
