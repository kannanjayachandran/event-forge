package statemachine

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"event-sim/internal/model"
)

var defaultTransitions = []model.TransitionRule{
	{From: model.StateLanding, To: model.StateSearch, Probability: 0.8},
	{From: model.StateSearch, To: model.StateProductView, Probability: 0.6},
	{From: model.StateProductView, To: model.StateAddToCart, Probability: 0.3},
	{From: model.StateProductView, To: model.StateSearch, Probability: 0.3},
	{From: model.StateAddToCart, To: model.StateCheckout, Probability: 0.5},
	{From: model.StateAddToCart, To: model.StateProductView, Probability: 0.3},
	{From: model.StateCheckout, To: model.StatePurchase, Probability: 0.7},
	{From: model.StateCheckout, To: model.StateAddToCart, Probability: 0.2},
}

func TestNewStateMachine(t *testing.T) {
	transitions := []model.TransitionRule{
		{From: model.StateLanding, To: model.StateSearch, Probability: 0.8},
		{From: model.StateSearch, To: model.StateProductView, Probability: 0.6},
	}

	rng := rand.New(rand.NewSource(42))
	sm, err := New(rng, transitions)
	require.NoError(t, err)
	require.NotNil(t, sm)

	_, ended := sm.Next(model.StatePurchase)
	assert.True(t, ended)
}

func TestStateMachineExceedsProbability(t *testing.T) {
	transitions := []model.TransitionRule{
		{From: model.StateLanding, To: model.StateSearch, Probability: 0.9},
		{From: model.StateLanding, To: model.StateProductView, Probability: 0.2},
	}

	rng := rand.New(rand.NewSource(42))
	_, err := New(rng, transitions)
	assert.Error(t, err)
}

func TestNoTransitionsEnds(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	sm, err := New(rng, nil)
	require.NoError(t, err)

	_, ended := sm.Next(model.StateLanding)
	assert.True(t, ended)
}

func TestEmptyTransitionsForStateEnds(t *testing.T) {
	transitions := []model.TransitionRule{
		{From: model.StateSearch, To: model.StateProductView, Probability: 1.0},
	}

	rng := rand.New(rand.NewSource(42))
	sm, err := New(rng, transitions)
	require.NoError(t, err)

	_, ended := sm.Next(model.StateLanding)
	assert.True(t, ended)
}

// Test 3 — State Machine Validity: no invalid transitions in 10,000 events
func TestStateMachineValidity(t *testing.T) {
	allowed := map[model.State]map[model.State]bool{
		model.StateLanding:     {model.StateSearch: true},
		model.StateSearch:      {model.StateProductView: true},
		model.StateProductView: {model.StateAddToCart: true, model.StateSearch: true},
		model.StateAddToCart:   {model.StateCheckout: true, model.StateProductView: true},
		model.StateCheckout:    {model.StatePurchase: true, model.StateAddToCart: true},
	}

	rng := rand.New(rand.NewSource(42))
	sm, err := New(rng, defaultTransitions)
	require.NoError(t, err)

	state := model.StateLanding
	for i := 0; i < 10000; i++ {
		next, ended := sm.Next(state)

		if ended || next == model.StatePurchase {
			if ended && state != model.StatePurchase {
				state = model.StateLanding
				continue
			}
			state = model.StateLanding
			continue
		}

		allowedNext, ok := allowed[state]
		require.True(t, ok, "state %s has no allowed transitions defined", state)
		assert.True(t, allowedNext[next],
			"invalid transition: %s -> %s", state, next)

		state = next
	}
}

// Test 4 — Drop-off Probability: verify ±2% of configured probability
func TestDropOffProbability(t *testing.T) {
	t.Run("landing drop-off 20%", func(t *testing.T) {
		transitions := []model.TransitionRule{
			{From: model.StateLanding, To: model.StateSearch, Probability: 0.8},
		}

		rng := rand.New(rand.NewSource(42))
		sm, err := New(rng, transitions)
		require.NoError(t, err)

		sessions := 100000
		dropOffs := 0
		for i := 0; i < sessions; i++ {
			_, ended := sm.Next(model.StateLanding)
			if ended {
				dropOffs++
			}
		}

		got := float64(dropOffs) / float64(sessions)
		assert.InDelta(t, 0.20, got, 0.02,
			"expected ~20%% drop-off, got %.2f%%", got*100)
	})

	t.Run("search drop-off 40%", func(t *testing.T) {
		transitions := []model.TransitionRule{
			{From: model.StateSearch, To: model.StateProductView, Probability: 0.6},
		}

		rng := rand.New(rand.NewSource(99))
		sm, err := New(rng, transitions)
		require.NoError(t, err)

		sessions := 100000
		dropOffs := 0
		for i := 0; i < sessions; i++ {
			_, ended := sm.Next(model.StateSearch)
			if ended {
				dropOffs++
			}
		}

		got := float64(dropOffs) / float64(sessions)
		assert.InDelta(t, 0.40, got, 0.02,
			"expected ~40%% drop-off, got %.2f%%", got*100)
	})

	t.Run("checkout to purchase 70%", func(t *testing.T) {
		transitions := []model.TransitionRule{
			{From: model.StateCheckout, To: model.StatePurchase, Probability: 0.7},
			{From: model.StateCheckout, To: model.StateAddToCart, Probability: 0.2},
		}

		rng := rand.New(rand.NewSource(7))
		sm, err := New(rng, transitions)
		require.NoError(t, err)

		total := 100000
		purchases := 0
		for i := 0; i < total; i++ {
			next, ended := sm.Next(model.StateCheckout)
			if next == model.StatePurchase {
				purchases++
			}
			_ = ended
		}

		got := float64(purchases) / float64(total)
		assert.InDelta(t, 0.70, got, 0.02,
			"expected ~70%% purchase rate, got %.2f%%", got*100)
	})
}

// Test 5 — Deterministic Replay: same seed → identical sequence
func TestDeterministicReplay(t *testing.T) {
	rng1 := rand.New(rand.NewSource(12345))
	sm1, _ := New(rng1, defaultTransitions)

	state := model.StateLanding
	var states1 []model.State
	for i := 0; i < 200; i++ {
		states1 = append(states1, state)
		next, ended := sm1.Next(state)
		if ended || next == model.StatePurchase {
			state = model.StateLanding
		} else {
			state = next
		}
	}

	rng2 := rand.New(rand.NewSource(12345))
	sm2, _ := New(rng2, defaultTransitions)

	state = model.StateLanding
	var states2 []model.State
	for i := 0; i < 200; i++ {
		states2 = append(states2, state)
		next, ended := sm2.Next(state)
		if ended || next == model.StatePurchase {
			state = model.StateLanding
		} else {
			state = next
		}
	}

	assert.Equal(t, states1, states2)
}

// Test 6 — Different Seeds → Different Data
func TestDifferentSeedsProduceDifferentData(t *testing.T) {
	rng1 := rand.New(rand.NewSource(42))
	sm1, _ := New(rng1, defaultTransitions)

	rng2 := rand.New(rand.NewSource(43))
	sm2, _ := New(rng2, defaultTransitions)

	var seq1, seq2 []model.State
	state := model.StateLanding
	for i := 0; i < 200; i++ {
		seq1 = append(seq1, state)
		next, ended := sm1.Next(state)
		if ended || next == model.StatePurchase {
			state = model.StateLanding
		} else {
			state = next
		}
	}

	state = model.StateLanding
	for i := 0; i < 200; i++ {
		seq2 = append(seq2, state)
		next, ended := sm2.Next(state)
		if ended || next == model.StatePurchase {
			state = model.StateLanding
		} else {
			state = next
		}
	}

	assert.NotEqual(t, seq1, seq2, "different seeds should produce different sequences")
}
