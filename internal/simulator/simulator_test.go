package simulator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"event-sim/internal/config"
	"event-sim/internal/generator"
	"event-sim/internal/model"
	"event-sim/internal/sink"
	"event-sim/internal/statemachine"
)

func defaultTestConfig() *config.Config {
	return &config.Config{
		Simulator: config.SimulatorConfig{
			Seed:            42,
			ConcurrentUsers: 1,
			EventsPerSecond: 100000,
			Timing: config.TimingConfig{
				Distribution: "exponential",
			},
		},
		Products: []config.Product{
			{ID: "prod-001", Name: "Headphones", Category: "electronics", Price: 79.99},
			{ID: "prod-002", Name: "Shoes", Category: "sports", Price: 129.99},
			{ID: "prod-003", Name: "Coffee Maker", Category: "home", Price: 49.99},
		},
		SearchQueries: []string{"headphones", "shoes", "coffee"},
		StateMachine: config.StateMachineConfig{
			Transitions: []model.TransitionRule{
				{From: model.StateLanding, To: model.StateSearch, Probability: 0.8},
				{From: model.StateSearch, To: model.StateProductView, Probability: 0.6},
				{From: model.StateProductView, To: model.StateAddToCart, Probability: 0.3},
				{From: model.StateProductView, To: model.StateSearch, Probability: 0.3},
				{From: model.StateAddToCart, To: model.StateCheckout, Probability: 0.5},
				{From: model.StateAddToCart, To: model.StateProductView, Probability: 0.3},
				{From: model.StateCheckout, To: model.StatePurchase, Probability: 0.7},
				{From: model.StateCheckout, To: model.StateAddToCart, Probability: 0.2},
			},
		},
	}
}

func runEvents(t *testing.T, cfg *config.Config, n int) []model.Event {
	t.Helper()

	s := &sink.SliceSink{}
	sim, err := New(cfg, nil, []sink.Sink{s}, zap.NewNop())
	require.NoError(t, err)
	sim.minDelay = 0

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sim.Run(ctx)
		close(done)
	}()

	timeout := time.After(30 * time.Second)
	for {
		select {
		case <-timeout:
			cancel()
			<-done
			t.Fatalf("timed out waiting for %d events, got %d", n, s.Count())
		default:
		}
		if s.Count() >= n {
			cancel()
			<-done
			return s.Events[:n]
		}
		time.Sleep(time.Millisecond)
	}
}

func hashEvents(events []model.Event) string {
	h := sha256.New()
	enc := json.NewEncoder(h)
	for _, evt := range events {
		// Marshal only the deterministic fields (exclude Timestamp)
		enc.Encode(struct {
			EventID   string          `json:"event_id"`
			SessionID string          `json:"session_id"`
			UserID    string          `json:"user_id"`
			EventType model.State     `json:"event_type"`
			Data      json.RawMessage `json:"data"`
		}{
			EventID:   evt.EventID,
			SessionID: evt.SessionID,
			UserID:    evt.UserID,
			EventType: evt.EventType,
			Data:      evt.Data,
		})
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func TestFullPipelineDeterministicReplay(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.Simulator.Seed = 42

	events1 := runEvents(t, cfg, 500)
	events2 := runEvents(t, cfg, 500)

	h1 := hashEvents(events1)
	h2 := hashEvents(events2)

	assert.Equal(t, h1, h2, "same seed must produce identical output")
}

func TestDifferentSeedsProduceDifferentOutput(t *testing.T) {
	cfg1 := defaultTestConfig()
	cfg1.Simulator.Seed = 42
	events1 := runEvents(t, cfg1, 500)

	cfg2 := defaultTestConfig()
	cfg2.Simulator.Seed = 43
	events2 := runEvents(t, cfg2, 500)

	h1 := hashEvents(events1)
	h2 := hashEvents(events2)

	assert.NotEqual(t, h1, h2, "different seeds must produce different output")
}

func TestFullPipelineStateMachineValidity(t *testing.T) {
	allowed := map[model.State]map[model.State]bool{
		model.StateLanding:     {model.StateSearch: true},
		model.StateSearch:      {model.StateProductView: true},
		model.StateProductView: {model.StateAddToCart: true, model.StateSearch: true},
		model.StateAddToCart:   {model.StateCheckout: true, model.StateProductView: true},
		model.StateCheckout:    {model.StatePurchase: true, model.StateAddToCart: true},
	}

	cfg := defaultTestConfig()
	events := runEvents(t, cfg, 10000)

	var prev model.State
	for i, evt := range events {
		if prev == "" {
			prev = evt.EventType
			continue
		}

		if model.TerminalStates[prev] {
			prev = evt.EventType
			continue
		}

		if evt.EventType == model.StateLanding && prev != model.StatePurchase {
			prev = evt.EventType
			continue
		}

		if allowed[prev] != nil && !allowed[prev][evt.EventType] {
			t.Errorf("event %d: invalid transition %s -> %s", i, prev, evt.EventType)
		}

		prev = evt.EventType
	}
}

func TestSimulatorContextCancellation(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.Simulator.ConcurrentUsers = 3

	s := &sink.SliceSink{}
	sim, err := New(cfg, nil, []sink.Sink{s}, zap.NewNop())
	require.NoError(t, err)
	sim.minDelay = 0

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sim.Run(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("simulator did not stop within 5s of cancellation")
	}

	produced := s.Count()
	t.Logf("produced %d events before cancellation", produced)
	assert.Greater(t, produced, 0, "expected at least some events before cancellation")
}

func TestSimulatorStopWaitsForGoroutines(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.Simulator.ConcurrentUsers = 5

	s := &sink.SliceSink{}
	sim, err := New(cfg, nil, []sink.Sink{s}, zap.NewNop())
	require.NoError(t, err)
	sim.minDelay = 0

	done := make(chan struct{})
	go func() {
		sim.Run(context.Background())
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	sim.Stop()
	elapsed := time.Since(start)
	<-done

	produced := s.Count()
	t.Logf("produced %d events, Stop() took %v", produced, elapsed)
	assert.Greater(t, produced, 0, "expected at least some events before stop")
}

func TestFullPipelineEventCount(t *testing.T) {
	cfg := defaultTestConfig()
	events := runEvents(t, cfg, 10000)
	assert.Len(t, events, 10000)
}

func TestFullPipelineAllValidEvents(t *testing.T) {
	cfg := defaultTestConfig()
	events := runEvents(t, cfg, 1000)

	for i, evt := range events {
		assert.NotEmpty(t, evt.EventID, "event %d: missing EventID", i)
		assert.NotEmpty(t, evt.SessionID, "event %d: missing SessionID", i)
		assert.NotEmpty(t, evt.UserID, "event %d: missing UserID", i)
		assert.NotEmpty(t, evt.EventType, "event %d: missing EventType", i)
		assert.False(t, evt.Timestamp.IsZero(), "event %d: zero timestamp", i)
		assert.NotEmpty(t, evt.Data, "event %d: missing Data", i)
	}
}

// Test 7 — Throughput benchmark (run: go test -bench=BenchmarkThroughput -benchtime=5s)
func BenchmarkThroughput(b *testing.B) {
	dir := b.TempDir()

	rng := rand.New(rand.NewSource(42))

	transitions := []model.TransitionRule{
		{From: model.StateLanding, To: model.StateSearch, Probability: 0.8},
		{From: model.StateSearch, To: model.StateProductView, Probability: 0.6},
		{From: model.StateProductView, To: model.StateAddToCart, Probability: 0.3},
		{From: model.StateProductView, To: model.StateSearch, Probability: 0.3},
		{From: model.StateAddToCart, To: model.StateCheckout, Probability: 0.5},
		{From: model.StateAddToCart, To: model.StateProductView, Probability: 0.3},
		{From: model.StateCheckout, To: model.StatePurchase, Probability: 0.7},
		{From: model.StateCheckout, To: model.StateAddToCart, Probability: 0.2},
	}
	sm, _ := statemachine.New(transitions)

	cfg := &config.Config{
		Products: []config.Product{
			{ID: "prod-001", Name: "Headphones", Category: "electronics", Price: 79.99},
			{ID: "prod-002", Name: "Shoes", Category: "sports", Price: 129.99},
			{ID: "prod-003", Name: "Coffee Maker", Category: "home", Price: 49.99},
		},
		SearchQueries: []string{"headphones", "shoes", "coffee"},
	}
	gen := generator.New(cfg)

	fs, _ := sink.NewFileSink(filepath.Join(dir, "bench.ndjson"))
	defer fs.Close()

	b.ResetTimer()
	sessionID := deterministicUUID(rng)
	userID := deterministicUUID(rng)
	fixedTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	state := model.StateLanding

	for i := 0; i < b.N; i++ {
		evt := model.Event{
			EventID:   deterministicUUID(rng),
			Timestamp: fixedTime,
			SessionID: sessionID,
			UserID:    userID,
			EventType: state,
			Data:      gen.GenerateData(rng, state),
		}
		fs.Write(evt)

		if model.TerminalStates[state] {
			sessionID = deterministicUUID(rng)
			userID = deterministicUUID(rng)
			state = model.StateLanding
			continue
		}
		next, ended := sm.Next(rng, state)
		if ended {
			sessionID = deterministicUUID(rng)
			userID = deterministicUUID(rng)
			state = model.StateLanding
			continue
		}
		state = next
	}
}


