package simulator

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"event-sim/internal/config"
	"event-sim/internal/generator"
	"event-sim/internal/model"
	"event-sim/internal/sink"
	"event-sim/internal/statemachine"
)

// deterministicUUID generates a UUID-like string from a seeded source
func deterministicUUID(rng *rand.Rand) string {
	b := make([]byte, 16)
	rng.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Test 5+6 — Full pipeline deterministic replay (bypasses ticker, uses fixed event count)
func produceEvents(t *testing.T, seed int64, sinkPath string, count int) {
	t.Helper()

	rng := rand.New(rand.NewSource(seed))
	fixedTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

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

	sm, err := statemachine.New(rng, transitions)
	require.NoError(t, err)

	cfg := &config.Config{
		Products: []config.Product{
			{ID: "prod-001", Name: "Headphones", Category: "electronics", Price: 79.99},
			{ID: "prod-002", Name: "Shoes", Category: "sports", Price: 129.99},
			{ID: "prod-003", Name: "Coffee Maker", Category: "home", Price: 49.99},
		},
		SearchQueries: []string{"headphones", "shoes", "coffee"},
	}

	gen := generator.New(rng, cfg)

	fs, err := sink.NewFileSink(sinkPath)
	require.NoError(t, err)
	defer fs.Close()

	sessionID := deterministicUUID(rng)
	userID := deterministicUUID(rng)
	state := model.StateLanding

	for i := 0; i < count; i++ {
		evt := model.Event{
			EventID:   deterministicUUID(rng),
			Timestamp: fixedTime,
			SessionID: sessionID,
			UserID:    userID,
			EventType: state,
			Data:      gen.GenerateData(state),
		}

		require.NoError(t, fs.Write(evt))

		if model.TerminalStates[state] {
			sessionID = deterministicUUID(rng)
			userID = deterministicUUID(rng)
			state = model.StateLanding
			continue
		}

		next, ended := sm.Next(state)
		if ended {
			sessionID = deterministicUUID(rng)
			userID = deterministicUUID(rng)
			state = model.StateLanding
			continue
		}
		state = next
	}
}

func hashFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func TestFullPipelineDeterministicReplay(t *testing.T) {
	dir := t.TempDir()

	// Run 1 — seed 42
	p1 := filepath.Join(dir, "run1.ndjson")
	produceEvents(t, 42, p1, 500)
	h1 := hashFile(t, p1)

	// Run 2 — same seed 42
	p2 := filepath.Join(dir, "run2.ndjson")
	produceEvents(t, 42, p2, 500)
	h2 := hashFile(t, p2)

	assert.Equal(t, h1, h2, "same seed must produce identical output")
}

func TestDifferentSeedsProduceDifferentOutput(t *testing.T) {
	dir := t.TempDir()

	p1 := filepath.Join(dir, "seed42.ndjson")
	produceEvents(t, 42, p1, 500)
	h1 := hashFile(t, p1)

	p2 := filepath.Join(dir, "seed43.ndjson")
	produceEvents(t, 43, p2, 500)
	h2 := hashFile(t, p2)

	assert.NotEqual(t, h1, h2, "different seeds must produce different output")
}

func TestFileSinkLineCount(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out.ndjson")
	produceEvents(t, 42, p, 10000)

	data, err := os.ReadFile(p)
	require.NoError(t, err)

	lines := 0
	for _, b := range data {
		if b == '\n' {
			lines++
		}
	}
	assert.Equal(t, 10000, lines, "file sink should have exactly 10,000 lines")
}

func TestFileSinkAllValidJSON(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out.ndjson")
	produceEvents(t, 99, p, 1000)

	data, err := os.ReadFile(p)
	require.NoError(t, err)

	lineNum := 0
	start := 0
	for i, b := range data {
		if b == '\n' {
			lineNum++
			line := data[start:i]
			require.True(t, len(line) > 2, "line %d is too short", lineNum)
			require.True(t, line[0] == '{', "line %d is not JSON object", lineNum)
			start = i + 1
		}
	}
}

func TestFullPipelineStateMachineValidity(t *testing.T) {
	allowed := map[model.State]map[model.State]bool{
		model.StateLanding:     {model.StateSearch: true},
		model.StateSearch:      {model.StateProductView: true},
		model.StateProductView: {model.StateAddToCart: true, model.StateSearch: true},
		model.StateAddToCart:   {model.StateCheckout: true, model.StateProductView: true},
		model.StateCheckout:    {model.StatePurchase: true, model.StateAddToCart: true},
	}

	dir := t.TempDir()
	p := filepath.Join(dir, "out.ndjson")
	produceEvents(t, 42, p, 10000)

	data, err := os.ReadFile(p)
	require.NoError(t, err)

	var prev model.State
	lineNum := 0
	start := 0
	for i, b := range data {
		if b == '\n' {
			lineNum++
			line := data[start:i]
			start = i + 1

			// Parse event_type from JSON
			var evt struct {
				EventType model.State `json:"event_type"`
			}
			// quick scan for event_type (avoids full json.Unmarshal per line in test)
			// Use json.Unmarshal for correctness
			if err := json.Unmarshal(line, &evt); err != nil {
				t.Fatalf("line %d: invalid JSON: %v", lineNum, err)
			}

			if prev == "" {
				prev = evt.EventType
				continue
			}

			// Terminal state → next event starts new session
			if model.TerminalStates[prev] {
				prev = evt.EventType
				continue
			}

			// Drop-off: previous session ended, new session starts at landing
			if evt.EventType == model.StateLanding && prev != model.StatePurchase {
				prev = evt.EventType
				continue
			}

			if allowed[prev] != nil && !allowed[prev][evt.EventType] {
				t.Errorf("line %d: invalid transition %s -> %s", lineNum, prev, evt.EventType)
			}

			prev = evt.EventType
		}
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
	sm, _ := statemachine.New(rng, transitions)

	cfg := &config.Config{
		Products: []config.Product{
			{ID: "prod-001", Name: "Headphones", Category: "electronics", Price: 79.99},
			{ID: "prod-002", Name: "Shoes", Category: "sports", Price: 129.99},
			{ID: "prod-003", Name: "Coffee Maker", Category: "home", Price: 49.99},
		},
		SearchQueries: []string{"headphones", "shoes", "coffee"},
	}
	gen := generator.New(rng, cfg)

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
			Data:      gen.GenerateData(state),
		}
		fs.Write(evt)

		if model.TerminalStates[state] {
			sessionID = deterministicUUID(rng)
			userID = deterministicUUID(rng)
			state = model.StateLanding
			continue
		}
		next, ended := sm.Next(state)
		if ended {
			sessionID = deterministicUUID(rng)
			userID = deterministicUUID(rng)
			state = model.StateLanding
			continue
		}
		state = next
	}
}


