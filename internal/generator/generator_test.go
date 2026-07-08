package generator

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"event-sim/internal/config"
	"event-sim/internal/model"
)

func testConfig() *config.Config {
	return &config.Config{
		Products: []config.Product{
			{ID: "prod-001", Name: "Headphones", Category: "electronics", Price: 79.99},
			{ID: "prod-002", Name: "Shoes", Category: "sports", Price: 129.99},
			{ID: "prod-003", Name: "Coffee Maker", Category: "home", Price: 49.99},
		},
		SearchQueries: []string{"headphones", "shoes", "coffee", "laptop"},
	}
}

func TestGeneratorDeterministic(t *testing.T) {
	cfg := testConfig()

	rng1 := rand.New(rand.NewSource(42))
	gen1 := New(cfg)

	rng2 := rand.New(rand.NewSource(42))
	gen2 := New(cfg)

	states := []model.State{
		model.StateLanding,
		model.StateSearch,
		model.StateProductView,
		model.StateAddToCart,
		model.StateCheckout,
		model.StatePurchase,
	}

	for i := 0; i < 50; i++ {
		s := states[i%len(states)]
		d1 := gen1.GenerateData(rng1, s)
		d2 := gen2.GenerateData(rng2, s)
		assert.Equal(t, string(d1), string(d2),
			"generator output differs at step %d for state %s", i, s)
	}
}

func TestGeneratorAllStatesProduceValidJSON(t *testing.T) {
	cfg := testConfig()
	rng := rand.New(rand.NewSource(42))
	gen := New(cfg)

	states := []model.State{
		model.StateLanding,
		model.StateSearch,
		model.StateProductView,
		model.StateAddToCart,
		model.StateCheckout,
		model.StatePurchase,
	}

	for _, s := range states {
		for i := 0; i < 20; i++ {
			data := gen.GenerateData(rng, s)
			require.True(t, len(data) > 2, "empty data for state %s", s)
			require.True(t, data[0] == '{', "data should be JSON object for state %s, got: %s", s, string(data))
		}
	}
}

func TestGeneratorProductSelection(t *testing.T) {
	cfg := testConfig()
	rng := rand.New(rand.NewSource(42))
	gen := New(cfg)

	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		data := gen.GenerateData(rng, model.StateProductView)
		seen[string(data)] = true
	}

	assert.True(t, len(seen) > 1, "should produce multiple different product views")
}

func TestGeneratorNoPanicOnEmptyProducts(t *testing.T) {
	cfg := &config.Config{
		Products:      nil,
		SearchQueries: []string{"headphones"},
	}
	gen := New(cfg)
	rng := rand.New(rand.NewSource(42))

	for _, s := range []model.State{model.StateProductView, model.StateAddToCart} {
		t.Run(string(s), func(t *testing.T) {
			data := gen.GenerateData(rng, s)
			require.True(t, len(data) >= 2, "should return valid JSON, got: %s", string(data))
			require.True(t, data[0] == '{', "should return JSON object for state %s", s)
		})
	}
}

func TestGeneratorNoPanicOnEmptyQueries(t *testing.T) {
	cfg := &config.Config{
		Products:      []config.Product{{ID: "p1", Name: "P1", Category: "c", Price: 10}},
		SearchQueries: nil,
	}
	gen := New(cfg)
	rng := rand.New(rand.NewSource(42))

	data := gen.GenerateData(rng, model.StateSearch)
	require.True(t, len(data) >= 2, "should return valid JSON, got: %s", string(data))
	require.True(t, data[0] == '{', "should return JSON object")
}

func TestGeneratorNoPanicOnEmptyProductsAndQueries(t *testing.T) {
	cfg := &config.Config{
		Products:      nil,
		SearchQueries: nil,
	}
	gen := New(cfg)
	rng := rand.New(rand.NewSource(42))

	states := []model.State{
		model.StateLanding,
		model.StateSearch,
		model.StateProductView,
		model.StateAddToCart,
		model.StateCheckout,
		model.StatePurchase,
	}
	for _, s := range states {
		t.Run(string(s), func(t *testing.T) {
			data := gen.GenerateData(rng, s)
			require.True(t, len(data) > 0, "should return data for state %s", s)
		})
	}
}
