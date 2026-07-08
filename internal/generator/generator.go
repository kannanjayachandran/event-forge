package generator

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"

	"event-sim/internal/config"
	"event-sim/internal/model"
)

// Generator produces realistic fake event payloads for each simulation state.
type Generator struct {
	products []config.Product
	queries  []string
}

// Returns a Generator configured from cfg
func New(cfg *config.Config) *Generator {
	return &Generator{
		products: cfg.Products,
		queries:  cfg.SearchQueries,
	}
}

// GenerateData produces a JSON payload for the given state
// Returns an empty JSON object for unrecognised states.
func (g *Generator) GenerateData(rng *rand.Rand, state model.State) json.RawMessage {
	switch state {
	case model.StateLanding:
		return g.landingData()
	case model.StateSearch:
		return g.searchData(rng)
	case model.StateProductView:
		return g.productViewData(rng)
	case model.StateAddToCart:
		return g.addToCartData(rng)
	case model.StateCheckout:
		return g.checkoutData(rng)
	case model.StatePurchase:
		return g.purchaseData(rng)
	default:
		return json.RawMessage(`{}`)
	}
}

// landingData is deterministic
func (g *Generator) landingData() json.RawMessage {
	return json.RawMessage(`{"page":"/","referrer":"direct"}`)
}

func (g *Generator) searchData(rng *rand.Rand) json.RawMessage {
	if len(g.queries) == 0 {
		return json.RawMessage(`{}`)
	}
	q := g.queries[rng.Intn(len(g.queries))]
	results := rng.Intn(50) + 1
	data, _ := json.Marshal(map[string]any{
		"query":   q,
		"results": results,
	})
	return data
}

func (g *Generator) randomProduct(rng *rand.Rand) *config.Product {
	if len(g.products) == 0 {
		return nil
	}
	return &g.products[rng.Intn(len(g.products))]
}

func (g *Generator) productViewData(rng *rand.Rand) json.RawMessage {
	p := g.randomProduct(rng)
	if p == nil {
		return json.RawMessage(`{}`)
	}
	data, _ := json.Marshal(map[string]any{
		"product_id": p.ID,
		"name":       p.Name,
		"category":   p.Category,
		"price":      p.Price,
	})
	return data
}

func (g *Generator) addToCartData(rng *rand.Rand) json.RawMessage {
	p := g.randomProduct(rng)
	if p == nil {
		return json.RawMessage(`{}`)
	}
	qty := rng.Intn(3) + 1
	data, _ := json.Marshal(map[string]any{
		"product_id": p.ID,
		"name":       p.Name,
		"quantity":   qty,
		"unit_price": p.Price,
		"total":      round2(float64(qty) * p.Price),
	})
	return data
}

func (g *Generator) checkoutData(rng *rand.Rand) json.RawMessage {
	items := rng.Intn(5) + 1
	total := float64(items) * (rng.Float64()*100 + 20)
	data, _ := json.Marshal(map[string]any{
		"cart_size": items,
		"total":     round2(total),
	})
	return data
}

func (g *Generator) purchaseData(rng *rand.Rand) json.RawMessage {
	items := rng.Intn(5) + 1
	total := float64(items) * (rng.Float64()*100 + 20)
	data, _ := json.Marshal(map[string]any{
		"order_id":    fmt.Sprintf("ORD-%08d", rng.Intn(100000000)),
		"total":       round2(total),
		"items_count": items,
	})
	return data
}

func round2(f float64) float64 {
	return math.Round(f*100) / 100
}
