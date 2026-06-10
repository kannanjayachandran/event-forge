package generator

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"event-sim/internal/config"
	"event-sim/internal/model"
)

type Generator struct {
	rng      *rand.Rand
	products []config.Product
	queries  []string
}

func New(rng *rand.Rand, cfg *config.Config) *Generator {
	return &Generator{
		rng:      rng,
		products: cfg.Products,
		queries:  cfg.SearchQueries,
	}
}

func (g *Generator) GenerateData(state model.State) json.RawMessage {
	switch state {
	case model.StateLanding:
		return g.landingData()
	case model.StateSearch:
		return g.searchData()
	case model.StateProductView:
		return g.productViewData()
	case model.StateAddToCart:
		return g.addToCartData()
	case model.StateCheckout:
		return g.checkoutData()
	case model.StatePurchase:
		return g.purchaseData()
	default:
		return json.RawMessage(`{}`)
	}
}

func (g *Generator) landingData() json.RawMessage {
	return json.RawMessage(`{"page":"/","referrer":"direct"}`)
}

func (g *Generator) searchData() json.RawMessage {
	q := g.queries[g.rng.Intn(len(g.queries))]
	results := g.rng.Intn(50) + 1
	data, _ := json.Marshal(map[string]any{
		"query":   q,
		"results": results,
	})
	return data
}

func (g *Generator) productViewData() json.RawMessage {
	p := g.products[g.rng.Intn(len(g.products))]
	data, _ := json.Marshal(map[string]any{
		"product_id": p.ID,
		"name":       p.Name,
		"category":   p.Category,
		"price":      p.Price,
	})
	return data
}

func (g *Generator) addToCartData() json.RawMessage {
	p := g.products[g.rng.Intn(len(g.products))]
	qty := g.rng.Intn(3) + 1
	data, _ := json.Marshal(map[string]any{
		"product_id": p.ID,
		"name":       p.Name,
		"quantity":   qty,
		"unit_price": p.Price,
		"total":      round2(float64(qty) * p.Price),
	})
	return data
}

func (g *Generator) checkoutData() json.RawMessage {
	items := g.rng.Intn(5) + 1
	total := float64(items) * (g.rng.Float64()*100 + 20)
	data, _ := json.Marshal(map[string]any{
		"cart_size": items,
		"total":     round2(total),
	})
	return data
}

func (g *Generator) purchaseData() json.RawMessage {
	items := g.rng.Intn(5) + 1
	total := float64(items) * (g.rng.Float64()*100 + 20)
	data, _ := json.Marshal(map[string]any{
		"order_id":    fmt.Sprintf("ORD-%08d", g.rng.Intn(100000000)),
		"total":       round2(total),
		"items_count": items,
	})
	return data
}

func round2(f float64) float64 {
	return float64(int(f*100)) / 100
}
