package model

import (
	"encoding/json"
	"time"
)

type State string

const (
	StateLanding     State = "landing"
	StateSearch      State = "search"
	StateProductView State = "product_view"
	StateAddToCart   State = "add_to_cart"
	StateCheckout    State = "checkout"
	StatePurchase    State = "purchase"
)

var AllStates = []State{StateLanding, StateSearch, StateProductView, StateAddToCart, StateCheckout, StatePurchase}

var TerminalStates = map[State]bool{
	StatePurchase: true,
}

type TransitionRule struct {
	From        State   `mapstructure:"from"`
	To          State   `mapstructure:"to"`
	Probability float64 `mapstructure:"probability"`
}

type Event struct {
	EventID   string          `json:"event_id"`
	Timestamp time.Time       `json:"timestamp"`
	SessionID string          `json:"session_id"`
	UserID    string          `json:"user_id"`
	EventType State           `json:"event_type"`
	Data      json.RawMessage `json:"data"`
}
