package sink

import "event-sim/internal/model"

type Sink interface {
	Write(event model.Event) error
	Close() error
}
