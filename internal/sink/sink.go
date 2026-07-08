package sink

import (
	"sync"

	"event-sim/internal/model"
)

type Sink interface {
	Write(event model.Event) error
	Close() error
}

type SliceSink struct {
	mu     sync.Mutex
	Events []model.Event
}

func (s *SliceSink) Write(event model.Event) error {
	s.mu.Lock()
	s.Events = append(s.Events, event)
	s.mu.Unlock()
	return nil
}

func (s *SliceSink) Close() error {
	return nil
}

func (s *SliceSink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.Events)
}
