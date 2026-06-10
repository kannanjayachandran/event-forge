package sink

import (
	"encoding/json"
	"os"
	"sync"

	"event-sim/internal/model"
)

type StdoutSink struct {
	mu sync.Mutex
}

func NewStdoutSink() *StdoutSink {
	return &StdoutSink{}
}

func (s *StdoutSink) Write(event model.Event) error {
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}
	b = append(b, '\n')

	s.mu.Lock()
	_, err = os.Stdout.Write(b)
	s.mu.Unlock()

	return err
}

func (s *StdoutSink) Close() error {
	return nil
}
