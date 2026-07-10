package sink

import (
	"context"
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

func (s *StdoutSink) Write(ctx context.Context, event model.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
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
