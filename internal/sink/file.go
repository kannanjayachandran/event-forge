package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"sync"

	"event-sim/internal/model"
)

type FileSink struct {
	mu   sync.Mutex
	file *os.File
	bw   *bufio.Writer
}

func NewFileSink(path string) (*FileSink, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &FileSink{file: f, bw: bufio.NewWriter(f)}, nil
}

func (s *FileSink) Write(ctx context.Context, event model.Event) error {
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
	_, err = s.bw.Write(b)
	s.mu.Unlock()

	return err
}

func (s *FileSink) Close() error {
	s.mu.Lock()
	err := s.bw.Flush()
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.file.Close()
}
