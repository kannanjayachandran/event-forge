package sink

import (
	"encoding/json"
	"os"
	"sync"

	"event-sim/internal/model"
)

type FileSink struct {
	mu   sync.Mutex
	file *os.File
}

func NewFileSink(path string) (*FileSink, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &FileSink{file: f}, nil
}

func (s *FileSink) Write(event model.Event) error {
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}
	b = append(b, '\n')

	s.mu.Lock()
	_, err = s.file.Write(b)
	s.mu.Unlock()

	return err
}

func (s *FileSink) Close() error {
	return s.file.Close()
}
