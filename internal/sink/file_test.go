package sink

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"event-sim/internal/model"
)

func TestFileSink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.ndjson")

	s, err := NewFileSink(path)
	require.NoError(t, err)

	evt := model.Event{
		EventID:   uuid.New().String(),
		Timestamp: time.Now().UTC(),
		SessionID: uuid.New().String(),
		UserID:    uuid.New().String(),
		EventType: model.StateLanding,
		Data:      json.RawMessage(`{"page":"/"}`),
	}

	err = s.Write(context.Background(), evt)
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	b, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.True(t, len(b) > 0)

	var decoded model.Event
	err = json.Unmarshal(b, &decoded)
	require.NoError(t, err)
	assert.Equal(t, evt.EventID, decoded.EventID)
	assert.Equal(t, evt.EventType, decoded.EventType)
}

func TestStdoutSink(t *testing.T) {
	s := NewStdoutSink()

	evt := model.Event{
		EventID:   uuid.New().String(),
		Timestamp: time.Now().UTC(),
		SessionID: uuid.New().String(),
		UserID:    uuid.New().String(),
		EventType: model.StateLanding,
		Data:      json.RawMessage(`{}`),
	}

	err := s.Write(context.Background(), evt)
	assert.NoError(t, err)

	err = s.Close()
	assert.NoError(t, err)
}
