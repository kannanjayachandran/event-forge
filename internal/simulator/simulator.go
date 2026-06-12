package simulator

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"event-sim/internal/config"
	"event-sim/internal/generator"
	"event-sim/internal/metrics"
	"event-sim/internal/model"
	"event-sim/internal/producer"
	"event-sim/internal/sink"
	"event-sim/internal/statemachine"
)

type Simulator struct {
	cfg      *config.Config
	sm       *statemachine.StateMachine
	gen      *generator.Generator
	producer *producer.Producer
	sinks    []sink.Sink
	logger   *zap.Logger
	rng      *rand.Rand
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func New(cfg *config.Config, p *producer.Producer, sinks []sink.Sink, logger *zap.Logger) (*Simulator, error) {
	rng := rand.New(rand.NewSource(cfg.Simulator.Seed))

	sm, err := statemachine.New(rng, cfg.StateMachine.Transitions)
	if err != nil {
		return nil, err
	}

	gen := generator.New(rng, cfg)

	return &Simulator{
		cfg:      cfg,
		sm:       sm,
		gen:      gen,
		producer: p,
		sinks:    sinks,
		logger:   logger,
		rng:      rng,
	}, nil
}

func (s *Simulator) Run(ctx context.Context) error {
	ctx, s.cancel = context.WithCancel(ctx)
	defer s.cancel()

	metrics.ActiveUsers.Set(float64(s.cfg.Simulator.ConcurrentUsers))

	for i := 0; i < s.cfg.Simulator.ConcurrentUsers; i++ {
		s.wg.Add(1)
		go s.runSession(ctx)
	}

	s.wg.Wait()
	return nil
}

func (s *Simulator) runSession(ctx context.Context) {
	defer s.wg.Done()

	sessionID := uuid.New().String()
	userID := uuid.New().String()
	currentState := model.StateLanding

	metrics.SessionsCreated.Inc()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		start := time.Now()

		evt := model.Event{
			EventID:   uuid.New().String(),
			Timestamp: time.Now().UTC(),
			SessionID: sessionID,
			UserID:    userID,
			EventType: currentState,
			Data:      s.gen.GenerateData(currentState),
		}

		for _, sk := range s.sinks {
			if err := sk.Write(evt); err != nil {
				s.logger.Error("sink write failed", zap.Error(err))
			}
		}

		if err := s.producer.Send(ctx, evt); err != nil {
			s.logger.Error("producer send failed", zap.Error(err))
		}

		metrics.EventsProduced.WithLabelValues(string(currentState)).Inc()
		metrics.EventDuration.WithLabelValues(string(currentState)).Observe(time.Since(start).Seconds())

		if model.TerminalStates[currentState] {
			sessionID = uuid.New().String()
			userID = uuid.New().String()
			currentState = model.StateLanding
			metrics.SessionsCreated.Inc()
		} else {
			nextState, ended := s.sm.Next(currentState)
			if ended {
				sessionID = uuid.New().String()
				userID = uuid.New().String()
				currentState = model.StateLanding
				metrics.SessionsCreated.Inc()
			} else {
				currentState = nextState
			}
		}

		delay := s.sampleDelay()
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (s *Simulator) sampleDelay() time.Duration {
	dist := s.cfg.Simulator.Timing.Distribution
	if dist == "" {
		dist = "exponential"
	}

	var delay float64
	switch dist {
	case "lognormal":
		mu := s.cfg.Simulator.Timing.Mu
		sigma := s.cfg.Simulator.Timing.Sigma
		if sigma <= 0 {
			sigma = 1.0
		}
		delay = s.logNormalSample(mu, sigma)
	default:
		alpha := s.cfg.Simulator.Timing.Alpha
		if alpha <= 0 {
			alpha = 1.0 / s.cfg.Simulator.EventsPerSecond
		}
		delay = s.rng.ExpFloat64() * alpha
	}

	minDelay := 1.0
	maxDelay := 30.0
	if delay < minDelay {
		delay = minDelay
	}
	if delay > maxDelay {
		delay = maxDelay
	}

	return time.Duration(delay * float64(time.Second))
}

func (s *Simulator) logNormalSample(mu, sigma float64) float64 {
	z := s.rng.NormFloat64()
	return math.Exp(mu + sigma*z)
}

func (s *Simulator) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}
