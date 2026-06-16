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

// Caps how long a single kafka produce call may block
const sendTimeout = 5 * time.Second

// It is called at most once per goroutine (to generate a local seed)
// so contention is negligible
type lockedSource struct {
	mu  sync.Mutex
	src rand.Source
}

func (ls *lockedSource) Int63() int64 {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.src.Int63()
}

func (ls *lockedSource) Seed(seed int64) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.src.Seed(seed)
}

// Simulator runs N concurrent user sessions, each driven by a state machine,
// emitting events to Kafka and optional secondary sinks.
type Simulator struct {
	cfg      *config.Config
	sm       *statemachine.StateMachine
	gen      *generator.Generator
	producer *producer.Producer
	sinks    []sink.Sink
	logger   *zap.Logger
	seeder   *rand.Rand
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func New(cfg *config.Config, p *producer.Producer, sinks []sink.Sink, logger *zap.Logger) (*Simulator, error) {
	seeder := rand.New(&lockedSource{
		src: rand.NewSource(cfg.Simulator.Seed),
	})

	sm, err := statemachine.New(cfg.StateMachine.Transitions)
	if err != nil {
		return nil, err
	}

	return &Simulator{
		cfg:      cfg,
		sm:       sm,
		gen:      generator.New(cfg),
		producer: p,
		sinks:    sinks,
		logger:   logger,
		seeder:   seeder,
	}, nil
}

// Run starts all concurrent user sessions and blocks until ctx is cancelled
// and every session goroutine has returned.
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

// runSession simulates a single user lifecycle indefinitely, resetting to the
// landing state whenever a terminal state or implicit drop-off is reached.
func (s *Simulator) runSession(ctx context.Context) {
	defer s.wg.Done()

	localRng := rand.New(rand.NewSource(s.seeder.Int63()))

	sessionID, userID, currentState := s.newSession()

	// Allocate one timer for the goroutine lifetime. Reusing it avoids the
	// per-iteration heap allocation
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		// Fast cancellation check before doing any work this iteration
		select {
		case <-ctx.Done():
			return
		default:
		}

		start := time.Now()

		evt := model.Event{
			EventID:   uuid.New().String(),
			Timestamp: start.UTC(),
			SessionID: sessionID,
			UserID:    userID,
			EventType: currentState,
			Data:      s.gen.GenerateData(localRng, currentState),
		}
		// edited ...
		for _, sk := range s.sinks {
			if err := sk.Write(evt); err != nil {
				s.logger.Error("sink write failed", zap.Error(err))
			}
		}
		// Bound the Kafka produce call. On timeout or error we count the drop
		// and keep the session alive {Frozen simulator is worse than lost events}
		sendCtx, cancelSend := context.WithTimeout(ctx, sendTimeout)
		if err := s.producer.Send(sendCtx, evt); err != nil {
			s.logger.Error("producer send failed", zap.Error(err))
			metrics.EventsDropped.WithLabelValues(string(currentState)).Inc()
		}
		cancelSend()

		metrics.EventsProduced.WithLabelValues(string(currentState)).Inc()
		metrics.EventDuration.WithLabelValues(string(currentState)).Observe(float64(time.Since(start).Seconds()))

		if model.TerminalStates[currentState] {
			sessionID, userID, currentState = s.newSession()
		} else {
			nextState, ended := s.sm.Next(localRng, currentState)
			if ended {
				sessionID, userID, currentState = s.newSession()
			} else {
				currentState = nextState
			}
		}

		delay := s.sampleDelay(localRng)
		timer.Reset(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// newSession returns fresh identifiers and the initial state for a user session.
func (s *Simulator) newSession() (sessionID, userID string, state model.State) {
	metrics.SessionsCreated.Inc()
	return uuid.New().String(), uuid.New().String(), model.StateLanding
}

// sampleDelay returns a delay drawn from the configured timing distribution,
// clamped to [10ms, 30s].
func (s *Simulator) sampleDelay(rng *rand.Rand) time.Duration {
	dist := s.cfg.Simulator.Timing.Distribution
	if dist == "" {
		dist = "exponential"
	}

	var delay float64
	switch dist {
	case "lognormal":
		sigma := s.cfg.Simulator.Timing.Sigma
		if sigma <= 0 {
			sigma = 1.0
		}
		delay = logNormalSample(rng, s.cfg.Simulator.Timing.Mu, sigma)
	default: // exponential
		alpha := s.cfg.Simulator.Timing.Alpha
		if alpha <= 0 && s.cfg.Simulator.EventsPerSecond > 0 {
			alpha = float64(s.cfg.Simulator.ConcurrentUsers) / s.cfg.Simulator.EventsPerSecond
		}
		if alpha <= 0 {
			alpha = 1.0
		}

		delay = rng.ExpFloat64() * alpha
	}

	minDelay := 0.01
	maxDelay := 30.0
	if delay < minDelay {
		delay = minDelay
	} else if delay > maxDelay {
		delay = maxDelay
	}

	return time.Duration(delay * float64(time.Second))
}

// logNormalSample draws from LogNormal(mu, sigma)
func logNormalSample(rng *rand.Rand, mu, sigma float64) float64 {
	return math.Exp(mu + sigma*rng.NormFloat64())
}

// Stop cancels the run context and blocks until all goroutines exit.
func (s *Simulator) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}
