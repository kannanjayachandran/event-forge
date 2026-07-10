# Implementation Plan: EventForge Technical Debt Remediation

## Overview

Remediate the 20 findings from the full code review of EventForge, organized into four
priority levels. Work proceeds in phases to minimize risk: critical correctness bugs first,
then high-quality gaps, then maintainability infrastructure, finally low-risk optimizations.
Every phase leaves the system building and passing tests.

## Dependency Graph

```
Phase 0 (Stabilize)
├── T1: Config validation        (foundation — many fixes depend on reliable config)
├── T2: Fix empty-list panics    (depends on T1 — or handle defensively in generator)
├── T3: Kafka-optional startup   (independent of T1/T2)
└── T4: Fix config test          (depends on config being stable; do after T1)
Checkpoint ─── all tests pass, build clean

Phase 1 (Correctness & Observability)
├── T5: De-duplicate state list   (safe refactor, no behavioral change)
├── T6: FileSink buffering        (depends on T10 if we touch sink interface)
├── T7: Surface async errors      (independent)
├── T8: Test Simulator.Run()      (benefits from T1 being done)
├── T9: Context-aware sinks       (sink interface change — do before or with T6)
└── T10: Integration tests        (after T3 Kafka-optional + T1 config validation)
Checkpoint ─── all tests pass, manual run of simulator succeeds

Phase 2 (Infrastructure & Maintainability)
├── T11: Makefile
├── T12: Golangci-lint config
├── T13: GitHub Actions CI
├── T14: Suppress stdout in test  (quick fix)
├── T15: Rename module             (touch every file — do once, coordinate)
└── T16: Move planning docs
Checkpoint ─── CI green, lint clean, module path consistent

Phase 3 (Optimization)
├── T17: Parallelize sink writes   (depends on T9 context-aware sinks)
├── T18: Custom error types        (independent, but scattered across packages)
├── T19: Log rotation in FileSink  (depends on T6 buffering)
└── T20: Pre-allocate event buffer (independent, hot-path micro-opt)
```

## Architecture Decisions

- **Sink interface will gain a `Context` parameter** — this is a breaking change that
  touches all three sink implementations and all call sites. Do it once in a single task.
- **Config validation is a new function** in `config.go`, not a separate package.
  Fail-fast at startup, not at runtime.
- **Integration tests use `testcontainers-go`** with a Redpanda container to avoid
  requiring a real Kafka cluster in CI.
- **State list moves to `model.go`** as an `AllStates` slice derived from the constants.
  `metrics.go` and any other consumers reference it by import.
- **Module rename** is a mechanical find-and-replace across ~20 files. Do it in one shot
  with a script, update `go.mod`, verify build.

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Sink interface change (T9) breaks callers elsewhere | High | Pinpoint all `Sink` references before starting; compiler catches missing implementations |
| Module rename introduces import drift | Medium | Do it last in Phase 2; run full build + test cycle immediately after |
| Integration tests flake due to container timing | Medium | Use retry logic and health checks; mark tests as `go test --tags=integration` |
| Parallel sink writes (T17) change event ordering | Medium | Errgroup with ordered output or document that ordering is not guaranteed |

## Open Questions

- Should integration tests live in a separate `tests/` directory or alongside package tests
  with a build tag? Recommend build tag approach (`//go:build integration`).
- Log rotation: simple file-size-based rotation, or time-based? Recommend size-based
  (100 MB default) to start — simpler, catches the unbounded-growth problem.
