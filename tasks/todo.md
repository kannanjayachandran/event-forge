# EventForge — Implementation Tasks

---

## Phase 0: Stabilize (Critical)

### Task 1: Validate configuration at load time  ✅ DONE

**Description:** Add a `Validate() error` method to `Config` that checks for semantic
correctness before the simulator starts. Catch negative values, empty required fields
(brokers, topic), empty product/query lists, invalid compression types, invalid timing
distribution names, probabilities outside [0,1], and transition probabilities that sum > 1.

**Acceptance criteria:**
- [x] Config with negative `concurrent_users` returns a clear error
- [x] Config with zero `events_per_second` returns a clear error
- [x] Config with empty `products` array returns a clear error
- [x] Config with empty `search_queries` returns a clear error
- [x] Config with invalid compression type returns a clear error
- [x] Config with missing required fields returns a clear error
- [x] Valid config passes without error
- [x] Error messages are human-readable and specify the field path

**Verification:**
- [x] New unit tests (26) in `internal/config/config_test.go` cover all validation cases
- [x] Existing tests still pass (pre-existing `TestLoadDefaultConfig` failure is unrelated — Task 4)
- [x] `go build ./...` succeeds

---

### Task 2: Fix `rng.Intn(0)` panic for empty products or queries  ✅ DONE

**Description:** In `generator.go`, `rng.Intn(len(g.queries))` and
`rng.Intn(len(g.products))` panic when the respective slice is empty. Fix by returning
a meaningful empty payload (e.g., `{"error": "no products configured"}`) or falling back
gracefully. The root cause (empty config) should already be caught by Task 1 validation,
but this adds defense-in-depth.

**Acceptance criteria:**
- [x] Generator does not panic when `products` is empty
- [x] Generator does not panic when `search_queries` is empty
- [x] Events are still produced with a sensible fallback payload (`{}`)

**Verification:**
- [x] Unit test with empty products (`TestGeneratorNoPanicOnEmptyProducts`)
- [x] Unit test with empty queries (`TestGeneratorNoPanicOnEmptyQueries`)
- [x] Unit test with both empty (`TestGeneratorNoPanicOnEmptyProductsAndQueries`)
- [x] All existing tests pass

**Dependencies:** None (defense-in-depth independent of Task 1)

**Files likely touched:**
- `internal/generator/generator.go`
- `internal/generator/generator_test.go`

**Estimated scope:** XS (1-2 files)

---

### Task 3: Make Kafka topic init conditional  ✅ DONE

**Description:** `main.go:90-92` calls `initTopic` unconditionally, which blocks startup
if Kafka is unreachable — even when the user only configured file/stdout sinks.
Check whether any sink requires Kafka (currently the producer is always created, so this
means: skip topic init if no Kafka sink is configured, or if the producer is unused).

**Design decision:** Add a `kafka` entry to the sinks list, or check if the simulator
should skip Kafka entirely. Simplest approach: only call `initTopic` and `producer.New`
when at least one configured sink is `"kafka"`. If no Kafka sink is configured, skip
producer creation entirely.

**Acceptance criteria:**
- [x] Simulator starts and runs with sinks = `["file"]` when no Kafka is available
- [x] Simulator starts and runs with sinks = `["stdout"]` when no Kafka is available
- [x] Simulator still requires Kafka when sinks include `"kafka"`
- [x] Existing behavior is preserved when Kafka is configured and available

**Verification:**
- [x] Unit tests: `TestValidateConfigWithoutKafkaSkipsKafkaValidation` validates no-Kafka configs
- [x] Unit tests: `TestValidateEmptyBrokers` etc. still validate when `"kafka"` is in sinks
- [x] All existing tests pass

**Dependencies:** None (independent)

**Files likely touched:**
- `cmd/sim/main.go`
- `internal/config/config.go` (add "kafka" as a valid sink type)
- `internal/config/config_test.go`

**Estimated scope:** Small (1-2 files)

---

### Task 4: Fix failing config test  ✅ DONE

**Description:** `TestLoadDefaultConfig` expects old config values (seed=42, users=20,
eps=25, 10 products, 15 queries) but the config file was updated (users=100, eps=200,
60 products, 99 queries). Update test expectations to match current config. Also consider
extracting test-specific configs rather than loading the production file.

**Acceptance criteria:**
- [x] `TestLoadDefaultConfig` passes against the current `configs/config.yaml`
- [x] Test values match actual config values
- [x] Tests are not brittle to config changes (consider using inline test configs)

**Verification:**
- [x] `go test ./internal/config/...` passes
- [x] Full `go test ./...` passes

**Dependencies:** None (safe to do early)

**Files likely touched:**
- `internal/config/config_test.go`

**Estimated scope:** XS (1 file)

---

> **Checkpoint Phase 0** — `go test ./...` passes, `go vet ./...` is clean,
> `go build ./...` succeeds. Simulator starts in file-only mode without Kafka.

---

## Phase 1: Correctness & Observability (High)

### Task 5: De-duplicate state list  ✅ DONE

**Description:** Remove the hardcoded `eventTypes` slice from `internal/metrics/metrics.go`
and replace with a single source of truth in `internal/model/model.go`. Add
`var AllStates = []State{StateLanding, StateSearch, ...}` derived from the constants.
Metrics pre-initialization loop uses `model.AllStates` instead of a separate list.

**Acceptance criteria:**
- [x] `model.AllStates` contains all defined states
- [x] `metrics.go` references `model.AllStates` instead of its own list
- [x] All state-dependent labels are pre-initialized via the shared list
- [x] Adding a new state constant to `model.go` automatically picks it up in metrics

**Verification:**
- [x] `go test ./...` passes
- [x] Prometheus `/metrics` endpoint still shows all event types

**Dependencies:** None (pure refactor)

**Files likely touched:**
- `internal/model/model.go`
- `internal/metrics/metrics.go`

**Estimated scope:** XS (2 files)

---

### Task 6: Add buffered writer to FileSink  ✅ DONE

**Description:** Wrap `file.Write` calls with a `bufio.Writer` to batch write(2) syscalls.
Flush on `Close()`. This reduces syscall overhead from O(events) to O(buffer flushes).
Default buffer size of 64KB is appropriate.

**Acceptance criteria:**
- [x] FileSink writes go through a buffered writer
- [x] `Close()` flushes the buffer before closing the file
- [x] All data is persisted on close (no data loss from buffering)
- [x] Existing FileSink tests pass unchanged

**Verification:**
- [x] Benchmark shows reduced syscall count
- [x] `TestFileSink`, `TestFullPipelineDeterministicReplay` still pass

**Dependencies:** Task 9 (context-aware sinks) if we change the Write signature.
If not, independent.

**Files likely touched:**
- `internal/sink/file.go`
- `internal/sink/file_test.go`

**Estimated scope:** Small (1-2 files)

---

### Task 7: Surface async Kafka producer errors

**Description:** The producer uses `Async: true` which means `WriteMessages` returns
immediately. Errors are only logged in the `Completion` callback. Change to synchronous
mode or add a channel-based error reporting mechanism so the simulator can count errors
and optionally back-pressure or stop on persistent failures. Simplest approach: switch to
`Async: false` and let the caller handle per-message errors with context timeouts.

**Acceptance criteria:**
- [ ] Producer send errors are surfaced to the caller (not just logged)
- [ ] `EventsDropped` counter is incremented on send failures (already done in simulator)
- [ ] Back-pressure propagates naturally through blocking writes
- [ ] Existing timeout mechanism (`sendTimeout`) continues to prevent hangs

**Verification:**
- [ ] Unit test with a mock/proxy that simulates Kafka failures
- [ ] All existing tests pass

**Dependencies:** None

**Files likely touched:**
- `internal/producer/producer.go`

**Estimated scope:** Small (1 file)

---

### Task 8: Test Simulator.Run() directly

**Description:** Replace the duplicated `produceEvents` test helper with tests that
instantiate a real `Simulator` with a mock sink and call `Run(ctx)` with a
short-lived context. This tests the actual goroutine management, timer reuse, and
shutdown logic. Use a `sink.Sink` that records events to a slice for assertion.

**Acceptance criteria:**
- [ ] `TestFullPipelineDeterministicReplay` uses `Simulator.Run()` not custom loop
- [ ] `TestFullPipelineStateMachineValidity` uses `Simulator.Run()` not custom loop
- [ ] A test verifies that cancelling the context stops all goroutines
- [ ] A test verifies that `Stop()` waits for all goroutines to finish
- [ ] Timer/delay path is exercised (use a zero-delay config for fast tests)

**Verification:**
- [ ] New tests pass consistently (not flaky)
- [ ] Old `produceEvents` helper is removed or kept only for benchmark
- [ ] Race detector: `go test -race ./internal/simulator/...` passes

**Dependencies:** Task 1 (config validation) makes test config setup cleaner

**Files likely touched:**
- `internal/simulator/simulator_test.go`
- `internal/sink/sink.go` (add a `SliceSink` test helper or use existing interface)

**Estimated scope:** Medium (2-3 files)

---

### Task 9: Add context parameter to Sink interface

**Description:** Add `ctx context.Context` as the first parameter of `sink.Sink.Write()`.
This allows sinks to respect cancellation during shutdown. Current sink implementations
(FileSink, StdoutSink) should pass the context through, or at minimum check
`ctx.Done()` before long operations. Update all call sites in simulator.go.

**Acceptance criteria:**
- [ ] `Sink.Write(ctx, event)` signature compiles
- [ ] FileSink checks context before/during write
- [ ] StdoutSink checks context before/during write
- [ ] Simulator passes cancellation context to sink writes
- [ ] Shutdown is responsive even if a sink is blocked

**Verification:**
- [ ] `go build ./...` succeeds
- [ ] All sink tests updated and pass
- [ ] Simulator tests pass
- [ ] Manual: simulator exits promptly on SIGINT with slow sink

**Dependencies:** None (but T6 and T17 depend on this)

**Files likely touched:**
- `internal/sink/sink.go`
- `internal/sink/file.go`
- `internal/sink/file_test.go`
- `internal/sink/stdout.go`
- `internal/simulator/simulator.go`

**Estimated scope:** Medium (5 files — interface change touches all sink users)

---

### Task 10: Add integration tests with testcontainers-go

**Description:** Add integration tests that spin up a Redpanda container, produce events
via the simulator, and verify they arrive. Use `//go:build integration` build tags so
they run separately from unit tests. Test: (a) basic produce and consume,
(b) deterministic replay produces identical events, (c) multiple sink modes.

**Acceptance criteria:**
- [ ] Integration test starts a Redpanda container and waits for it to be healthy
- [ ] Test produces N events via the simulator and consumes them back
- [ ] Test verifies event count matches
- [ ] Test verifies event schema is valid
- [ ] Tests are skipped during normal `go test ./...` (build tags)
- [ ] Container is cleaned up on test completion

**Verification:**
- [ ] `go test --tags=integration -v ./internal/simulator/...` passes
- [ ] No rouge containers left after test run

**Dependencies:** Task 3 (Kafka-optional startup), Task 1 (config validation)

**Files likely touched:**
- `internal/simulator/simulator_integration_test.go` (new file)
- `go.mod` (add `testcontainers-go` dependency)
- `internal/sink/sink.go` (may need a Kafka sink test double)

**Estimated scope:** Medium (2-3 files)

---

> **Checkpoint Phase 1** — `go test -race ./...` passes, integration tests pass,
> manual verification: simulator runs with file sink, kafka sink, and both.

---

## Phase 2: Infrastructure & Maintainability (Medium)

### Task 11: Add Makefile

**Description:** Create a Makefile with targets: `build`, `test`, `test-race`,
`test-integration`, `vet`, `lint`, `fmt`, `clean`, `run`. Standardize on `make`
as the entry point for all common development tasks.

**Acceptance criteria:**
- [ ] `make build` compiles the binary
- [ ] `make test` runs all unit tests
- [ ] `make test-race` runs with the race detector
- [ ] `make test-integration` runs integration tests
- [ ] `make vet` runs `go vet`
- [ ] `make lint` runs golangci-lint (after T12)
- [ ] `make clean` removes build artifacts
- [ ] `make` (default) runs `build` + `test` + `vet`

**Verification:**
- [ ] Each target succeeds on a clean checkout
- [ ] `make clean` leaves no build artifacts

**Dependencies:** None

**Files likely touched:**
- `Makefile` (new file)

**Estimated scope:** Small (1 file)

---

### Task 12: Add golangci-lint configuration

**Description:** Create `.golangci.yml` with sensible defaults. Enable: `errcheck`,
`gosimple`, `govet`, `ineffassign`, `staticcheck`, `unused`. Disable noisy linters.
Fix any lint issues found in existing code.

**Acceptance criteria:**
- [ ] `.golangci.yml` exists with project-appropriate configuration
- [ ] `golangci-lint run ./...` passes with zero issues (or explicitly waived)
- [ ] CI runs lint (after T13)

**Verification:**
- [ ] `golangci-lint run ./...` exits 0

**Dependencies:** Task 11 (Makefile), Task 14 (suppress stdout test first)

**Files likely touched:**
- `.golangci.yml` (new file)
- Various source files (to fix pre-existing lint issues)

**Estimated scope:** Small (1 config file + fixes)

---

### Task 13: Set up GitHub Actions CI

**Description:** Create `.github/workflows/ci.yml` with jobs for: build, test (unit),
test (race), vet, lint. Run on push/PR to main. Cache Go modules and build cache.
Integration test job is optional (marked as allow-failure if no Docker in runner).

**Acceptance criteria:**
- [ ] CI runs on every push and pull request
- [ ] Build, test, vet, lint all run
- [ ] Go module cache is cached between runs
- [ ] Failing CI blocks merge (branch protection)
- [ ] Integration tests run in CI if Docker is available

**Verification:**
- [ ] Push to a branch triggers CI
- [ ] CI is green

**Dependencies:** Task 11 (Makefile), Task 12 (lint config)

**Files likely touched:**
- `.github/workflows/ci.yml` (new file)
- `.github/workflows/integration.yml` (new file, optional)

**Estimated scope:** Small (1-2 files)

---

### Task 14: Suppress StdoutSink test output

**Description:** `sink/file_test.go:TestStdoutSink` writes a real JSON line to stdout
during `go test ./...`, polluting test output in CI and local runs. Capture stdout
during this test, or skip the real write and only test the serialization logic.
Simplest fix: redirect `os.Stdout` to a temp buffer during the test.

**Acceptance criteria:**
- [ ] `go test ./...` produces no spurious stdout lines from the StdoutSink test
- [ ] StdoutSink Write logic is still tested

**Verification:**
- [ ] `go test -v ./internal/sink/...` output is clean

**Dependencies:** None

**Files likely touched:**
- `internal/sink/file_test.go`

**Estimated scope:** XS (1 file)

---

### Task 15: Rename module from `event-sim` to `event-forge`

**Description:** Update `go.mod` module path from `event-sim` to `event-forge`.
Update all internal import paths. This is a mechanical find-and-replace across the
entire codebase. Do it in one commit, then verify build.

**Acceptance criteria:**
- [ ] `go.mod` declares `module event-forge`
- [ ] All imports in all `.go` files use `event-forge/...`
- [ ] `go build ./...` succeeds
- [ ] `go test ./...` succeeds

**Verification:**
- [ ] Full build + test + vet cycle passes

**Dependencies:** Task 11 (Makefile) — do after Makefile exists so `make build` is the
canonical build command

**Files likely touched:**
- `go.mod`
- Every `.go` file with imports from the module (~20 files)

**Estimated scope:** Medium (~20 files, but mechanical)

---

### Task 16: Move planning documents out of temp/

**Description:** Move `temp/PH2-Feat-Issues.md`, `temp/Phase1.5.md`, `temp/What_is_this.md`
to a `docs/` directory or retire them to project issues. Update `.gitignore` to exclude
`temp/` contents (currently `/temp` ignores only the directory entry, not committed files).

**Acceptance criteria:**
- [ ] Planning documents are no longer in `temp/`
- [ ] Planned work items are tracked as GitHub Issues (or moved to `docs/`)
- [ ] `.gitignore` correctly prevents future temp file commits
- [ ] No dangling references to `temp/*` paths

**Verification:**
- [ ] `git status` shows no unexpected files under `temp/`

**Dependencies:** None

**Files likely touched:**
- `.gitignore`
- `docs/` (new directory with moved/archived docs)

**Estimated scope:** XS (1-2 files)

---

> **Checkpoint Phase 2** — CI green, lint passes, module path consistent, build
> succeeds from clean checkout with `make build`.

---

## Phase 3: Optimization (Low)

### Task 17: Parallelize sink writes

**Description:** In `simulator.go:131-135`, sink writes are sequential. Use an
`errgroup` or `sync.WaitGroup` to write to all sinks concurrently. Respect per-sink
timeouts. Collect errors from all sinks rather than returning on the first failure.
Use the context from Task 9 for cancellation.

**Acceptance criteria:**
- [ ] Concurrent sink writes take max(sink latency) instead of sum
- [ ] All sinks are written to even if one fails
- [ ] All sink errors are logged
- [ ] Context cancellation propagates to in-flight sink writes

**Verification:**
- [ ] Benchmark shows reduced end-to-end event latency with multiple sinks
- [ ] All existing tests pass
- [ ] Race detector: `go test -race ./...` passes

**Dependencies:** Task 9 (context-aware sinks)

**Files likely touched:**
- `internal/simulator/simulator.go`

**Estimated scope:** Small (1 file)

---

### Task 18: Add custom error types

**Description:** Replace `fmt.Errorf` with typed sentinel errors or custom error types
in strategic locations: config validation errors, state machine validation errors,
producer errors. This allows callers to use `errors.Is` / `errors.As` for proper
error handling.

**Acceptance criteria:**
- [ ] Config validation uses `ErrConfigInvalid` or `ConfigError` type
- [ ] State machine exceeds-probability uses `ErrProbabilitiesExceeded`
- [ ] Producer errors are not swallowed
- [ ] Error types are exported and documented

**Verification:**
- [ ] `go build ./...` succeeds
- [ ] Tests can use `errors.Is` to assert specific error conditions
- [ ] All existing tests pass

**Dependencies:** None

**Files likely touched:**
- `internal/config/config.go`
- `internal/statemachine/machine.go`
- `internal/producer/producer.go`

**Estimated scope:** Small (3 files)

---

### Task 19: Add log rotation to FileSink

**Description:** Add configurable log rotation to FileSink: max file size (default
100 MB), max backup files. When the current file exceeds max size, close it, rename
to `.1`, open a new file. This prevents unbounded disk usage in long-running
simulations.

**Acceptance criteria:**
- [ ] FileSink rotates when file exceeds configured `max_size`
- [ ] Old files are renamed with sequential suffixes (`.1`, `.2`, ...)
- [ ] Configurable max backup count prevents unlimited disk usage
- [ ] Rotation preserves existing events (no data loss)
- [ `Close()` flushes and closes the current file

**Verification:**
- [ ] Unit test: write enough data to trigger rotation, verify multiple files exist
- [ ] Unit test: verify max backup count is respected
- [ ] All existing tests pass

**Dependencies:** Task 6 (buffered writer — rotation logic builds on buffered I/O)

**Files likely touched:**
- `internal/sink/file.go`
- `internal/config/config.go` (add `MaxFileSize`, `MaxBackups` config fields)
- `internal/config/config_test.go`
- `configs/config.yaml` (add rotation config)

**Estimated scope:** Medium (3-4 files)

---

### Task 20: Pre-allocate event buffer in hot loop

**Description:** The event creation in `simulator.go:122-129` allocates a new
`model.Event` struct on every iteration. Pre-allocate and reuse the struct, resetting
only the fields that change (EventID, Timestamp, Data). This reduces GC pressure in
the hot path at high event rates.

**Acceptance criteria:**
- [ ] Event struct is reused across iterations in `runSession`
- [ ] All fields are correctly reset before each use
- [ ] No data races (mutated only by one goroutine)
- [ ] Behavior is identical (tested via deterministic replay)

**Verification:**
- [ ] Deterministic replay tests still produce identical output
- [ ] Benchmark shows reduced allocation rate
- [ ] `go test -race ./...` passes

**Dependencies:** None (hot-path micro-optimization, safe to do last)

**Files likely touched:**
- `internal/simulator/simulator.go`

**Estimated scope:** Small (1 file)

---

> **Checkpoint Phase 3** — All 20 tasks complete, full test suite green,
> race-clean, lint-clean, CI green.
