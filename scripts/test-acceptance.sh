#!/usr/bin/env bash
# Phase 1 Acceptance Tests
# Run after: docker compose up -d
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'
PASS=0
FAIL=0

pass() { PASS=$((PASS+1)); echo -e "  ${GREEN}✓${NC} $1"; }
fail() { FAIL=$((FAIL+1)); echo -e "  ${RED}✗${NC} $1"; }

cleanup() {
  [ -n "${TMPDIR:-}" ] && rm -rf "$TMPDIR"
}
trap cleanup EXIT

# --- Test 1: One Command Startup ---
echo "=== Test 1: One Command Startup ==="
if docker compose ps --format json 2>/dev/null | jq -e 'select(.State == "running")' >/dev/null 2>&1; then
  pass "docker compose ps shows running containers"
else
  fail "containers not running — run 'docker compose up -d' first"
  echo "   Skipping remaining tests."
  exit 1
fi

for svc in redpanda simulator grafana; do
  if docker compose ps --format json 2>/dev/null | jq -e --arg n "$svc" 'select(.Name | endswith($n)) | select(.State == "running")' >/dev/null 2>&1; then
    pass "$svc container is running"
  else
    fail "$svc container is not running"
  fi
done

# --- Test 2: Kafka Events Actually Arrive ---
echo ""
echo "=== Test 2: Kafka Events Arrive ==="
TMPDIR=$(mktemp -d)
if docker compose exec -T redpanda rpk topic list 2>/dev/null | grep -q ecommerce-events; then
  pass "topic 'ecommerce-events' exists"
else
  fail "topic not found — check simulator logs"
fi

# Consume 5 events with timeout
if docker compose exec -T redpanda timeout 10 rpk topic consume ecommerce-events --num=5 2>/dev/null > "$TMPDIR/events.json"; then
  COUNT=$(jq -c 'select(.event_type != null)' "$TMPDIR/events.json" 2>/dev/null | wc -l)
  if [ "$COUNT" -ge 5 ]; then
    pass "consumed $COUNT events from topic"
    # Check event structure
    FIRST=$(jq -c 'select(.event_type != null)' "$TMPDIR/events.json" 2>/dev/null | head -1)
    if echo "$FIRST" | jq -e '.event_id and .timestamp and .session_id and .user_id and .event_type' >/dev/null 2>&1; then
      pass "event has all required fields"
    else
      fail "event missing required fields: $FIRST"
    fi
  else
    fail "expected ≥5 events, got $COUNT"
  fi
else
  fail "rpk consume failed"
fi

# --- Test 9: File Sink Validation ---
echo ""
echo "=== Test 9: File Sink ==="
# Get the file sink output from the simulator container
if docker compose exec -T simulator wc -l /var/log/sim/events.ndjson 2>/dev/null | awk '{print $1}' | grep -q '^[1-9]'; then
  LINECOUNT=$(docker compose exec -T simulator wc -l /var/log/sim/events.ndjson 2>/dev/null | awk '{print $1}')
  pass "file sink has $LINECOUNT lines"

  # Validate JSON with jq
  if docker compose exec -T simulator head -100 /var/log/sim/events.ndjson 2>/dev/null | jq . >/dev/null 2>&1; then
    pass "file sink events are valid JSON"
  else
    fail "file sink contains invalid JSON"
  fi
else
  fail "file sink empty or missing"
fi

# --- Test 3: State Machine Validity ---
echo ""
echo "=== Test 3: State Machine Validity ==="
# Dump events from file sink and validate transitions
docker compose exec -T simulator cat /var/log/sim/events.ndjson 2>/dev/null | bash scripts/validate-transitions.sh > "$TMPDIR/validation.txt" 2>&1 || true
if grep -q "ALL VALID" "$TMPDIR/validation.txt"; then
  pass "all state transitions are valid"
else
  cat "$TMPDIR/validation.txt"
  fail "invalid state transitions found"
fi

# --- Test 11: Grafana Visibility ---
echo ""
echo "=== Test 11: Grafana Visibility ==="
if curl -sf http://localhost:3000/api/health >/dev/null 2>&1; then
  pass "Grafana is accessible at http://localhost:3000"
  # Check datasource
  if curl -sf http://localhost:3000/api/datasources 2>/dev/null | jq -e '.[] | select(.name == "Prometheus")' >/dev/null 2>&1; then
    pass "Prometheus datasource provisioned in Grafana"
  else
    fail "Prometheus datasource not found in Grafana"
  fi
  # Check dashboard
  if curl -sf http://localhost:3000/api/search?query=E-Commerce 2>/dev/null | jq -e '.[] | select(.title == "E-Commerce Event Stream")' >/dev/null 2>&1; then
    pass "dashboard 'E-Commerce Event Stream' provisioned"
  else
    fail "dashboard not found in Grafana"
  fi
else
  fail "Grafana not accessible at localhost:3000"
fi

# --- Prometheus check ---
echo ""
echo "=== Prometheus Targets ==="
if curl -sf http://localhost:9090/api/v1/targets 2>/dev/null | jq -e '.data.activeTargets[] | select(.health == "up")' >/dev/null 2>&1; then
  pass "Prometheus target simulator:8080 is UP"
else
  fail "Prometheus target simulator:8080 is not UP — check http://localhost:9090/targets"
fi

# --- Summary ---
echo ""
echo "========================"
echo "Results: $PASS passed, $FAIL failed"
echo "========================"
[ "$FAIL" -eq 0 ] || exit 1
