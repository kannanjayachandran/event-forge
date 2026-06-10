#!/usr/bin/env bash
# Test 3 — State Machine Validity
# Reads events.ndjson and validates every state transition
set -euo pipefail

FILE="${1:-/dev/stdin}"
ERRORS=0
COUNT=0

declare -A ALLOWED
ALLOWED[landing]="search"
ALLOWED[search]="product_view"
ALLOWED[product_view]="add_to_cart search"
ALLOWED[add_to_cart]="checkout product_view"
ALLOWED[checkout]="purchase add_to_cart"
# purchase is terminal — no transitions out

PREV=""
while IFS= read -r line; do
  # Validate JSON
  if ! echo "$line" | jq . >/dev/null 2>&1; then
    echo "INVALID JSON at line $((COUNT + 1))"
    ERRORS=$((ERRORS + 1))
    COUNT=$((COUNT + 1))
    continue
  fi

  ET=$(echo "$line" | jq -r '.event_type')
  COUNT=$((COUNT + 1))

  if [ -n "$PREV" ]; then
    # Skip transitions out of purchase (terminal → new session)
    if [ "$PREV" = "purchase" ]; then
      PREV="$ET"
      continue
    fi
    # Skip drop-off transitions (session ended, new session starting at landing)
    if [ "$ET" = "landing" ] && [ "$PREV" != "purchase" ]; then
      # Previous session ended (drop-off), new session starts — valid
      PREV="$ET"
      continue
    fi
    # Check allowed
    ALLOWED_LIST="${ALLOWED[$PREV]:-}"
    FOUND=0
    for a in $ALLOWED_LIST; do
      if [ "$a" = "$ET" ]; then
        FOUND=1
        break
      fi
    done
    if [ "$FOUND" -eq 0 ]; then
      echo "INVALID TRANSITION at line $COUNT: $PREV -> $ET"
      ERRORS=$((ERRORS + 1))
    fi
  fi
  PREV="$ET"
done < "$FILE"

echo "---"
echo "Events checked: $COUNT"
echo "Invalid transitions: $ERRORS"
if [ "$ERRORS" -gt 0 ]; then
  exit 1
fi
echo "ALL VALID"
