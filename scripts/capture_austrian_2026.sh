#!/bin/bash
# Live CLOB bid/ask capture for the 2026 Austrian Grand Prix.
#
# capture-live-weekend self-waits until (session_start - start_buffer) and streams
# until (session_end + stop_buffer), persisting best_bid/best_ask into
# polymarket_price_history (source_kind="polymarket_ws"). Launched a little before
# each window by the launchd agents in scripts/launchd/.
#
# Usage: capture_austrian_2026.sh <session_key> <start_buffer_min> <stop_buffer_min>
#
# Requires: Docker Desktop running (local Postgres) and network access.
set -euo pipefail

SESSION_KEY="${1:?session_key required}"
START_BUFFER="${2:-90}"
STOP_BUFFER="${3:-20}"

# Only the 2026 running of this date is intended; launchd's calendar trigger would
# otherwise repeat every year. No-op on any other year.
if [ "$(date +%Y)" != "2026" ]; then
  echo "$(date -u +%FT%TZ) skip: not 2026" >&2
  exit 0
fi

REPO_DIR="/Users/ysoh/Projects/f1-polymarket-lab"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"
cd "$REPO_DIR"

LOG_DIR="$REPO_DIR/data/reports/operations/austrian-2026-capture"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/capture_${SESSION_KEY}_$(date -u +%Y%m%dT%H%M%SZ).log"

echo "$(date -u +%FT%TZ) starting capture session_key=$SESSION_KEY start_buffer=$START_BUFFER stop_buffer=$STOP_BUFFER" | tee -a "$LOG_FILE"

# '--' so the negative session_key is not parsed as an option flag.
uv run --package f1-polymarket-worker python -m f1_polymarket_worker.cli \
  capture-live-weekend \
  --execute \
  --start-buffer-min "$START_BUFFER" \
  --stop-buffer-min "$STOP_BUFFER" \
  -- "$SESSION_KEY" 2>&1 | tee -a "$LOG_FILE"

echo "$(date -u +%FT%TZ) capture finished session_key=$SESSION_KEY" | tee -a "$LOG_FILE"
