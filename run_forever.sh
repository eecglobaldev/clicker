#!/usr/bin/env bash
# Run main.py with uv; restart on exit or crash. Intended for 24/7 operation.
#
# The Python script loops internally over keyword cycles. This shell wrapper
# only exists to restart the process if it crashes or is killed by timeout.
#
# Environment variables:
#   RESTART_DELAY    - Seconds to wait between restarts (default: 10)
#   RUN_TIMEOUT_SEC  - Max seconds per run before restart (default: 18000 = 5 hours)
#   DISABLE_RESTART  - Set to "1" to disable auto-restart (useful for debugging)
#   NUM_WORKERS      - Number of parallel browser workers (default: 2)

# NOTE: no 'set -e' — it silently kills the loop on any non-zero command
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

RESTART_DELAY="${RESTART_DELAY:-10}"
RUN_TIMEOUT_SEC="${RUN_TIMEOUT_SEC:-18000}"

# Rapid-crash detection: if the process exits in under this many seconds,
# escalate the restart delay to avoid spin-looping.
RAPID_CRASH_THRESHOLD=30
RAPID_CRASH_MAX_DELAY=300
rapid_crash_count=0

# --- Signal handling ---
SHOULD_STOP=0

_cleanup() {
  echo "[$(date -Iseconds)] Stopping... (killing all processes)"
  SHOULD_STOP=1
  trap '' INT TERM HUP EXIT  # Prevent recursive trapping
  kill -TERM -$$ 2>/dev/null || true
  exit 0
}
trap _cleanup INT TERM HUP

# --- Kill orphan Chrome processes from previous crashed runs ---
_kill_orphan_browsers() {
  pkill -f 'gclicker_profile_' 2>/dev/null || true
}

echo "[$(date -Iseconds)] run_forever.sh started (timeout=${RUN_TIMEOUT_SEC}s, delay=${RESTART_DELAY}s)"
_kill_orphan_browsers

while true; do
  if [[ "$SHOULD_STOP" -eq 1 ]]; then
    echo "[$(date -Iseconds)] Exiting loop."
    exit 0
  fi

  echo "[$(date -Iseconds)] Starting main.py (timeout ${RUN_TIMEOUT_SEC}s, workers ${NUM_WORKERS:-2})..."
  START_TIME=$(date +%s)

  # --kill-after=60: if SIGINT doesn't kill the process within 60s, send SIGKILL
  timeout --foreground --signal=INT --kill-after=60 "$RUN_TIMEOUT_SEC" uv run main.py
  EXIT_CODE=$?

  END_TIME=$(date +%s)
  RUNTIME=$((END_TIME - START_TIME))

  # Exit codes:
  #   0   = normal exit (all keywords done or clean shutdown)
  #   124 = timeout killed the process (5-hour cycle)
  #   125 = timeout command itself failed
  #   126 = command not executable
  #   127 = command not found
  #   130 = SIGINT (Ctrl+C)
  #   137 = SIGKILL (from --kill-after)

  # --- Stop conditions (only explicit user action or DISABLE_RESTART) ---
  if [[ "$SHOULD_STOP" -eq 1 ]] || [[ "$DISABLE_RESTART" == "1" ]]; then
    echo "[$(date -Iseconds)] main.py exited (code $EXIT_CODE). Not restarting."
    exit 0
  fi

  if [[ $EXIT_CODE -eq 130 ]]; then
    echo "[$(date -Iseconds)] Interrupted by Ctrl+C (code 130). Not restarting."
    exit 0
  fi

  # --- Fatal config errors: don't spin-loop on these ---
  if [[ $EXIT_CODE -eq 126 ]] || [[ $EXIT_CODE -eq 127 ]]; then
    echo "[$(date -Iseconds)] FATAL: command not found/executable (code $EXIT_CODE). Stopping."
    exit "$EXIT_CODE"
  fi

  # --- Timeout (5-hour cycle): restart immediately, reset crash counter ---
  if [[ $EXIT_CODE -eq 124 ]] || [[ $EXIT_CODE -eq 137 ]]; then
    echo "[$(date -Iseconds)] Timed out after ${RUN_TIMEOUT_SEC}s (code $EXIT_CODE, ran ${RUNTIME}s). Restarting..."
    rapid_crash_count=0
    _kill_orphan_browsers
    continue
  fi

  # --- Normal exit: restart (Python handles cycling internally) ---
  # If it ran long enough, treat it as a normal restart
  if [[ $RUNTIME -ge $RAPID_CRASH_THRESHOLD ]]; then
    rapid_crash_count=0
    echo "[$(date -Iseconds)] main.py exited (code $EXIT_CODE, ran ${RUNTIME}s). Restarting in ${RESTART_DELAY}s..."
    _kill_orphan_browsers
    sleep "$RESTART_DELAY" || true
    continue
  fi

  # --- Rapid crash: escalate delay to avoid spin-loop ---
  rapid_crash_count=$((rapid_crash_count + 1))
  backoff=$((RESTART_DELAY * rapid_crash_count))
  if [[ $backoff -gt $RAPID_CRASH_MAX_DELAY ]]; then
    backoff=$RAPID_CRASH_MAX_DELAY
  fi
  echo "[$(date -Iseconds)] RAPID CRASH #${rapid_crash_count} (code $EXIT_CODE, ran ${RUNTIME}s). Waiting ${backoff}s..."
  _kill_orphan_browsers
  sleep "$backoff" || true
done
