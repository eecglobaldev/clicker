#!/usr/bin/env bash
# Run main.py with uv; restart on exit or crash. Intended for 24/7 operation.
#
# Environment variables:
#   RESTART_DELAY    - Seconds to wait between restarts (default: 10)
#   RUN_TIMEOUT_SEC  - Max seconds per run before restart (default: 18000 = 5 hours)
#   DISABLE_RESTART  - Set to "1" to disable auto-restart (useful for debugging)
#   NUM_WORKERS      - Number of parallel browser workers (default: 2)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Optional: delay between restarts (seconds) to avoid tight loops on repeated crashes
RESTART_DELAY="${RESTART_DELAY:-10}"

# Max runtime per run (5 hours); script is restarted after this even if still running
RUN_TIMEOUT_SEC="${RUN_TIMEOUT_SEC:-18000}"

# Handle Ctrl+C and termination signals gracefully
SHOULD_STOP=0

trap 'echo "[$(date -Iseconds)] Stopping..."; SHOULD_STOP=1; exit 0' INT TERM EXIT

while true; do
  if [[ "$SHOULD_STOP" -eq 1 ]]; then
    echo "[$(date -Iseconds)] Exiting loop."
    exit 0
  fi

  echo "[$(date -Iseconds)] Starting main.py (timeout ${RUN_TIMEOUT_SEC}s, workers ${NUM_WORKERS:-2})..."

  # Run with timeout; preserve exit code properly
  set +e
  timeout --signal=INT "$RUN_TIMEOUT_SEC" uv run main.py
  EXIT_CODE=$?
  set -e

  # Exit codes:
  #   0 = normal exit
  #   124 = timeout killed the process
  #   125 = timeout command failed
  #   126 = command not executable
  #   127 = command not found
  #   130 = process killed by Ctrl+C (SIGINT)
  #   137 = process killed by SIGKILL

  if [[ "$SHOULD_STOP" -eq 1 ]] || [[ "$DISABLE_RESTART" == "1" ]]; then
    echo "[$(date -Iseconds)] main.py exited (code $EXIT_CODE). Not restarting."
    exit 0
  fi

  if [[ $EXIT_CODE -eq 130 ]] || [[ $EXIT_CODE -eq 124 ]]; then
    echo "[$(date -Iseconds)] main.py interrupted by signal (code $EXIT_CODE). Not restarting."
    exit 0
  fi

  echo "[$(date -Iseconds)] main.py exited (code $EXIT_CODE). Restarting in ${RESTART_DELAY}s..."
  sleep "$RESTART_DELAY"
done
