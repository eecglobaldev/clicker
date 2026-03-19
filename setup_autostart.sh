#!/usr/bin/env bash
# Setup google-clicker as a systemd user service that starts on graphical login.
#
# Usage:
#   chmod +x setup_autostart.sh
#   ./setup_autostart.sh
#
# After running, manage with:
#   systemctl --user status google-clicker
#   systemctl --user stop google-clicker
#   systemctl --user restart google-clicker
#   journalctl --user -u google-clicker -f

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="google-clicker"
SERVICE_DIR="$HOME/.config/systemd/user"
SERVICE_FILE="$SERVICE_DIR/${SERVICE_NAME}.service"
USERNAME="$(whoami)"
UV_PATH="$(command -v uv 2>/dev/null)"

echo "=== Google Clicker Autostart Setup ==="
echo ""

# --- Preflight checks ---
if [[ -z "$UV_PATH" ]]; then
  echo "ERROR: 'uv' not found in PATH. Install it first:"
  echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
  exit 1
fi

if ! command -v google-chrome &>/dev/null && ! command -v chromium-browser &>/dev/null && ! command -v chromium &>/dev/null; then
  echo "WARNING: No Chrome/Chromium found. The bot needs a browser to run."
  echo "  Install with: sudo apt install -y google-chrome-stable"
  echo ""
fi

if [[ ! -f "$SCRIPT_DIR/run_forever.sh" ]]; then
  echo "ERROR: run_forever.sh not found in $SCRIPT_DIR"
  exit 1
fi

if [[ ! -f "$SCRIPT_DIR/main.py" ]]; then
  echo "ERROR: main.py not found in $SCRIPT_DIR"
  exit 1
fi

chmod +x "$SCRIPT_DIR/run_forever.sh"

# --- Create service file ---
mkdir -p "$SERVICE_DIR"

cat > "$SERVICE_FILE" << SERVICEEOF
[Unit]
Description=Google Clicker automation bot
After=graphical-session.target
PartOf=graphical-session.target

[Service]
Type=exec
WorkingDirectory=$SCRIPT_DIR
ExecStart=$SCRIPT_DIR/run_forever.sh
Restart=on-failure
RestartSec=30
Environment=DISPLAY=:0
Environment=PATH=$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin
Environment=NUM_WORKERS=2

KillMode=control-group
KillSignal=SIGINT
TimeoutStopSec=120

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=graphical-session.target
SERVICEEOF

echo "Created service file: $SERVICE_FILE"

# --- Enable autostart ---
systemctl --user daemon-reload
systemctl --user enable "$SERVICE_NAME.service"

echo ""
echo "=== Done ==="
echo ""
echo "Service is ENABLED and will auto-start on next login."
echo ""
echo "To start it now:  systemctl --user start $SERVICE_NAME"
echo ""
echo "Other commands:"
echo "  Status:   systemctl --user status $SERVICE_NAME"
echo "  Logs:     journalctl --user -u $SERVICE_NAME -f"
echo "  Stop:     systemctl --user stop $SERVICE_NAME"
echo "  Restart:  systemctl --user restart $SERVICE_NAME"
echo "  Disable:  systemctl --user disable $SERVICE_NAME"
