#!/usr/bin/env bash
set -euo pipefail

APP_NAME="${APP_NAME:-simplenote-sync}"
ENTRY_FILE="${ENTRY_FILE:-src/index.js}"
MAX_MEMORY="${MAX_MEMORY:-700M}"
LOGROTATE_MAX_SIZE="${LOGROTATE_MAX_SIZE:-20M}"
LOGROTATE_RETAIN="${LOGROTATE_RETAIN:-14}"
LOGROTATE_CRON="${LOGROTATE_CRON:-0 0 * * *}"

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

echo "[1/8] Checking runtime dependencies"
command -v node >/dev/null 2>&1 || { echo "node is required"; exit 1; }
command -v npm >/dev/null 2>&1 || { echo "npm is required"; exit 1; }

if [ ! -f ".env" ]; then
  echo ".env not found in $PROJECT_DIR"
  echo "Create .env before running this script."
  exit 1
fi

echo "[2/8] Installing project dependencies"
if [ -f "package-lock.json" ]; then
  npm ci
else
  npm install
fi

echo "[3/8] Installing pm2 globally if missing"
if ! command -v pm2 >/dev/null 2>&1; then
  npm install -g pm2
fi

echo "[4/8] Restarting app in pm2"
pm2 delete "$APP_NAME" >/dev/null 2>&1 || true
pm2 start "$ENTRY_FILE" --name "$APP_NAME" --time --max-memory-restart "$MAX_MEMORY"

echo "[5/8] Configuring startup with systemd"
if command -v sudo >/dev/null 2>&1; then
  sudo env PATH="$PATH" pm2 startup systemd -u "$USER" --hp "$HOME"
else
  pm2 startup systemd -u "$USER" --hp "$HOME"
fi

echo "[6/8] Enabling pm2 log rotation"
pm2 install pm2-logrotate >/dev/null 2>&1 || true
pm2 set pm2-logrotate:max_size "$LOGROTATE_MAX_SIZE"
pm2 set pm2-logrotate:retain "$LOGROTATE_RETAIN"
pm2 set pm2-logrotate:compress true
pm2 set pm2-logrotate:rotateInterval "$LOGROTATE_CRON"

echo "[7/8] Saving process list"
pm2 save

echo "[8/8] Final status"
pm2 status "$APP_NAME"

echo "Done. Common commands:"
echo "  pm2 logs $APP_NAME --lines 200"
echo "  pm2 restart $APP_NAME"
echo "  pm2 monit"
