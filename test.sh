#!/usr/bin/env bash
set -e
echo "Running quick demo/test..."

# ensure fresh DB
rm -f queuectl.sqlite3

# install deps if missing (safe to call)
if [ ! -d node_modules ]; then
  echo "Installing node modules..."
  npm install --silent
fi

# Enqueue a success job and a failing job
echo "Enqueue success job..."
node queuectl.js enqueue "echo HelloQueueCTL"

echo "Enqueue failing job..."
node queuectl.js enqueue "bash -c 'exit 2'"

# start one worker in background (so this script continues)
echo "Starting 1 worker in background (will exit after finishing jobs)..."
node queuectl.js worker:start --count 1 &
WORKER_PID=$!

# wait a bit for processing
sleep 5

echo "Status:"
node queuectl.js status

echo "All jobs (list):"
node queuectl.js list

echo "DLQ:"
node queuectl.js dlq:list

# cleanup
echo "Stopping worker if still running..."
kill ${WORKER_PID} >/dev/null 2>&1 || true
echo "Demo complete."
