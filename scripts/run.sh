#!/bin/bash
# Run tts-service and backend service in parallel
# Usage: ./scripts/run.sh

set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cleanup() {
    echo ""
    echo "Stopping services..."
    kill "$TTS_PID" "$BACKEND_PID" 2>/dev/null || true
    wait "$TTS_PID" "$BACKEND_PID" 2>/dev/null || true
    echo "All services stopped."
}
trap cleanup EXIT INT TERM

# Start infrastructure (postgres, rabbitmq) if not running
echo "Starting infrastructure (docker-compose)..."
docker compose -f "$ROOT_DIR/docker-compose.yml" up -d

# Start TTS RabbitMQ worker
echo "Starting TTS RabbitMQ worker..."
cd "$ROOT_DIR/tts-service"
uv run vieneu-rabbitmq &
TTS_PID=$!

# Start backend service
echo "Starting backend service..."
cd "$ROOT_DIR"
cargo run &
BACKEND_PID=$!

echo "All services started. Press Ctrl+C to stop."
wait "$TTS_PID" "$BACKEND_PID"
