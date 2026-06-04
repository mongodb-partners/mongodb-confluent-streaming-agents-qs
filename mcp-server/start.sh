#!/bin/sh
# Container entrypoint.
#
# Starts mongodb-mcp-server on port 8000 in the background, waits for the
# port to bind, then starts the Accept-header proxy on port 8080.
#
# Previously both processes were launched in parallel
# with no readiness check; ALB health probes during the ~2-5s startup
# window saw 502 ECONNREFUSED responses (returned by the proxy, with the
# very Content-Type bug the proxy exists to prevent).

set -e

# Start mongodb-mcp-server on :8000 in the background.
mongodb-mcp-server &
BACKEND_PID=$!

# Wait for the backend to bind 127.0.0.1:8000. Cap the wait at 30s so a
# broken backend doesn't hang the container forever.
for i in $(seq 1 60); do
  if nc -z 127.0.0.1 8000 2>/dev/null; then
    echo "[start] backend ready on :8000 (after $((i * 500))ms)"
    break
  fi
  sleep 0.5
done

if ! nc -z 127.0.0.1 8000 2>/dev/null; then
  echo "[start] backend did not bind :8000 within 30s — starting proxy anyway"
fi

# Now exec the proxy on :8080 (replaces this shell so PID 1 forwards signals).
exec node /home/mcp/proxy.mjs
