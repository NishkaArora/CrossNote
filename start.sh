#!/bin/bash

mkdir -p /data/model_cache

# Start Python pipeline in background
uvicorn pipeline.server:app --uds /tmp/pipeline.sock --log-level info &

# Start Node.js immediately so Fly's health check (GET /) passes right away.
# detect.ts handles connection errors gracefully until Python is ready.
npm start &

echo "Waiting for Python pipeline to initialize..."

MAX_WAIT=600
WAITED=0

until python3 -c "
import socket, http.client, sys
try:
    conn = http.client.HTTPConnection('localhost')
    conn.sock = socket.socket(socket.AF_UNIX)
    conn.sock.connect('/tmp/pipeline.sock')
    conn.request('GET', '/health')
    sys.exit(0 if b'true' in conn.getresponse().read() else 1)
except:
    sys.exit(1)
" 2>/dev/null; do
    sleep 5
    WAITED=$((WAITED + 5))
    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "ERROR: Python pipeline did not become ready after ${MAX_WAIT}s"
        exit 1
    fi
done

echo "Python pipeline ready."

# Keep script alive — if either process exits, Fly will restart the machine
wait
