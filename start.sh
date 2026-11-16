#!/bin/bash

mkdir -p "${DATA_DIR:-/data}/library"

# The Python matcher is only used by automatic labelling.
if [ "$DISABLE_AUTO_LABEL" != "1" ]; then
  # Restart the Python matcher automatically if it crashes.
  (while true; do
    uvicorn pipeline.crossnote_server:app --uds /tmp/pipeline.sock --log-level info --workers 1
    echo "[start.sh] Python matcher exited (code $?) — restarting in 3s..."
    sleep 3
  done) &
fi

# Restart the Node labeller too: if it dies, the label stream and the
# health check go down while the container keeps running.
(while true; do
  npm start
  echo "[start.sh] Node labeller exited (code $?) — restarting in 3s..."
  sleep 3
done) &

wait
