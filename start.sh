#!/bin/bash

mkdir -p /data/model_cache

# The Python pipeline is only used by automatic labelling.
if [ "$DISABLE_AUTO_LABEL" != "1" ]; then
  # Restart the Python pipeline automatically if it crashes.
  (while true; do
    uvicorn pipeline.server:app --uds /tmp/pipeline.sock --log-level info --workers 2
    echo "[start.sh] Python pipeline exited — restarting in 3s..."
    sleep 3
  done) &
fi

npm start &

wait
