#!/bin/bash

mkdir -p /data/model_cache /data/library

# The Python pipeline is only used by automatic labelling.
if [ "$DISABLE_AUTO_LABEL" != "1" ]; then
  # Restart the Python pipeline automatically if it crashes.
  (while true; do
    uvicorn pipeline.crossnote_server:app --uds /tmp/pipeline.sock --log-level info --workers 1
    echo "[start.sh] Python pipeline exited — restarting in 3s..."
    sleep 3
  done) &
fi

npm start &

wait
