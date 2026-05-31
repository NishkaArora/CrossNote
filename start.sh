#!/bin/bash

mkdir -p /data/model_cache

# Restart the Python pipeline automatically if it crashes.
(while true; do
  uvicorn pipeline.server:app --uds /tmp/pipeline.sock --log-level info
  echo "[start.sh] Python pipeline exited — restarting in 3s..."
  sleep 3
done) &

npm start &

wait
