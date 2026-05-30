#!/bin/bash

mkdir -p /data/model_cache

uvicorn pipeline.server:app --uds /tmp/pipeline.sock --log-level info &
npm start &

wait
