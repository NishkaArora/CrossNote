#!/bin/bash
# Upload a library snapshot to the Fly volume and make it the live one.
#
#   library/upload.sh 2026-11-10
#
# Copies data/library/<name>/ to /data/library/<name>/ on the machine, then
# writes <name> to /data/library/CURRENT. The matcher checks CURRENT every
# LIBRARY_CHECK_SECONDS (default 300) and swaps the new snapshot in; no
# restart needed. Refuses STUB builds.
set -euo pipefail

APP="${FLY_APP:-crossnote-labeler}"
NAME="${1:?usage: library/upload.sh <snapshot-name>}"
SRC="$(cd "$(dirname "$0")/.." && pwd)/data/library/$NAME"
REMOTE="/data/library/$NAME"
FILES="notes.parquet embeddings.npy tfidf_vectorizer.pkl tfidf_matrix.pkl candidates.parquet manifest.json"

[ -d "$SRC" ] || { echo "No snapshot at $SRC"; exit 1; }
case "$NAME" in
  *STUB*) echo "Refusing to upload a stub build ($NAME)"; exit 1 ;;
esac
grep -q '"stub_x_posts": false' "$SRC/manifest.json" || { echo "manifest says this is a stub build"; exit 1; }

fly ssh console -a "$APP" -C "mkdir -p $REMOTE"
for f in $FILES; do
  echo "uploading $f"
  fly ssh console -a "$APP" -C "rm -f $REMOTE/$f"   # sftp put won't overwrite
  fly ssh sftp put -a "$APP" "$SRC/$f" "$REMOTE/$f"
done
fly ssh console -a "$APP" -C "sh -c 'echo $NAME > /data/library/CURRENT && cat /data/library/CURRENT'"
echo "Done. The matcher picks up $NAME within LIBRARY_CHECK_SECONDS."
