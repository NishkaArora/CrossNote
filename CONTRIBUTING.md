# Contributing to CrossNote

Thanks for helping. CrossNote is a live labeler: anything merged to `main` deploys to the running bot, which labels and replies to real Bluesky posts. That's why a few kinds of change need more than a code review (see [Changes that affect live labels](#changes-that-affect-live-labels)).

## Set up

You need Node 20, Python 3.11 and an OpenAI API key.

```bash
npm ci
python -m venv .venv && .venv/bin/pip install -r pipeline/requirements.txt
cp .env.example .env        # fill in OPENAI_API_KEY; keep CROSSNOTE_DRY_RUN=1
mkdir -p local-data
```

For a local run, `LABELER_DID` can be any `did:` string and `LABELER_SIGNING_KEY` any 32-byte hex key (`openssl rand -hex 32`). Leave `BSKY_PASSWORD` empty: dry run never posts.

## Build a small test library

Without an X API token, build a stub library from a random sample of notes:

```bash
python data/download_notes.py                       # → data/raw/<export-date>/
.venv/bin/python -m library.build --cn-dir data/raw/<export-date> --stub-x --sample 20
echo <snapshot-folder-name> > data/library/CURRENT  # the folder the build printed, ending in -STUB-sample20
```

Stub libraries have no X post text, so matching quality is poor, but every stage runs. The matcher refuses stub libraries unless `CROSSNOTE_ALLOW_STUB_LIBRARY=1`.

## Run in dry-run mode

```bash
set -a; source .env; set +a
export CROSSNOTE_ALLOW_STUB_LIBRARY=1 CROSSNOTE_MODE=full
./start.sh
```

`CROSSNOTE_MODE=full` reads the public Bluesky stream, so you see posts without needing followers. With `CROSSNOTE_DRY_RUN=1`, matches are written to `local-data/crossnote_matches.jsonl` and nothing is labelled or posted. `curl localhost:8080/stats` shows the counters.

## Checks

```bash
npm run typecheck
.venv/bin/python -m compileall -q pipeline library data
```

CI runs both on every pull request.

## Pull requests

- Open an issue first for anything larger than a small fix, so we can agree on the approach.
- Keep each pull request to one change, and describe how you tested it (dry run, sample library, etc.).
- Never commit `.env`, API keys, signing keys, Bluesky or X post text, or library snapshots.
- A maintainer reviews every pull request before merge.

## Changes that affect live labels

Changes to `configs/`, `prompts/`, the matching code in `pipeline/` or the models used change which posts get labelled. These need evidence that precision doesn't drop: for example, a dry-run log over a few thousand posts with matches checked by hand, or results on the paper's evaluation set. Say in the pull request what you measured.

## Conduct

By taking part you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
