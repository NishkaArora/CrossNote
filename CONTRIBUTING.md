# Contributing to crossnote

Anything merged to `main` deploys to the live labeler, so:

- Open an issue before anything larger than a small fix.
- Changes to `configs/`, `prompts/`, `pipeline/` or the models change which posts get labeled. Show that precision doesn't drop, for example with hand-checked matches from a dry run.
- Never commit `.env`, keys, post text or library snapshots.
- CI runs `npm run typecheck` and a Python compile check on every pull request.

By taking part you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
