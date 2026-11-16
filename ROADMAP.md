# Roadmap and open problems

These are problems we'd like help with. If one interests you, open an issue or a discussion, or email nishkaa@stanford.edu.

## Coverage

- **Non-English posts.** The library and live matching are English-only. Community Notes exist in many languages; matching across languages is open.
- **Claims in images and video.** Posts are judged on their text only, and notes that depend on media are dropped from the library. Many misleading posts are screenshots.
- **More note sources.** Other crowd-sourced or professional fact-checks could feed the same pipeline.

## Scale and cost

- **Beyond 10,000 followers.** Followers mode uses Jetstream's `wantedDids` filter, which is capped at 10,000 accounts.
- **Full-feed throughput.** In full mode most posts are dropped by the queue limit. Cheaper Stage 1 gates or batching embeddings would let more posts through.
- **Cheaper Stage 3.** A smaller or fine-tuned model for the applicability judgment, with precision kept at the paper's level.

## Evaluation

- **Live precision.** Annotate a sample of live matches to measure precision in deployment, and compare with the paper.
- **Effects of labels and replies.** How do people respond to a label or a reply, and does it change engagement with the post?
- **Drift.** Model aliases are not pinned; track how Stage 3 decisions change as the underlying model changes.

## Trust and safety

- **Appeals.** A lightweight way for authors to dispute a label and see the outcome.
- **Self-service opt-out.** Today exclusion is manual (`EXCLUDED_DIDS`); honour blocks or a reply command automatically.
- **Prompt injection.** Posts and notes are untrusted input to the LLM; test and harden against posts written to trigger or avoid a label.
