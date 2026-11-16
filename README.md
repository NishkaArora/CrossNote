# crossnote

crossnote is a Bluesky labeler that brings X's Community Notes to Bluesky. When a Bluesky post makes a claim that a Helpful Community Note already corrects, crossnote labels the post `misinformation` and replies with the note.

Paper: [*Community Notes Across Platforms: Evaluating the Feasibility of Re-using Crowd-sourced Context Notes*](paper/community-notes-across-platforms.pdf), Nishka Arora, Yann Issam Aouidef and Ashish Goel, Stanford University.

- **See labels:** subscribe to [@crossnote.bsky.social](https://bsky.app/profile/crossnote.bsky.social).
- **Have your posts checked:** follow the account.
- **See why a post was labeled:** paste its link into the [demo](https://nishkaarora.github.io/crossnote_bsky/).
- **Wrong label?** [Report it](../../issues/new?template=wrong_label.yml).

## How it works

1. **Note library.** Notes first rated Helpful on X in the past 7 days whose X post is in English, minus notes that only make sense for a particular author, image or video. Rebuilt periodically.
2. **Post filters.** Original and quote posts in English, longer than two words.
3. **Stage 1.** TF-IDF similarity to a note of at least 0.1376, then embedding similarity (text-embedding-3-large) of at least 0.5003.
4. **Stage 2.** The 3 most similar notes.
5. **Stage 3.** An LLM judges whether each note applies to the post; the highest-ranked applicable note is used.

In the paper's evaluation, 72.6% of the pipeline's matches on the live Bluesky stream were correct, and it found 70.1% of known-positive posts.

The deployment uses `gpt-6.1-sol` for Stage 3 and the library filters, where the paper used `gpt-5.6-sol` and `gpt-5.6`, and model versions aren't pinned. Its note library is a rolling 7-day window rather than the paper's fixed evaluation set. Thresholds and prompts are the ones the paper evaluated, but live precision may differ.

## Data

crossnote stores:

- For each labeled post: its `at://` link, the matched note, similarity scores and the LLM's reason.
- For posts that reach Stage 3 without a match: scores and note IDs.
- Answers submitted on the demo page.

## Collaborate

We're looking for collaborators in research, engineering and evaluation. See the [roadmap](ROADMAP.md) or email **nishkaa@stanford.edu**. Pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md).

## Cite

```bibtex
@misc{arora2026crossnote,
  author = {Arora, Nishka and Aouidef, Yann Issam and Goel, Ashish},
  title  = {Community Notes Across Platforms: Evaluating the Feasibility of Re-using Crowd-sourced Context Notes},
  year   = {2026},
  note   = {Preprint},
  url    = {https://github.com/NishkaArora/crossnote_bsky/blob/main/paper/community-notes-across-platforms.pdf}
}
```

## License

[MIT](LICENSE). Community Notes data and X content keep their own terms.
