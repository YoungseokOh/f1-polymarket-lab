---
name: change-finalizer
description: Use when preparing a change for handoff, review, release notes, or commit drafting, especially when validation coverage and commit message quality matter.
---

# change-finalizer

Use this skill when wrapping up a change.

## Workflow

1. Identify the impacted surfaces: storage, worker, API, web, docs, tests.
2. Run the smallest relevant validation set first, then broader repo checks if the change spans
   multiple layers.
3. Summarize what changed, what was validated, and what remains risky or unverified.
4. Draft commit messages using the repository rule: `<type>: <subject>`.

## Commit rules

- No scope.
- Preferred types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `ci`, `build`, `perf`,
  `data`
- Use an imperative, concise subject with no trailing period.

## Final checks

- Do not claim tests passed unless they were actually run.
- Call out skipped validations or unresolved follow-ups explicitly.
