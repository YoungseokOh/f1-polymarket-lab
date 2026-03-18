# Commit Convention

This repository uses a simple header format:

`<type>: <subject>`

Rules:

- Do not use a scope.
- Keep the subject imperative and concise.
- Do not end the subject with a period.
- Prefer lowercase unless a proper noun or identifier needs case preservation.
- Keep the first line focused on the user-visible or maintenance-relevant change.

Preferred types:

- `feat`: new functionality
- `fix`: bug fix or regression fix
- `refactor`: structural change without intended behavior change
- `docs`: documentation-only change
- `test`: test-only change
- `chore`: repository maintenance or housekeeping
- `ci`: CI or automation changes
- `build`: build, dependency, packaging, or tooling pipeline changes
- `perf`: performance improvement
- `data`: schema, ingestion, migration, or dataset maintenance changes

Examples:

- `feat: add polymarket market hydration jobs`
- `fix: correct session mapping confidence threshold`
- `refactor: split worker ingestion pipeline by source`
- `docs: add shared agent skills guide`
- `data: add ingestion cursor and manifest tables`
