---
name: research-reporting
description: Use when producing EDA summaries, analyst notes, model explanations, data lineage summaries, or concise research reports from structured project data.
---

# research-reporting

Use this skill for analytical writing and report generation.

## Workflow

1. Start from structured data, saved metrics, or persisted artifacts before writing conclusions.
2. Include provenance: source tables, snapshot/model version, and relevant timestamps.
3. State missing data, weak coverage, and low-confidence cases explicitly.
4. Keep notebooks exploratory only; durable logic belongs in reusable modules.
5. Prefer concise analyst-ready output: what changed, why it matters, what is uncertain, and what to
   inspect next.

## Output rules

- Separate observed facts from inference.
- Do not imply signal where data coverage is thin.
- Keep recommendations aligned with the project’s non-goals: no automated live trading claims.
