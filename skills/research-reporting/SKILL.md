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
## Polymarket 시장 조회

- 레이스별 오픈 마켓 전체 목록은 **`https://polymarket.com/sports/f1/props`** 에서 먼저 확인하라.
- Gamma API 단독 스캔은 주요 고유동성 마켓을 누락할 수 있다.
- 베팅 분석 시 negRisk 배치 구조 때문에 각 마켓 YES 가격 합이 100%를 초과할 수 있음에 유의한다.