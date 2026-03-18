---
name: f1-modeling-backtest
description: Use when building features, snapshots, experiments, models, calibration, evaluation, or backtests for the F1 prediction-market research stack.
---

# f1-modeling-backtest

Use this skill for feature engineering, modeling, and backtesting work.

## Hard constraints

- Follow the modeling order exactly:
  1. FP2 / FP3 head-to-head
  2. constructor / team fastest lap
  3. driver outright fastest lap
  4. red flag / safety car
- Every predictive dataset must be built from an explicit as-of snapshot.
- Use time-aware walk-forward splits only.
- Backtests must use executable bid/ask or orderbook depth, not midpoint-only evaluation.
- The agent layer explains and retrieves results; it is not the primary forecaster.

## Workflow

1. Inspect existing feature registry, model stage definitions, experiments, and storage contracts.
2. Add or update features only if they can be reproduced from saved cutoffs.
3. Persist dataset version, config, seed, metrics, and calibration artifacts together.
4. Prefer simpler baselines before more complex ML or DL models, and justify any added complexity.
5. Include error analysis by season, circuit, team, and market/liquidity bucket.

## Validation

- Add or update tests for snapshot leakage boundaries and backtest execution rules.
- Run the relevant Python checks and any experiment smoke tests tied to the changed stage.
