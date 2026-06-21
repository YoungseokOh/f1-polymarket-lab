# 2026 Multitask Walk-Forward Modeling (Q/R) — First Pass

_Generated 2026-06-22. Stage: `multitask_qr` (shared-encoder multitask: pole / constructor_pole / winner / h2h heads)._

## Summary

A first walk-forward pass of the shared-encoder multitask model was trained and
evaluated out-of-sample on 2026 race weekends. The model produces calibrated
Q/R probabilities (pole, race winner, head-to-head) from as-of practice/qualifying
checkpoints. **Predictive evaluation is in place; an executable-price PnL backtest
is currently blocked by missing order-book (bid/ask) depth in the captured price
history** — see [Executable backtest status](#executable-price-backtest-status).

## Data scope and lineage

Modeling requires three things to overlap for a GP: F1 session **results**
(labels), linked Polymarket **markets**, and **price history within the as-of
entry window** (before the target session).

The 2026 season is represented twice in storage:

| Representation | meeting_key | Results | Pre-session price history |
|----------------|-------------|---------|---------------------------|
| OpenF1 canonical | `1279`–`1289` | Q only (early GPs); R missing | none (capture started Jun 2026) |
| jolpica synthetic | `-2026RR` (round-encoded) | Q **and** R | yes, for recent rounds |

Only the **jolpica synthetic** meetings carry results *and* pre-session price
history together, and only for the most recently completed rounds:

- `-202605` Canadian GP (R5, sprint weekend: FP1/Q only)
- `-202606` Monaco GP (R6)
- `-202607` Barcelona GP (R7)

Earlier rounds have results but no retained pre-session price series; upcoming
rounds (Austrian, British, Belgian) have price history but no results yet.

## Modeling setup

- **As-of snapshots** per GP at FP1 / FP2 / FP3 / Q checkpoints. Qualifying-derived
  features are only visible at the Q checkpoint — verified that `qualifying_position`
  is null for all pre-Q checkpoint rows (no leakage across the as-of boundary).
- **Time-aware walk-forward** ordered by earliest as-of timestamp (not meeting_key,
  which is negative/round-encoded for jolpica meetings and would otherwise reverse
  the season).
- Snapshots: 65 rows/checkpoint (22 pole + 22 h2h + 21 winner), labels join across
  the jolpica/OpenF1 driver-id schemes by normalized driver name.

### Walk-forward folds (out-of-sample)

| Fold | Train | Test | log_loss | Brier | ECE |
|------|-------|------|----------|-------|-----|
| 1 | Canadian | Monaco | **0.244** | **0.082** | 0.047 |
| 2 | Canadian + Monaco | Barcelona | **0.412** | **0.126** | 0.113 |

Fold 1 is well-calibrated; Fold 2 degrades, consistent with a 2-GP training set and
a single added weekend. Both folds are genuinely out-of-sample and chronological.

## Error analysis (Fold 2 — Barcelona, by market family)

| Family | rows | log_loss | Brier | ECE | Notes |
|--------|------|----------|-------|-----|-------|
| pole | 88 | 0.251 | 0.057 | 0.117 | overpredicts favorites (10–20% bucket: predicted 16% vs actual 5%) |
| winner | 84 | 0.270 | 0.063 | 0.132 | similar overconfidence on longshots |
| h2h | 88 | 0.706 | 0.256 | 0.091 | near coin-flip (predicted 36% vs actual 45%); weakest head |

The h2h head is the weakest (Brier ≈ 0.26, barely better than 0.25 baseline) and is
the priority for the next iteration, consistent with the modeling order (FP2/FP3
head-to-head first).

> Note: the trainer also reports a paper `roi_pct`/`bet_count` computed against
> `entry_yes_price` (midpoint). These are **midpoint-only** and are deliberately
> excluded from any PnL claim here — see below.

## Executable-price backtest status

**Blocked.** The skill requires executable bid/ask (or order-book depth), not
midpoint-only evaluation. Current price history is midpoint/last-price only:

- `polymarket_price_history`: 919,844 rows, of which **3,848 (0.4%)** carry
  `best_ask` and 3,582 carry `best_bid`. `price` is 100% populated.
- For the three modeling GPs, **0 of 451,806** in-window price points carry a
  positive `best_ask`.

Consequently `settle_backtest` (which correctly gates on a positive `best_ask` and
skips midpoint-only rows) places zero executable bets here — the guard behaves as
designed; the data simply lacks the quotes.

### To unblock

1. Capture CLOB order-book bid/ask (or top-of-book) alongside the midpoint series,
   especially in the pre-session entry window, for upcoming GPs.
2. Re-run the walk-forward backtest once a completed GP has both labels and
   captured executable quotes.
3. Adapt `_enrich_snapshot_probabilities` / `settle_backtest` to the multitask
   snapshot schema (it currently expects the GP-registry snapshot shape and raises
   `KeyError: 'row_id'`), or add a dedicated settler that joins `ModelPrediction`
   probabilities to snapshot `best_ask`.

## Engineering fixes landed with this pass

- **Driver-id unification**: results (jolpica slug ids, e.g. `driver:russell`) now
  join to market-matched drivers (OpenF1 numeric ids, e.g. `driver:63`) by
  normalized driver name; previously every label and FP feature was silently null.
- **Time-aware fold ordering**: walk-forward orders by as-of timestamp so negative
  round-encoded jolpica keys don't reverse the season.
- **Snapshot concat**: `vertical_relaxed` so all-null (Null-dtype) checkpoint columns
  promote to the numeric supertype instead of raising `SchemaError`.
- **Feature tensor**: cast features to `Float64` before `fill_null(0)` so all-null
  columns coerce to numeric.
- **MLflow logging**: sanitize metric/param names (calibration bucket keys like
  `0-10%` contain `%`, which MLflow rejects).

## Next steps

1. Capture executable quotes to enable the PnL backtest (above).
2. Strengthen the h2h head (richer FP2/FP3 pace deltas).
3. Extend coverage as more 2026 rounds complete with retained price history.
4. Resolve the Antonelli-style name-alias gap (jolpica "Andrea Kimi Antonelli" vs
   market "Kimi Antonelli") that drops one pole/winner label at Monaco.
