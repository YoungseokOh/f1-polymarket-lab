# 2026 Multitask Walk-Forward Modeling (Q/R) — First Pass

_Generated 2026-06-22; revised after a pro/con review that surfaced and fixed
four correctness defects (see [Engineering fixes](#engineering-fixes-landed-with-this-pass)).
Stage: `multitask_qr` (shared-encoder multitask: pole / constructor_pole / winner / h2h heads)._

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

- **As-of snapshots** per GP at FP1 / FP2 / FP3 / Q checkpoints. A checkpoint that
  already observes the target session cannot predict it: the **Q (pole /
  constructor_pole) target is built only from the FP checkpoints**, never at the Q
  checkpoint (which would leak the label and produce an inverted entry window). The
  R (winner / h2h) target is never observed by any FP/Q checkpoint. Verified that
  `qualifying_position` is null for all pre-Q rows.
- **Time-aware walk-forward** ordered by earliest as-of timestamp (not meeting_key,
  which is negative/round-encoded for jolpica meetings and would otherwise reverse
  the season).
- **Labels join** across the jolpica/OpenF1 driver-id schemes by the FIA 3-letter
  acronym (e.g. `ANT`), which is stable across both schemes even when full_name
  differs ("Andrea Kimi Antonelli" vs "Kimi ANTONELLI"); it falls back to
  normalized name, then id slug. Rows whose matched driver still has no target
  result are **dropped, not labeled 0**, so no false negatives are fabricated.
  (Verified there are no acronym collisions among each session's participants.)
- Snapshots after fixes: ~52 rows per FP checkpoint (20 pole + 20 winner + 12 h2h),
  32 rows at the Q checkpoint (winner + h2h only).

### Walk-forward folds (out-of-sample)

| Fold | Train | Test | log_loss | Brier | ECE |
|------|-------|------|----------|-------|-----|
| 1 | Canadian | Monaco | **0.344** | **0.107** | 0.061 |
| 2 | Canadian + Monaco | Barcelona | **0.338** | **0.104** | 0.047 |

Both folds are genuinely out-of-sample and chronological. Fold 1 is the harder test
(a single sprint-weekend training GP against a Monaco that now carries a real pole
positive, recovered via the acronym join); Fold 2 — trained on two GPs whose
positives are now intact — is well-calibrated (ECE 0.047).

## Error analysis (Fold 2 — Barcelona, by market family)

| Family | rows | log_loss | Brier | Notes |
|--------|------|----------|-------|-------|
| pole | 60 | 0.205 | 0.048 | sharpest head; favorite-driven |
| winner | 80 | 0.201 | 0.048 | well-separated on the field |
| h2h | 48 | 0.731 | 0.268 | near coin-flip (≈ 0.25 baseline); weakest head |

The h2h head is the weakest (Brier ≈ 0.27) and is the priority for the next
iteration, consistent with the modeling order (FP2/FP3 head-to-head first).

## Executable-price backtest status

**Blocked for the three completed GPs; capture path now in place for upcoming
GPs.** The skill requires executable bid/ask (or order-book depth), not
midpoint-only evaluation. The historical price history is midpoint/last-price only:

- `polymarket_price_history`: 919,844 rows, of which **3,848 (0.4%)** carry
  `best_ask` and 3,582 carry `best_bid`. `price` is 100% populated.
- For the three modeling GPs, **0 of 451,806** in-window price points carry a
  positive `best_ask` — and their markets are already resolved, so this cannot be
  captured retroactively.

The trainer's paper PnL is now itself **gated on a positive executable `best_ask`**
(it previously priced bets at the midpoint `entry_yes_price`). With no ask quotes
available, both folds report **bet_count = 0 and roi_pct = None** — there is no
midpoint PnL anywhere in the output. The standalone `settle_backtest` path applies
the same gate (and currently expects the GP-registry snapshot schema).

`capture-live-weekend` now **persists the live CLOB best_bid/best_ask quote stream
into `polymarket_price_history`** (`source_kind="polymarket_ws"`), not just the WS
manifest, so future captures land executable quotes in the same table the snapshot
builder reads.

### To unblock

1. ✅ Live capture persists executable bid/ask to `polymarket_price_history`
   (`source_kind="polymarket_ws"`).
2. Schedule `capture-live-weekend` for the upcoming GP sessions (Austrian GP, 6/27
   onward), covering the pre-session entry windows (FP3→Q, Q→R).
3. Re-run the walk-forward backtest once a completed GP has both labels and
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

Landed after the pro/con review:

- **Q-checkpoint leakage removed**: Q-target families are no longer built at the Q
  checkpoint (the checkpoint that observes qualifying), eliminating both the inverted
  entry window and the trivially-leaked pole label.
- **No false-negative labels**: rows whose matched driver lacks a target result are
  dropped instead of emitted as `label_yes=0`.
- **Executable PnL gate**: the trainer prices paper bets at `best_ask` and skips rows
  without a positive ask, so midpoint-only PnL can no longer appear in metrics.
- **De-duplicated training path**: the CLI now delegates to
  `model_workflow.train_multitask_walk_forward` (and reuses its chronological
  ordering helper for plan-only), so the two implementations can no longer diverge.
- **Acronym-based driver identity**: the label join keys on the FIA acronym, which
  bridges the jolpica/OpenF1 name aliases that previously dropped rows (recovered
  Monaco's pole/winner positives and grew pole/winner to 20 rows/GP).

## Next steps

1. Live capture now persists executable quotes; schedule `capture-live-weekend`
   for upcoming GP sessions so a completed GP gains an executable entry window.
2. Strengthen the h2h head (richer FP2/FP3 pace deltas) once more GPs accumulate.
3. Extend coverage as more 2026 rounds complete with retained price history.
4. The Antonelli-style name-alias gap is resolved via the acronym join (positives
   recovered). A fuller driver-identity table (number + acronym + name) would
   harden the jolpica/OpenF1 mapping beyond modeling, for ingestion/reconcile too.
