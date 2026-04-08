# Signal Ensemble Layer

This repository now supports a reusable signal-combination layer that sits on top of the existing as-of feature snapshots.

## Repo locations

- Python model math: `py/models/src/f1_polymarket_lab/models/signal_ensemble.py`
- Worker orchestration and CLI: `apps/worker/src/f1_polymarket_worker/signal_ensemble.py`
- Storage models: `py/storage/src/f1_polymarket_lab/storage/models.py`
- API schemas/routes: `apps/api/src/f1_polymarket_api/schemas.py`, `apps/api/src/f1_polymarket_api/api/v1/routes.py`
- Web surfaces:
  - `apps/web/src/app/predictions/page.tsx`
  - `apps/web/src/app/markets/[marketId]/page.tsx`
  - `apps/web/src/app/backtest/page.tsx`

## Local commands

- Register the default registry rows:
  - `uv run python -m f1_polymarket_worker.cli register-default-signals --execute`
- Train on stored snapshots:
  - `uv run python -m f1_polymarket_worker.cli train-signal-ensemble --snapshot-ids <id1,id2,id3> --execute`
- Score a stored snapshot:
  - `uv run python -m f1_polymarket_worker.cli score-signal-ensemble-snapshot --snapshot-id <id> --model-run-id <run_id> --execute`
- Run the ensemble backtest path:
  - `uv run python -m f1_polymarket_worker.cli run-signal-ensemble-backtest --snapshot-id <id> --model-run-id <run_id> --execute`

For local API development without a running Postgres instance:

- `DATABASE_URL_OVERRIDE=sqlite+pysqlite:///./data/lab.db make api`
- `make web`

## Modeling flow

The flow is:

1. load as-of snapshot rows
2. generate weak signal probabilities
3. calibrate each signal with grouped out-of-fold logic
4. convert each calibrated signal into a delta-logit versus the market anchor
5. fit a ridge logistic stacker with the market logit as an offset
6. emit final ensemble probabilities and executable trade decisions

The market anchor is the snapshot executable YES reference:

- `p_market_ref = entry_yes_price` when available
- midpoint remains diagnostic only unless no executable price exists

The final ensemble follows:

- `z_market = logit(p_market_ref)`
- `delta_i = logit(p_yes_calibrated_i) - z_market`
- `z_ensemble = z_market + intercept + sum_i(w_i * delta_i)`
- `p_yes_ensemble = sigmoid(z_ensemble)`

## Market-type applicability

The signal platform is generic across the repo taxonomy, but each signal declares applicability by market group.

Current market groups:

- `driver_outright`
- `constructor_outright`
- `head_to_head`
- `incident_binary`
- `championship`
- `other`

Signals are routed by market group instead of hard-coding one practice or H2H path. Unsupported scopes are stored as diagnostics and blocked from executable trade decisions.

## Calibration fallback

Calibration is hierarchical and explicit:

1. taxonomy-specific calibrator if enough rows exist
2. market-group calibrator if taxonomy is too sparse
3. global calibrator inside the stored run if the group is still sparse
4. raw probability fallback if no calibrator can be fitted

Method choice is data-dependent:

- isotonic when the bucket is large enough
- Platt scaling when the bucket is medium-sized
- raw fallback when both are too sparse

## Leakage control

OOF calibration and stacking use grouped temporal folds.

- Primary group: `meeting_key`
- Fallback group for sparse cases: `event_id`

Signals are calibrated on past groups and scored on held-out groups. The stacker is fitted on these OOF calibrated signal outputs instead of in-sample fitted probabilities.

## Diagnostics

Stored diagnostics include:

- per-signal Brier / log loss / ECE
- skill versus market anchor
- coverage rate
- residual correlation matrix
- disagreement score
- effective-N estimate
- uncertainty score

## Extending to a new signal or market type

1. Add a new `SignalDefinition` in `signal_ensemble.py`
2. Add a signal builder to `_SIGNAL_BUILDERS`
3. Declare applicability by market group or taxonomy
4. Re-train the ensemble

The storage, API, and UI layers are already generic, so new signals do not require schema changes unless they need brand-new raw snapshot inputs.
