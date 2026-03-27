# Multi-Checkpoint Q/R Autoresearch Design

Date: 2026-03-27
Status: Approved for planning

## Goal

Build an offline research system that converts practice information into qualifying and race betting
signals, evaluates those signals with executable-price backtests, and uses an `autoresearch`-style
loop to improve model expected value over time.

The target is not general forecasting quality in isolation. The primary objective is higher
out-of-sample expected value under realistic execution assumptions.

## Scope

### In scope

- A unified snapshot pipeline that emits point-in-time rows for:
  - qualifying `driver_pole_position`
  - qualifying `constructor_pole_position`
  - race `race_winner`
  - race `head_to_head_session`
- Multiple as-of checkpoints per GP weekend:
  - `FP1`
  - `FP2`
  - `FP3`
  - `Q`
- A shared-encoder, multi-head model that predicts all four market families.
- Walk-forward training and evaluation using only point-in-time available information.
- Executable-price backtests with stateful position management across checkpoints.
- An `autoresearch`-inspired experiment loop that keeps evaluation rules fixed while exploring
  model and feature configurations.
- Structured experiment logging, ranking, and promotion from fast screening to full validation.

### Out of scope for v1

- Paper trading integration
- API and web UI exposure
- Live trading or automated order placement
- Rewriting the existing storage model or market ingestion architecture
- Search over snapshot cutoffs, split logic, or backtest scoring rules

## Design Principles

1. Keep the evaluation harness fixed.
2. Treat point-in-time reproducibility as a hard constraint.
3. Optimize for expected value, not only probability calibration.
4. Prefer a narrow research mutation surface over full-pipeline self-modification.
5. Make failure analysis easy by preserving family-level and checkpoint-level metrics.

## Why This Differs From The Current Stack

The current repository already has:

- snapshot generation for quick tests
- baseline probability evaluation
- walk-forward model harnesses for XGBoost and LightGBM
- executable backtests with realized PnL metrics

What it does not yet have is:

- one unified snapshot schema spanning qualifying and race markets
- a shared representation across market families
- checkpoint-by-checkpoint stateful signal updates
- an `autoresearch`-style loop that can repeatedly mutate the model within a fixed evaluation frame

## Approaches Considered

### 1. Search only over configs

Keep the model architecture mostly fixed and explore feature gates, loss weights, and
hyperparameters.

Pros:

- safest
- easiest to compare
- lowest implementation complexity

Cons:

- limits upside
- does not fully move the project toward a shared multi-head model

### 2. Search over model internals with fixed evaluator

Use a shared encoder and four family heads, while keeping snapshot cutoffs, walk-forward splitting,
and executable backtesting fixed.

Pros:

- supports long-term architecture direction
- still preserves comparability across experiments
- large enough search space to discover real improvements

Cons:

- more implementation work than config-only search
- requires new trainer and backtest extensions

### 3. Search over the full pipeline

Allow the experiment loop to modify snapshot logic, splitting, calibration, and backtest rules.

Pros:

- maximum freedom

Cons:

- high leakage risk
- weak comparability
- difficult failure attribution
- likely to overfit the evaluator

### Recommendation

Use approach 2.

The fixed evaluator is the control surface that makes overnight research useful. The experiment loop
must be able to say that a new result is better because the model improved, not because the scoring
rules moved.

## Architecture Overview

The v1 system has five major parts:

1. Multi-checkpoint snapshot builder
2. Shared-encoder multi-head model trainer
3. Head-wise calibration and walk-forward evaluator
4. Stateful executable backtest engine
5. `Autoresearch`-style experiment orchestrator

The data flow is:

1. Build point-in-time candidate rows for each GP and checkpoint.
2. Train the model with walk-forward splits.
3. Generate calibrated predictions per checkpoint and market family.
4. Convert predictions into checkpoint-level state transitions on positions.
5. Score realized PnL and supporting diagnostics.
6. Rank the experiment and decide whether to keep or discard it.

## Snapshot Design

### Row definition

Each row represents one candidate tradable market outcome at one point in time.

Core keys:

- `season`
- `meeting_key`
- `event_id`
- `market_id`
- `token_id`
- `target_session_code`
- `target_market_family`
- `as_of_checkpoint`
- `as_of_ts`

### Checkpoints

The builder emits one snapshot per GP weekend for each checkpoint:

- `FP1`
- `FP2`
- `FP3`
- `Q`

Checkpoint semantics:

- `FP1`: only information available before FP2
- `FP2`: FP1 plus FP2 information only
- `FP3`: FP1 plus FP2 plus FP3 information only
- `Q`: all practice plus qualifying information; used for race-market refreshes

### Market families

The unified snapshot supports four heads:

- `pole`
- `constructor_pole`
- `winner`
- `h2h`

Each family is stored in the same row schema, with family-specific columns present when relevant.

### Common features

- market state:
  - `entry_yes_price`
  - `entry_best_bid`
  - `entry_best_ask`
  - `entry_spread`
  - `entry_midpoint`
  - `trade_count_pre_entry`
  - `last_trade_age_seconds`
  - liquidity bucket
- weekend context:
  - circuit identifier
  - sprint-weekend indicator
  - season
  - round number
- practice summaries:
  - position by session
  - gap to leader by session
  - teammate gap by session
  - lap count by session
  - stint count by session
  - session best time proxy
  - momentum or improvement deltas across completed sessions
- availability mask:
  - `has_fp1`
  - `has_fp2`
  - `has_fp3`
  - `has_q`

### Family-specific features

#### Pole

- driver pace rank by completed practice sessions
- market-implied rank within the event
- best recent single-lap pace proxy
- session-to-session improvement trend

#### Constructor pole

- top driver pace within team
- second driver support within team
- within-team variance
- team consistency over completed sessions

#### Winner

- practice pace trend
- long-run or stint-derived pace proxies when available
- qualifying-derived position and gap features only at `Q`
- market structure features across winner contenders

#### H2H

- driver A vs driver B pace deltas across completed sessions
- teammate indicator
- qualifying delta at `Q`
- relative market price and liquidity between outcomes

### Labels

All heads are modeled as binary YES probabilities at the token level.

- `pole`: token wins pole or not
- `constructor_pole`: constructor secures pole or not
- `winner`: driver wins race or not
- `h2h`: token outcome finishes ahead or not

### Leakage controls

- No future sessions may contribute to a snapshot.
- Race snapshots before `Q` must not include qualifying-derived fields.
- Calibration is fit only on the training portion of each fold.
- Threshold selection and backtest policy parameters are fixed outside experiment mutation.

## Model Design

### Core structure

Use a shared encoder plus four family heads.

- shared encoder:
  - tabular MLP or gated residual MLP
  - consumes common features, family-specific features, and availability masks
- head outputs:
  - `pole`
  - `constructor_pole`
  - `winner`
  - `h2h`

The first model version should prefer a fast tabular network over a deeper or more exotic model.
The main reason is iteration speed. The experiment loop only works if the model can be retrained
often enough to compare many variants.

### Training objective

- base loss: binary cross-entropy per head
- multi-task aggregation: weighted sum of head losses
- weights: tunable by experiment config

Class imbalance must be addressed explicitly with per-head weighting or sampling controls, but the
exact tradeoff remains part of the model search space.

### Calibration

Calibration is head-specific and fold-local.

Acceptable v1 choices:

- isotonic regression
- Platt scaling
- identity calibration when a head lacks enough support

Calibration choice is a valid mutation target for the experiment loop. Calibration training data is
not.

## Training And Validation Design

### Splitting

Use time-aware walk-forward splits only.

For each test GP:

- train on all prior GPs
- validate on the most recent group within the training window when needed
- test on the held-out GP only

This must remain fixed across experiments.

### Fast screening versus promotion

The experiment loop runs in two phases.

#### Screening

- use a smaller subset of GPs and folds
- reject crashes and obviously weak candidates quickly
- rank by EV-oriented score with diagnostic guardrails

Screening promotion gate for v1:

- no crash
- positive `total_pnl`
- positive `roi_pct`
- at least `10` simulated bets across screened folds
- no single family contributes more than `80%` of screened PnL

#### Promotion

- run full walk-forward training and executable backtest on the complete study window
- persist full artifacts and diagnostics
- update experiment leaderboard

This preserves the spirit of `autoresearch` without allowing cheap wins from partial evaluation to
be mistaken for real progress.

## Backtest Design

### Why existing backtesting is not enough

The current backtester settles one snapshot into realized positions. The new design requires
checkpoint-aware state transitions for the same market across the same GP weekend.

### Stateful policy

At each checkpoint, each tracked market may receive one of:

- `open`
- `add`
- `reduce`
- `close`
- `hold`

The policy compares:

- current calibrated probability
- current executable price
- previous position state
- fixed decision thresholds

The thresholds are part of the evaluation harness and should stay fixed during v1 automated search.

### Execution assumptions

- executable entry uses best ask when available
- fallback uses spread-derived estimate or midpoint
- fees remain included
- realized PnL is computed from actual market outcomes and logged at the market level

### Primary objective

The main business objective is higher out-of-sample expected value. In practice v1 will optimize an
offline proxy built from realized backtest outcomes.

Primary reporting metrics:

- `total_pnl`
- `roi_pct`
- `bet_count`
- `hit_rate`
- `average_edge`
- `sharpe`

### Ranking score

Incumbent selection uses the following fixed priority order on full-validation results:

1. higher `total_pnl`
2. higher `roi_pct`
3. lower standard deviation of fold-level PnL
4. lower `expected_calibration_error`

Eligibility gate for a candidate to replace the incumbent:

- `bet_count >= 25`
- `roi_pct > 0`
- no single family contributes more than `75%` of total PnL

This ranking is preferred over pure `log_loss` because the user goal is expected value, not only
probability estimation quality. The thresholds above remain fixed throughout a research run unless a
human explicitly resets the experiment program.

### Supporting diagnostics

- `log_loss`
- `brier_score`
- `expected_calibration_error`
- family-level bet counts and PnL
- checkpoint uplift from `FP1 -> FP2 -> FP3 -> Q`
- season, circuit, and liquidity-bucket breakdowns

An experiment that improves log loss but reduces executable EV should not advance.

## Autoresearch Loop Design

### Fixed versus mutable surfaces

#### Fixed

- snapshot cutoff rules
- walk-forward splitting logic
- executable evaluator
- stateful backtest policy definition

#### Mutable

- shared encoder structure
- head structure
- feature-group enablement
- loss weights
- calibration choice
- optimizer and hyperparameters
- search-space configuration

This is the key adaptation from `karpathy/autoresearch`. The model can evolve, but the experiment
judge must remain stable.

### Experiment loop

For each iteration:

1. Load current best experiment state.
2. Propose a bounded model or config mutation.
3. Run screening evaluation.
4. Record crash or metric summary.
5. Promote strong candidates to full validation.
6. Keep or discard based on fixed ranking logic.
7. Persist structured run metadata and artifacts.

### Logging

Use structured experiment storage instead of a plain TSV:

- experiment id
- parent experiment id
- model family version
- snapshot dataset version
- mutable config
- screening metrics
- promoted metrics
- keep or discard decision
- artifact paths

This complements the existing `ModelRun`, `ModelPrediction`, and backtest tables instead of
replacing them.

## Failure Modes And Mitigations

### Leakage through snapshot generation

Mitigation:

- add explicit tests for checkpoint visibility boundaries
- persist checkpoint metadata in artifacts

### Overfitting to one family

Mitigation:

- record family-level metrics
- penalize concentration in ranking

### False improvements from tiny trade counts

Mitigation:

- require minimum bet support for promotion
- include bet-count penalty in the ranking score

### Research loop thrashing on broken experiments

Mitigation:

- narrow mutation surface
- fast crash detection in screening phase
- promote only stable candidates

## Experiment Plan

### Phase 1: Build the dataset and offline evaluator

- implement multi-checkpoint Q/R snapshot generation
- add family-aware row schema and labels
- extend backtest support for checkpoint state transitions
- verify leakage boundaries and execution assumptions with tests

### Phase 2: Build the shared multi-head baseline

- implement the initial shared encoder and four heads
- train with walk-forward splits
- calibrate per head
- compare against current baselines on the same folds

### Phase 3: Add the autoresearch loop

- define bounded mutable config space
- implement screening and promotion workflow
- persist experiment leaderboard and artifacts
- run repeated controlled searches

### Phase 4: Analyze uplift and prepare for paper trading

- inspect checkpoint uplift
- inspect family-level EV contribution
- identify the strongest promotion candidates for subsequent paper-trading integration

## Success Criteria

The design is successful when all of the following are true:

- snapshots are reproducible from saved as-of cutoffs
- walk-forward training covers all four target families
- backtests evaluate checkpoint state transitions with executable pricing
- experiment search can run repeatedly without modifying the evaluator
- at least one promoted candidate beats the current baseline on out-of-sample EV-oriented ranking
- diagnostics make it clear which family, checkpoint, and market conditions drive gains or losses

## Non-Goals Reaffirmed

This design does not attempt to:

- deploy live trading
- automate UI workflows
- turn the agent layer into the primary forecaster
- optimize only for classifier loss without regard to market execution
