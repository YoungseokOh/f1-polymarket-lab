# 2026 Chinese Grand Prix Subset Validation

## Provenance
- Validation command: `validate-f1-weekend-subset --meeting-key 1280 --season 2026 --validation-mode smoke --execute`
- Primary artifact: `data/reports/validation/2026/2026-chinese-grand-prix/summary.json`
- Artifact generated at: `2026-03-20T07:39:35.100737+00:00`
- Source scope:
  - OpenF1 weekend sessions for `FP1`, `SQ`, `S`, `Q`, `R`
  - Polymarket Chinese GP session-linked markets discovered and selectively hydrated

## Observed Facts
- Overall validator status was `completed` with `0` failures and `0` warnings.
- Session inventory matched the expected sprint-weekend shape:
  - `FP1` `11235`
  - `SQ` `11236`
  - `S` `11240`
  - `Q` `11241`
  - `R` `11245`
- Smoke-mode heavy hydration was applied only to `SQ`, `Q`, and `R`.
- F1 subset counts were sufficient for downstream analysis:
  - `FP1`: `results=22`, `laps=540`
  - `SQ`: `results=21`, `laps=257`, `telemetry_total=44`
  - `S`: `results=22`, `laps=399`, `intervals=9019`
  - `Q`: `results=22`, `laps=325`, `telemetry_total=44`
  - `R`: `results=22`, `laps=924`, `intervals=20663`, `telemetry_total=44`
- Polymarket discovery produced session-aligned candidate sets:
  - `FP1`: `20` practice fastest-lap mappings
  - `SQ`: `22` pole-position mappings
  - `S`: `27` sprint-winner mappings
  - `Q`: `39` pole-position mappings
  - `R`: `124` auto-mappings across winner, podium, H2H, fastest lap, safety car, red flag, and constructor scoring props
- Representative market probes hydrated successfully:
  - `pole`: `price_history=91`, `trades=106`
  - `race_head_to_head`: `price_history=165`, `trades=10`
  - `race_outcome`: `price_history=183`, `trades=211`

## Research Readiness
- `f1_subset_data`: `ready`
- `session_market_mapping`: `ready`
- `market_history_probe`: `ready`
- `analysis_joinability`: `ready`

These checks support a first-pass research workflow that joins:
- session metadata to official F1 results, laps, intervals, and telemetry manifests
- Chinese GP session mappings to discovered Polymarket markets
- selected race and qualifying markets to historical price and trade series

## Gaps And Cautions
- The validator artifact shows `orderbook_snapshots=0` and `open_interest=0` for the representative probes. This subset is still usable for outcome, price-path, and trade-flow analysis, but not for depth-of-book research.
- Global DQ still records `polymarket_ws_message_manifest` as failing because live websocket capture is outside this smoke validation scope.
- A real taxonomy edge case was found after the validator run: race winner questions could be misclassified as `driver_podium` when the market description mentioned the podium ceremony. The parser has since been fixed in code and covered by tests, but the saved validator artifact above predates that refresh.

## Interpretation
- The Chinese GP subset is strong enough for offline research on session-linked market structure, especially:
  - qualifying and sprint-qualifying pole markets
  - race winner and podium markets
  - race head-to-head markets
  - safety car and red flag props
- The remaining practical risk is classification drift when Polymarket wording changes. The new taxonomy regression should reduce that risk, but periodic spot checks on real event pages remain necessary.

## Next Checks
1. Re-run the Chinese GP subset validator after the taxonomy fix to refresh the persisted `market_probes` section.
2. Add one explicit acceptance check that a `winner` question never lands in `driver_podium`.
3. Build a lightweight feature snapshot from `Q` and `R` session outputs plus mapped Polymarket price histories.
