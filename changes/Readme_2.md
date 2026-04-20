# PR #2 — Scope reduction: bootstrap-only watchdog, minimal schema

**Branch:** `dev`
**Date:** 2026-04-19

## Summary

Rolled back over-eager initial scope. The system now starts cleanly,
runs bootstrap (coins + leverage + DB schema), and exits. Nothing more.
Processes and tables are added incrementally, one PR at a time.

## Changes

### `00_main_watchdog.py`
- `PROCESSES` list is now empty — commented example shows the pattern
- Watchdog exits cleanly after bootstrap when no processes are configured
- Monitor loop only runs when processes are actually registered
- All process management code retained for future use

### `core/schema.py`
- Removed all trade tracking tables (`active_trades_master`,
  `closed_trades_master`, `ai_signals`, `closed_ai_signals`, etc.)
- Removed all regime / orchestrator tables
- Removed `ml_predictions_master`, `master_ai_processed_signals`
- **Kept:** OHLCV tables (16 timeframes), indicator tables (6 timeframes),
  `telegram_outbox`, `trade_cooldowns`
- Added doc comment explaining what is NOT here yet and why
- `verify_schema()` updated to match reduced table set

## Tables created on startup

| Group | Tables |
|-------|--------|
| OHLCV | `ohlcv_10s` … `ohlcv_1M` (16 total) |
| Indicators | `indicators_30m` … `indicators_1w` (6 total) |
| Infrastructure | `telegram_outbox`, `trade_cooldowns` |

## Tables NOT yet created (pending data model design)

- Trade tracking (single unified table — model TBD)
- AI / ML signal tracking (model TBD)
- Regime / orchestrator tables (model TBD)

## Next steps

| PR | Scope |
|----|-------|
| #3 | `10_data_ingestion.py` — WebSocket fleet + REST catch-up |
| #4 | `12_indicator_engine.py` — batch indicator calculation |
| #5 | Trade table data model design + schema migration |
