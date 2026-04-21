# PR #74 — Watchdog + .env + Schema migration for detectors & traders

**Branch:** `dev`
**Date:** 2026-04-21

## `00_main_watchdog.py` — 4 new processes added

| Process | Script | Start delay |
|---------|--------|-------------|
| Pattern Detector  | `40_pattern_detector.py`     | t+40s |
| Trendbreaker Det. | `41_trendbreaker_detector.py`| t+45s |
| Pattern Trader    | `50_pattern_trader.py`       | t+50s |
| Trendbreaker Trd. | `51_trendbreaker_trader.py`  | t+55s |

Detectors start before traders to ensure events exist before polling begins.
All use `restart_interval: None` — only restart on crash.

## `.env.example` — 2 new channel IDs

```
ATB1_INFO_CHANNEL_ID=-100xxxxxxxxxx    # ATB-1 detection info alerts
PATTERN_INFO_CHANNEL_ID=-100xxxxxxxxxx  # Pattern Detector info alerts
```

**Action required:** Add these two channel IDs to your real `.env` file.

## `core/schema.py` — `migrate_schema()` extended

`migrate_schema()` now also calls:
- `_infrastructure_tables_ddl()` — ensures outbox, cooldowns, etc. exist
- `_trade_tables_ddl()` — ensures trades, pattern_events, trendline_events exist

This is safe to run on a live system — all DDL uses `IF NOT EXISTS`.

**Run this once after pulling to create the new tables:**
```bash
python -c "from core.schema import migrate_schema; migrate_schema()"
```

Or simply restart the watchdog — bootstrap calls `create_all_tables()` which
covers all tables automatically.

## New tables created by this PR (via schema.py PR #72)

### `pattern_events`
Tracks geometric chart pattern lifecycle:
`BREAKOUT` → `WAITING_RETEST` → `RETEST` → `CONFIRMED` / `FAKEOUT` / `EXPIRED`

### `trendline_events`
Tracks trendline break/bounce lifecycle:
`DETECTED` / `WAITING_RETEST` → `CONFIRMED` / `EXPIRED`

Both tables are indexed on `(state, symbol)` with partial indexes for
active states only — minimal overhead on non-active rows.
