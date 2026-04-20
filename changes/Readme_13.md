# PR #13 — Schema migration: add missing indicator columns to existing tables

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

The indicator engine crashed with:
```
psycopg2.errors.UndefinedColumn: column "atr_pct" of relation "indicators_30m" does not exist
```

The new columns (derived features + swing highs/lows) were added to
`INDICATOR_COLUMNS` in `core/schema.py` but existing DB tables were not
updated. `CREATE TABLE IF NOT EXISTS` only creates the table if it doesn't
exist — it does not add new columns to existing tables.

## Solution

### `core/schema.py` — new `migrate_schema()` function

- Reads all existing columns from `information_schema.columns` for each
  `indicators_*` table
- Adds any columns present in `INDICATOR_COLUMNS` but missing from the DB
  using `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
- Safe to run repeatedly — idempotent
- Logs which columns were added per table

### `core/bootstrap.py` — calls `migrate_schema()` automatically

Migration runs on every startup, after `create_all_tables()`:
```
create_all_tables()   → creates tables if they don't exist
migrate_schema()      → adds missing columns to existing tables
verify_schema()       → confirms everything is in order
```

## Columns added by this migration (29 total per indicator table)

**Derived features (9):**
`atr_pct`, `bb_position_relative`, `dc_position_relative`,
`dist_close_ema9_pct`, `dist_ema9_ema21_pct`, `dist_close_kama9_pct`,
`dist_close_ema200_pct`, `macd_hist_fast`, `macd_hist_normal`

**Swing Highs/Lows (20):**
`swing_high_1..5`, `swing_high_1..5_age`,
`swing_low_1..5`, `swing_low_1..5_age`

Applied to all 6 indicator tables:
`indicators_30m`, `indicators_1h`, `indicators_2h`,
`indicators_4h`, `indicators_1d`, `indicators_1w`
