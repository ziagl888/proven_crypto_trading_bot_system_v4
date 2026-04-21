# PR #76 — Unlimited whale retention + V3 market data migration

**Branch:** `dev`
**Date:** 2026-04-21

## `core/schema.py` — whale_trades retention changed

`whale_trades` table: **unlimited retention** — no automatic purge.
Rationale: historical whale flow data is valuable for market direction analysis
and correlation studies. Manage disk space manually if needed via SQL.

`funding_rates` table: unchanged — 3-day rolling window (purged by housekeeping).

## `23_housekeeping.py` — funding_rates cleanup added (Task 4/5)

New `task_funding_cleanup()`:
- Deletes `funding_rates` rows older than 3 days
- Runs nightly at 03:00 UTC along with other housekeeping tasks
- `whale_trades` is NOT purged (unlimited retention)

## `92_migrate_v3_market_data.py` — new migration script

One-time migration of V3 JSON files into V4 DB tables.

**V3 source paths (default):**
```
C:\_BOT\proven_crypto_bot\whale_data\whale_trades_YYYY-MM-DD.json
C:\_BOT\proven_crypto_bot\funding_data\funding_history_YYYY-MM-DD.json
```

**Usage:**
```bash
# Preview (no DB writes)
python 92_migrate_v3_market_data.py --dry-run

# Migrate both
python 92_migrate_v3_market_data.py

# Whale only
python 92_migrate_v3_market_data.py --whale-only

# Funding only
python 92_migrate_v3_market_data.py --funding-only

# Custom paths
python 92_migrate_v3_market_data.py --whale-dir "D:\other\whale_data"
```

**Migration details:**
- V3 whale records have no `qty` field → stored as `0.0`
- `funding_rates` uses `ON CONFLICT DO NOTHING` (idempotent)
- `whale_trades` uses migration log (`v3_market_migration_log`) to skip
  already-processed files — safe to run multiple times
- Batch size: 5,000 rows per INSERT for performance
- Prints DB stats (row count, date range, symbol count) after migration

**Recommended migration sequence:**
1. Stop V3 whale/funding bots (or wait until last save)
2. `python 92_migrate_v3_market_data.py --dry-run` (verify counts)
3. `python 92_migrate_v3_market_data.py` (execute)
4. Start V4 `32_funding_monitor.py` and `33_whale_monitor.py`
