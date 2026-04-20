# PR #18 — Trade tables schema + V3 migration script

**Branch:** `dev`
**Date:** 2026-04-20

## Summary

Implements the complete V4 trade data model and a one-time migration
script to import all V3 trade data without data loss.

---

## New tables (`core/schema.py`)

### `trades` — unified trade table for all bots

| Column | Type | Notes |
|--------|------|-------|
| `bot_name` | TEXT | e.g. `AI_MIS1`, `strat_5_percent` |
| `bot_version` | TEXT | `v4` for new, `v3` for migrated |
| `timeframe` | TEXT | `1h`, `4h`, etc. |
| `symbol` | TEXT | e.g. `BTCUSDT` |
| `direction` | TEXT | `LONG` / `SHORT` |
| `entry` | REAL | entry price |
| `tp1`..`tp6` | REAL | targets — tp1 required, rest nullable |
| `sl` | REAL | current SL (moves with trailing) |
| `sl_initial` | REAL | original SL, never changes |
| `leverage` | INTEGER | always integer, always CROSS |
| `tp_count` | INTEGER | total TPs set at signal time |
| `tp_hit` | INTEGER | highest TP reached (0=none) |
| `position_pct` | REAL | remaining position (1.0=100%) |
| `ml_model` | TEXT | NULL for classic bots |
| `ml_confidence` | REAL | ML score or rolling win rate |
| `status` | TEXT | OPEN/CLOSED_TP/CLOSED_SL/CLOSED_MANUAL/CLOSED_EXPIRED |
| `outcome` | TEXT | WIN/LOSS/BREAKEVEN |
| `close_price` | REAL | price at close |
| `pnl_r` | REAL | R-multiple result |
| `weighted_pnl_r` | REAL | position-weighted R |

**Trailing SL logic** (enforced by trade monitor, not schema):
- TP1 hit → SL stays, 1/N position closed
- TP2 hit → SL moves to TP1, 1/N position closed
- TP3 hit → SL moves to TP2, etc.
- Fast bot → full close at TP1, no trailing

### `signal_log` — all signals including filtered ones

Records every signal a bot generates. `was_posted=FALSE` entries are
signals that were blocked by cooldown, confidence filter, regime filter, etc.
Housekeeping calculates `shadow_outcome` for unposted signals using 5m candles.

### `bot_performance` — daily win rate cache

Updated by housekeeping. `win_rate` is used as `ml_confidence` for
classic bots on new signals. Window: 90 days or all trades if < 90 days old.

### `v3_migration_log` — migration audit trail

Tracks which V3 rows have been migrated. Prevents duplicate migration
on repeated script runs.

---

## New file: `90_migrate_v3_trades.py`

One-time migration script. Run manually after V4 deployment.

**Usage:**
```bash
# Preview what would be migrated (no writes)
python 90_migrate_v3_trades.py --dry-run

# Execute migration
python 90_migrate_v3_trades.py
```

**Migrates 4 V3 tables:**

| V3 table | Rows | Notes |
|----------|------|-------|
| `active_trades_master` | Open + recent closed | Classic bots |
| `closed_trades_master` | All closed | Classic bots, partial data (no SL/TP stored) |
| `ai_signals` | Open | AI/ML bots, full data |
| `closed_ai_signals` | All closed | AI/ML bots |

**Safe to run multiple times** — `v3_migration_log` prevents duplicate rows.

**Migration sequence:**
1. Run V4 bootstrap (creates all tables)
2. `python 90_migrate_v3_trades.py --dry-run` (verify counts)
3. `python 90_migrate_v3_trades.py` (execute)
4. Verify `trades` table in pgAdmin
5. Start V4 trade monitor
6. Once confirmed working, stop V3 trade monitor
