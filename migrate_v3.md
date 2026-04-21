# Migration V3 → V4 — Complete Guide

**Document:** `migrate_v3.md`  
**Last updated:** 2026-04-21  
**Target system:** proven_crypto_trading_bot_system_v4 (branch: `dev`)  
**Source system:** proven_crypto_bot (branch: `main`)

---

## Overview

This document describes the complete procedure for migrating all historical data from V3 to V4.  
Three separate migration scripts cover all data categories:

| Script | What it migrates | Target tables |
|--------|-----------------|---------------|
| `90_migrate_v3_trades.py` | Trade history (all bots) | `trades`, `v3_migration_log` |
| `91_simulate_v3_trades.py` | Recalculates outcomes via 5m candles | `trades` (UPDATE) |
| `92_migrate_v3_market_data.py` | Whale trades + Funding rates (JSON files) | `whale_trades`, `funding_rates` |

---

## Prerequisites

Before starting migration:

**1. V4 bootstrap must have run at least once**
```bash
cd C:\_BOT\proven_crypto_trading_bot_system_v4
python 00_main_watchdog.py
# Wait for "Bootstrap complete" then Ctrl+C
# OR run bootstrap directly:
python -c "from core.bootstrap import run; run()"
```

**2. All V4 DB tables must exist**
```bash
python -c "from core.schema import create_all_tables; create_all_tables()"
```

Verify:
```sql
-- Run in pgAdmin or psql
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
-- Must include: trades, signal_log, bot_performance, v3_migration_log,
--              whale_trades, funding_rates, pattern_events, trendline_events
```

**3. V3 and V4 must use the same PostgreSQL instance**  
`90_migrate_v3_trades.py` reads directly from V3 tables via DB connection.  
If they are on separate servers, set up a temporary DB link or dump/restore first.

**4. V3 bot should still be running during migration**  
Migration is non-destructive — V3 tables are never modified or dropped.  
New V4 trades during migration will be caught by the `v3_migration_log` dedup logic.

---

## Step 1 — Migrate Trade History

**Script:** `90_migrate_v3_trades.py`

### What it does

Reads four V3 tables and inserts all records into the unified V4 `trades` table:

| V3 table | Content | Notes |
|----------|---------|-------|
| `active_trades_master` | Open + recently closed classic bot trades | Full data (entry, SL, TP1-4) |
| `closed_trades_master` | All closed classic bot trades | Partial data — SL/TP may be 0 |
| `ai_signals` | Open AI/ML bot trades | Full data (entry1/2, SL, targets JSON) |
| `closed_ai_signals` | All closed AI/ML bot trades | Full data |

### Bot name mapping (V3 → V4)

All V3 bot name variants are normalized to unified V4 names:

| V3 name(s) | V4 name |
|------------|---------|
| `Fast In And Out` | `FIO-1` |
| `Volume Indicator` | `VOL-1` |
| `5 Percent` | `PCT-5` |
| `Support Resistance` | `SR-1` |
| `Main Channel` | `MAIN-1` |
| `MIS1-8H`, `MIS1-8h`, `MIS1-8h_pump/dump`, `MSI1-8h_pump/dump` | `MIS-1-8h` |
| `MIS1-24H`, `MIS1-24h`, `MIS1-24h_pump/dump`, `MSI1-24h_pump/dump` | `MIS-1-24h` |
| `MIS1-72H` + variants | `MIS-1-72h` |
| `MIS1-168H` + variants | `MIS-1-168h` |
| `BR1H`, `BR2H`, `BR4H`, `BR1D` | `BR-1h`, `BR-2h`, `BR-4h`, `BR-1d` |
| `BB_1H`, `BB_4H` | `BB-1h`, `BB-4h` |
| `QM_1H`, `QM_4H` | `QM-1h`, `QM-4h` |
| `TD_1H`, `TD_4H` | `TD-1h`, `TD-4h` |
| `ATS1`, `ATS1_Robust` | `ATS-1` |
| `ATB1` | `ATB-1` |
| `AIM1` | `AIM-1` |
| `ABR1` | `ABR-1` |
| `RUB1` | `RUB-1` |
| `SRA1` | `SRA-1` |
| `EPD1` | `EPD-1` |
| `UFI1` | `UFI-1` |
| `ROM1` | `ROM-1` |
| `SMC_15M`, `SMC_30M`, `SMC_4H` | `SMC-1` |

### Status mapping

V3 numeric status → V4 status + outcome:

| V3 status | V4 status | outcome |
|-----------|-----------|---------|
| `WORKING` | `OPEN` | NULL |
| `0` | `CLOSED_SL` | `LOSS` |
| `1` | `CLOSED_TP` | `WIN` (tp_hit=1) |
| `2` | `CLOSED_TP` | `WIN` (tp_hit=2) |
| `3` | `CLOSED_TP` | `WIN` (tp_hit=3) |
| `4` | `CLOSED_TP` | `WIN` (tp_hit=4) |

V3 AI close reason strings → V4:

| V3 close_reason | V4 status | outcome |
|-----------------|-----------|---------|
| `SL Hit (SL: 0.123)` | `CLOSED_SL` | `LOSS` |
| `ALL TARGETS HIT` | `CLOSED_TP` | `WIN` |
| `LEGACY TARGET HIT (+2.5%)` | `CLOSED_TP` | `WIN` |
| `LEGACY FALLBACK SL (-5.0%)` | `CLOSED_SL` | `LOSS` |
| *(empty)* | `CLOSED_MANUAL` | NULL |

### Execution

```bash
cd C:\_BOT\proven_crypto_trading_bot_system_v4

# Step 1a: Dry run — preview counts, no DB writes
python 90_migrate_v3_trades.py --dry-run

# Expected output:
#   active_trades_master : 47 rows → 47 to migrate, 0 already done
#   closed_trades_master : 1,234 rows → 1,234 to migrate, 0 already done
#   ai_signals           : 12 rows → 12 to migrate, 0 already done
#   closed_ai_signals    : 892 rows → 892 to migrate, 0 already done

# Step 1b: Execute migration
python 90_migrate_v3_trades.py

# Expected output:
#   Migration complete — 2,185 trades inserted, 0 skipped
#   Open trades: 59 | Closed: 2,126
```

### Verify in pgAdmin

```sql
-- Total trades migrated
SELECT COUNT(*), bot_version FROM trades GROUP BY bot_version;
-- Expect: v3 = ~2185, v4 = 0 (no V4 trades yet)

-- Open trades by bot
SELECT bot_name, COUNT(*) FROM trades
WHERE status = 'OPEN' AND bot_version = 'v3'
GROUP BY bot_name ORDER BY count DESC;

-- Check migration log
SELECT source_table, COUNT(*) FROM v3_migration_log
GROUP BY source_table;
```

---

## Step 2 — Simulate / Verify Trade Outcomes

**Script:** `91_simulate_v3_trades.py`

### What it does

Replays each migrated V3 trade through actual 5m candle data to:
- Verify and correct `outcome` (WIN / LOSS / BREAKEVEN)
- Calculate correct `close_price`, `pnl_r`, `weighted_pnl_r`, `tp_hit`
- Update trailing SL for still-open trades

Data source priority:
1. Local `ohlcv_5m` table (fast, no API)
2. Binance REST API (for trades older than 30 days)

### Trailing SL logic used in simulation

- TP1 hit → SL stays unchanged
- TP2 hit → SL moves to TP1
- TP3 hit → SL moves to TP2 (etc.)
- Each TP closes `1/N` of position (Cornix behaviour)

### Execution

```bash
# Step 2a: Test on first 100 trades
python 91_simulate_v3_trades.py --dry-run --limit 100

# Step 2b: Full dry run
python 91_simulate_v3_trades.py --dry-run

# Expected output:
#   Trades to simulate: 2,185
#   Progress: 2185/2185 (100%) — W:1,102 L:891 NoData:134 StillOpen:58 (245s)

# Step 2c: Execute
python 91_simulate_v3_trades.py

# Expected output:
#   Simulation complete in 312s
#   Total trades:  2,185
#   Still open:    58
#   Wins:          1,102
#   Losses:        891
#   No data:       134
#   Win rate:      55.3%
#   Avg pnl_r:     +0.42R
```

### Notes

- `no_data` entries: trades too old for local 5m data AND Binance API had no data.
  These remain as `CLOSED_MANUAL` with NULL outcome. Normal for trades > 90 days.
- `Still open` entries: V3 open trades that have not yet hit SL or TP. These stay
  `OPEN` and are picked up by the V4 Trade Monitor.
- Safe to re-run — updates are idempotent (re-simulates from scratch each time).

### Verify in pgAdmin

```sql
-- Outcome distribution
SELECT outcome, status, COUNT(*) FROM trades
WHERE bot_version = 'v3'
GROUP BY outcome, status
ORDER BY count DESC;

-- Win rate by bot
SELECT bot_name,
       COUNT(*) FILTER (WHERE outcome='WIN')  AS wins,
       COUNT(*) FILTER (WHERE outcome='LOSS') AS losses,
       ROUND(AVG(pnl_r)::numeric, 2)          AS avg_r
FROM trades
WHERE bot_version = 'v3' AND outcome IS NOT NULL
GROUP BY bot_name
ORDER BY bot_name;

-- Still open from V3 (should be monitored by Trade Monitor)
SELECT bot_name, symbol, direction, entry, sl, tp1, opened_at
FROM trades
WHERE bot_version = 'v3' AND status = 'OPEN'
ORDER BY opened_at DESC;
```

---

## Step 3 — Migrate Whale Trades & Funding Rates

**Script:** `92_migrate_v3_market_data.py`

### What it does

Reads V3 JSON files and inserts records into V4 DB tables:

| V3 source | V3 format | V4 table | Notes |
|-----------|-----------|----------|-------|
| `whale_data/whale_trades_YYYY-MM-DD.json` | `{ts, sym, dir, usd, prc}` | `whale_trades` | `qty` stored as 0.0 (not in V3) |
| `funding_data/funding_history_YYYY-MM-DD.json` | `{ts, sym, rate}` | `funding_rates` | `ON CONFLICT DO NOTHING` |

### Default V3 source paths

```
C:\_BOT\proven_crypto_bot\whale_data\whale_trades_YYYY-MM-DD.json
C:\_BOT\proven_crypto_bot\funding_data\funding_history_YYYY-MM-DD.json
```

If the V3 bot is installed in a different location, use `--whale-dir` / `--funding-dir`.

### Retention

| Table | Retention |
|-------|-----------|
| `whale_trades` | **Unlimited** — never auto-purged |
| `funding_rates` | **3 days** — nightly housekeeping purge |

### Execution

```bash
# Step 3a: Dry run — shows file count and record counts
python 92_migrate_v3_market_data.py --dry-run

# Expected output:
#   Found 45 whale trade files.
#   [DRY RUN] whale_trades_2025-08-23.json: 1,204 records → 1,201 valid, 3 skipped
#   ...
#   Whale migration complete: 54,230 total → 54,193 valid, 37 skipped
#
#   Found 45 funding history files.
#   [DRY RUN] funding_history_2025-08-23.json: 3,438 records → 3,438 valid, 0 skipped
#   ...
#   Funding migration complete: 154,710 total → 154,710 valid, 0 skipped

# Step 3b: Execute both
python 92_migrate_v3_market_data.py

# Step 3c: Migrate only whale trades (if funding already done)
python 92_migrate_v3_market_data.py --whale-only

# Step 3d: Custom paths
python 92_migrate_v3_market_data.py \
  --whale-dir "D:\OtherBot\whale_data" \
  --funding-dir "D:\OtherBot\funding_data"
```

### Verify in pgAdmin

```sql
-- Whale trades
SELECT
    COUNT(*)                AS total_rows,
    COUNT(DISTINCT symbol)  AS symbols,
    MIN(ts)::date           AS oldest,
    MAX(ts)::date           AS newest,
    SUM(usd_value) / 1e9    AS total_volume_bn_usd
FROM whale_trades;

-- Top 10 symbols by whale volume
SELECT symbol,
       COUNT(*)             AS trades,
       SUM(usd_value)/1e6   AS volume_m_usd,
       COUNT(*) FILTER (WHERE direction='LONG')  AS longs,
       COUNT(*) FILTER (WHERE direction='SHORT') AS shorts
FROM whale_trades
GROUP BY symbol
ORDER BY volume_m_usd DESC
LIMIT 10;

-- Funding rates
SELECT
    COUNT(*)                AS total_rows,
    COUNT(DISTINCT symbol)  AS symbols,
    MIN(ts)::date           AS oldest,
    MAX(ts)::date           AS newest
FROM funding_rates;

-- Check migration log
SELECT source_type, COUNT(*), SUM(records_ok) AS total_records
FROM v3_market_migration_log
GROUP BY source_type;
```

---

## Step 4 — Start V4 Trade Monitor

After migration, the V4 Trade Monitor (`21_trade_monitor.py`) picks up all open V3 trades automatically.

**Before starting:**

```bash
# Verify open V3 trades look correct
SELECT symbol, direction, entry, sl, tp1, tp_hit, bot_name
FROM trades
WHERE status = 'OPEN'
ORDER BY opened_at DESC;
```

**Start order (if not using watchdog):**

```bash
# 1. Telegram Bot (needed for outbox delivery)
python 20_telegram_bot.py

# 2. Trade Monitor
python 21_trade_monitor.py
```

The Trade Monitor will start monitoring all `OPEN` trades regardless of `bot_version`.

---

## Step 5 — Start V4 Market Monitors

```bash
# Add to .env first:
MARKET_MONITOR_CHANNEL_ID=-100...   # Funding + Whale shared channel

# Start monitors
python 32_funding_monitor.py   # REST poll every 5 min
python 33_whale_monitor.py     # WebSocket aggTrade listener
```

At this point the V3 whale/funding bots (`19_whale_logger_bot.py`, `20_funding_logger_bot.py`) can be stopped.

---

## Complete Migration Checklist

```
PRE-MIGRATION
□ V4 bootstrap run (coins.json + DB schema created)
□ V4 DB tables verified (32 tables including trades, whale_trades, funding_rates)
□ V3 and V4 on same PostgreSQL instance (or DB link configured)
□ .env file configured with all required channel IDs

STEP 1 — TRADE HISTORY
□ python 90_migrate_v3_trades.py --dry-run   (verify counts)
□ python 90_migrate_v3_trades.py             (execute)
□ SQL: verify trades count and bot_version = 'v3'

STEP 2 — TRADE SIMULATION
□ python 91_simulate_v3_trades.py --dry-run --limit 100  (test run)
□ python 91_simulate_v3_trades.py                        (execute)
□ SQL: verify outcome distribution (WIN/LOSS/BREAKEVEN)
□ SQL: check win rates per bot look reasonable

STEP 3 — WHALE + FUNDING DATA
□ python 92_migrate_v3_market_data.py --dry-run  (verify file counts)
□ python 92_migrate_v3_market_data.py            (execute)
□ SQL: verify whale_trades date range and row count
□ SQL: verify funding_rates date range and row count

STEP 4 — START V4
□ Start 21_trade_monitor.py (picks up all OPEN V3 trades)
□ Verify Trade Monitor logs show V3 open trades being monitored
□ Stop V3 trade monitors (5_trade_monitor.py, 8_ai_trade_monitor.py)

STEP 5 — MARKET MONITORS
□ Start 32_funding_monitor.py
□ Start 33_whale_monitor.py
□ Verify 30-min overview messages appear in Telegram
□ Stop V3 market monitors (19_whale_logger_bot.py, 20_funding_logger_bot.py)

DONE
□ V3 bots still running: Data Ingestion only (until V4 data ingestion verified)
□ V4 running: Watchdog with all configured processes
```

---

## Troubleshooting

### `90_migrate_v3_trades.py` — "table does not exist"

V3 tables not found in the connected database.

```bash
# Check which DB you're connecting to
python -c "from core.config import DB_HOST, DB_NAME; print(DB_HOST, DB_NAME)"

# Check V3 tables exist
psql -U dbfiller -d cryptodata -c "\dt active_trades_master"
```

If V3 and V4 use different databases, you need to either:
- Point V4's `.env` temporarily to the V3 database, migrate, then switch back
- Or use `pg_dump` + `pg_restore` to copy V3 tables to V4's database first

### `90_migrate_v3_trades.py` — unknown bot names

```
WARNING: Unknown bot name 'MY_CUSTOM_BOT' — keeping as-is.
```

Add the mapping to `_BOT_NAME_MAP` in `90_migrate_v3_trades.py`:
```python
"MY_CUSTOM_BOT": "CUSTOM-1",
```

### `91_simulate_v3_trades.py` — many `no_data` entries

Expected for trades older than what's available in `ohlcv_5m` (usually 30 days).
The script falls back to Binance REST API. If REST also returns no data, the trade
remains as `CLOSED_MANUAL` with NULL outcome. This is normal for old trades.

To check which date range has local 5m data:
```sql
SELECT MIN(open_time)::date, MAX(open_time)::date
FROM ohlcv_5m WHERE symbol = 'BTCUSDT';
```

### `92_migrate_v3_market_data.py` — "Directory not found"

```
ERROR: Directory not found: C:\_BOT\proven_crypto_bot\whale_data
```

The V3 bot is installed in a different location. Use `--whale-dir`:
```bash
python 92_migrate_v3_market_data.py \
  --whale-dir "C:\Users\ASUS\proven_crypto_bot\whale_data" \
  --funding-dir "C:\Users\ASUS\proven_crypto_bot\funding_data"
```

Or update `V3_ROOT` directly in the script.

### Re-running migration scripts

All three scripts are safe to re-run:

- `90_migrate_v3_trades.py`: uses `v3_migration_log` — already migrated records are skipped
- `91_simulate_v3_trades.py`: re-simulates everything from scratch (always overwrites)
- `92_migrate_v3_market_data.py`: uses `v3_market_migration_log` per file — already processed files skipped; `funding_rates` uses `ON CONFLICT DO NOTHING`

---

## Post-Migration SQL Analysis Queries

```sql
-- ── Trade performance overview ────────────────────────────────────────────────
SELECT
    bot_name,
    COUNT(*)                                            AS total,
    COUNT(*) FILTER (WHERE outcome = 'WIN')             AS wins,
    COUNT(*) FILTER (WHERE outcome = 'LOSS')            AS losses,
    ROUND(
        COUNT(*) FILTER (WHERE outcome = 'WIN')::numeric /
        NULLIF(COUNT(*) FILTER (WHERE outcome IN ('WIN','LOSS')), 0) * 100,
    1)                                                  AS win_rate_pct,
    ROUND(AVG(pnl_r)::numeric, 2)                       AS avg_pnl_r,
    COUNT(*) FILTER (WHERE status = 'OPEN')             AS still_open
FROM trades
WHERE bot_version = 'v3'
GROUP BY bot_name
ORDER BY total DESC;

-- ── Whale flow correlation with price ─────────────────────────────────────────
-- Long/short ratio per day for BTC
SELECT
    DATE_TRUNC('day', ts)                              AS day,
    SUM(usd_value) FILTER (WHERE direction='LONG')     AS long_vol,
    SUM(usd_value) FILTER (WHERE direction='SHORT')    AS short_vol,
    ROUND(
        SUM(usd_value) FILTER (WHERE direction='LONG') /
        NULLIF(SUM(usd_value) FILTER (WHERE direction='SHORT'), 0),
    2)                                                  AS ls_ratio
FROM whale_trades
WHERE symbol = 'BTCUSDT'
GROUP BY 1 ORDER BY 1;

-- ── Extreme funding rate days ─────────────────────────────────────────────────
SELECT
    DATE_TRUNC('hour', ts)  AS hour,
    symbol,
    rate * 100              AS rate_pct
FROM funding_rates
WHERE ABS(rate) > 0.001   -- > 0.1% = extreme
ORDER BY ABS(rate) DESC
LIMIT 50;
```

---

## Data Retention Summary

| Table | Retention | Managed by |
|-------|-----------|------------|
| `ohlcv_5m`, `ohlcv_15m`, `ohlcv_30m` | 30 days | `23_housekeeping.py` |
| `ohlcv_1h` .. `ohlcv_1M` | 1–3 years | `23_housekeeping.py` |
| `funding_rates` | 3 days rolling | `23_housekeeping.py` |
| `whale_trades` | **Unlimited** | Manual |
| `trades` | **Unlimited** | Manual |
| `telegram_outbox` | 7 days (sent only) | `23_housekeeping.py` |
| `pattern_events`, `trendline_events` | Unlimited | Manual |
