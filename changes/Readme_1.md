# PR #1 — Initial Commit: Project Foundation V4

**Branch:** `dev`
**Date:** 2026-04-19
**Author:** Claude (Anthropic)

---

## Summary

Initial commit of the V4 Crypto Trading Bot System. This establishes the project skeleton, database schema, configuration layer, and watchdog. No trading logic is included yet — this PR provides the foundation all subsequent PRs will build upon.

---

## Changes

### New Files

| File | Purpose |
|------|---------|
| `00_main_watchdog.py` | System entry point. Runs bootstrap, starts all processes in staggered order, monitors and auto-restarts on crash with exponential back-off. |
| `core/__init__.py` | Package marker. |
| `core/config.py` | Central configuration. All secrets and channel IDs read exclusively from `.env` — nothing hardcoded. Added `_int()` helper for safe int-casting of env vars. |
| `core/database.py` | PostgreSQL connection pool (`psycopg2.ThreadedConnectionPool`). Pool size increased to max 20 (V3: 8) to support more concurrent bots. Drop-in replacement for bare `psycopg2.connect()`. |
| `core/schema.py` | Full DB schema: OHLCV tables (16 timeframes, 10s–1M), indicator tables (6 timeframes), all operational tables. BRIN + composite B-tree indexes on every time-series table. |
| `core/bootstrap.py` | First-run sequence: fetches coin list and max leverage from Binance, writes `coins.json` / `max_leverage.json`, calls schema initialiser. |
| `.env.example` | Template for all required environment variables. |
| `.gitignore` | Excludes `.env`, logs, state files, ML models, charts, caches. |
| `requirements.txt` | Python dependencies (unchanged from V3). |
| `README.md` | Project documentation. |
| `changes/Readme_1.md` | This file. |

---

## Architecture decisions

### 1. Unified DB schema (OHLCV + indicators)

**V3 problem:** One table per coin × timeframe = ~4,300 OHLCV tables + ~3,200 indicator tables = ~7,500 tables. PostgreSQL catalog overhead and planner cost scale with table count.

**V4 solution:** One table per timeframe (`ohlcv_1h`, `ohlcv_4h`, …). Symbol is a column with a composite primary key `(symbol, open_time)`. BRIN indexes on `open_time` (cheap for append-only time-series) + B-tree on `(symbol, open_time DESC)` for point lookups.

**Expected impact:** -40% DB I/O, significantly faster schema introspection, simpler migrations.

### 2. Trimmed indicator columns

**V3 problem:** Indicator engine computed and stored 91 columns per row. Full audit of all 29 bots and 5 strategy files revealed that several V3 columns are never read.

**V4 solution:** 67 columns stored (removed unused: `ma_kama_200`, `donchian_4/10/12/15` mid/upper/lower duplicates consolidated). Every stored column is traced to at least one consumer.

**Expected impact:** -25% indicator table size, faster indicator writes.

### 3. File naming convention

Clear numeric prefix groups related files and makes `ls` output self-documenting:
- `00` Watchdog
- `10–19` Core data services
- `20–29` Infrastructure
- `30–69` Trading bots
- `70–89` Dashboard / utilities
- `90–99` Backtest / training (not auto-started)

### 4. All config in `.env`

**V3 problem:** Channel IDs hardcoded in individual bot files. Changing a channel required editing multiple files.

**V4 solution:** Every channel ID, token, and credential lives in `.env`. `core/config.py` reads them with safe defaults. Bot files import named constants from `core.config`.

### 5. Pool size increase

`_POOL_MAX` raised from 8 → 20. With 30+ bots each potentially holding a connection during a scan cycle, the V3 pool was frequently exhausted (logged as "Pool exhausted" errors), causing scan delays.

---

## DB tables created by this PR

### OHLCV (16 tables)
`ohlcv_10s`, `ohlcv_1m`, `ohlcv_3m`, `ohlcv_5m`, `ohlcv_15m`, `ohlcv_30m`,
`ohlcv_1h`, `ohlcv_2h`, `ohlcv_4h`, `ohlcv_6h`, `ohlcv_8h`, `ohlcv_12h`,
`ohlcv_1d`, `ohlcv_3d`, `ohlcv_1w`, `ohlcv_1M`

### Indicators (6 tables)
`indicators_30m`, `indicators_1h`, `indicators_2h`,
`indicators_4h`, `indicators_1d`, `indicators_1w`

### Operational (14 tables)
`active_trades_master`, `closed_trades_master`, `ai_signals`, `closed_ai_signals`,
`telegram_outbox`, `trade_cooldowns`, `ml_predictions_master`,
`master_ai_processed_signals`, `regime_history`, `regime_current`,
`bot_regime_performance`, `bot_regime_whitelist`,
`orchestrator_open_trades`, `orchestrator_suppressed_signals`

---

## Next steps (upcoming PRs)

| PR | Scope |
|----|-------|
| #2 | `10_data_ingestion.py` — WebSocket fleet + REST catch-up, writes to `ohlcv_*` |
| #3 | `12_indicator_engine.py` — batch SQL reads, vectorized KAMA, writes to `indicators_*` |
| #4 | `13_detectors.py` + strategy files |
| #5 | `20_telegram_bot.py`, `21_trade_monitor.py`, `22_ai_trade_monitor.py` |
| #6+ | Individual trading bots (30–50) |
