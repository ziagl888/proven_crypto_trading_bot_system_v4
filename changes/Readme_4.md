# PR #4 — 10_data_ingestion.py: OHLCV ingestion with WS fleet + backfill + gap checker

**Branch:** `dev`
**Date:** 2026-04-19

## Summary

Implements the complete OHLCV data ingestion pipeline. Covers initial backfill,
live WebSocket streaming, and periodic gap detection/repair.

## New files

### `10_data_ingestion.py`

**Startup check**
- Calls `verify_schema()` on start — aborts with clear error if DB tables missing

**Initial backfill** (REST, runs once on first start)
- Detects symbols with no data in `ohlcv_5m` and fills all timeframes
- Backfill depth per timeframe:

| Timeframes | Depth |
|------------|-------|
| 5m, 15m | 30 days |
| 30m, 1h, 2h, 4h | 1 year |
| 6h, 8h, 12h, 1d, 3d | 3 years |
| 1w, 1M | Maximum available |

- 3 parallel workers with 80ms inter-request delay (safe under Binance 2400 weight/min limit)
- Symbols that already have data are skipped automatically

**WebSocket fleet**
- Streams all coins × all INGEST_TIMEFRAMES via combined stream URL
- 860 streams per worker (safe under Binance 1024 limit)
- Staggered worker startup (10s between workers)
- RAM buffer: `{symbol: {timeframe: kline_tuple}}`
- **Write trigger: 5m candle close** (`k.x = true` in WS payload)
- On 5m close: all buffered timeframes for that symbol flushed to DB in one upsert
- No timer-based flush — DB writes happen exactly on candle boundary
- TCP keepalive on all WS sockets (prevents NAT timeout disconnects)
- Exponential backoff on reconnect (5s → 300s, with per-worker spread)
- Message watchdog: reconnects if silent for 120s

**Gap checker** (async scheduler)
- Runs at UTC hours: 00, 04, 08, 12, 16, 20 (every 4 hours)
- Per symbol/timeframe: compares actual row count vs expected in retention window
- Fetches and fills only the missing range via REST
- 3 parallel workers

## Changed files

### `00_main_watchdog.py`
- Added `10_data_ingestion.py` to `PROCESSES` list

## Architecture notes

- No Redis dependency — Trade Monitor will get its own dedicated WS stream
  for active symbols only (implemented in a future PR)
- Retention cleanup (DELETE old rows) is handled by `23_housekeeping.py` (future PR)
- Coin list is refreshed only by bootstrap + housekeeping, not by ingestion itself
