# PR #6 — 23_housekeeping.py: nightly maintenance

**Branch:** `dev`
**Date:** 2026-04-19

## Summary

Implements nightly housekeeping. Runs once on startup, then every day
at 03:00 UTC.

## New files

### `23_housekeeping.py`

Four tasks run sequentially:

| # | Task | Detail |
|---|------|--------|
| 1 | Coin list update | Fetches fresh pairs from Binance, saves `coins.json`, logs added/removed |
| 2 | Max leverage update | Fetches leverage brackets, saves `max_leverage.json` |
| 3 | OHLCV cleanup | Single `DELETE` per timeframe — no per-symbol loop |
| 4 | Outbox cleanup | Deletes sent `telegram_outbox` rows older than 7 days |

**Retention limits:**

| Timeframes | Retention |
|------------|-----------|
| 5m, 15m | 30 days |
| 30m, 1h, 2h, 4h | 1 year |
| 6h, 8h, 12h, 1d, 3d, 1w, 1M | 3 years |

Each task is wrapped in its own try/except — one failing task does not
prevent the others from running.

## Changed files

### `00_main_watchdog.py`
- Added `23_housekeeping.py` to PROCESSES (start_delay=15s)
