# PR #14 — 11_gap_checker.py + indicator gap fill

**Branch:** `dev`
**Date:** 2026-04-20

## Summary

Implements a dedicated gap checker that runs at :20 and :50 every hour,
and extends the indicator engine to detect and fill its own gaps on startup.

---

## New file: `11_gap_checker.py`

Runs at :20 and :50 of every hour (after 30m candle close + buffer).
Also runs once immediately on startup.

**Per symbol/timeframe:**
1. Detects missing candles using `generate_series` vs actual DB rows
2. Fills gaps via REST backfill (3 parallel workers)
3. Deletes stale indicator rows from the oldest gap onward
   → indicator engine will recalculate those rows automatically

**Why delete indicators after a gap?**
Indicators calculated with missing OHLCV data are mathematically wrong
(e.g. EMA_200 skips candles → wrong smoothing). Deleting them forces
a clean recalculation from correct data.

---

## Changes: `12_indicator_engine.py`

### New: `_get_indicator_gaps()`
Compares `MAX(open_time)` in `indicators_{tf}` vs `ohlcv_{tf}` per symbol.
Returns symbols where indicators lag behind OHLCV.

### New: `fill_indicator_gaps()`
For each symbol with a gap:
- Loads full OHLCV history (lookback window for warmup)
- Calculates ALL indicators for the full period
- Writes only the missing rows (after last_ind_time)
- Updates INDICATOR_CACHE for priority timeframes

### Updated: `poll_and_process()`
On startup, before the normal cycle:
1. `fill_indicator_gaps()` for ALL indicator timeframes
2. Then normal `run_indicator_cycle()` for cache-priority TFs

---

## Changes: `00_main_watchdog.py`
- Added `11_gap_checker.py` to PROCESSES (start_delay=5s)

---

## Full startup sequence

```
t=0s   Data Ingestion starts (WS fleet + backfill)
t=5s   Gap Checker starts → immediate gap check on all TFs
t=10s  Indicator Engine starts:
         → fill_indicator_gaps() all TFs (fixes any stale indicators)
         → run_indicator_cycle() for 1h + 30m (populates cache)
         → poll_and_process() loop begins
t=15s  Housekeeping starts
```

## Timing of gap checks

| Time | Event |
|------|-------|
| :20 | Gap check (20 min after :00 candle close) |
| :50 | Gap check (20 min after :30 candle close) |
| Startup | Immediate gap check |
