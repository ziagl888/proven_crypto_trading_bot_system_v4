# PR #24 — Backfill: always fill restart gaps on every startup

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

`run_initial_backfill()` only ran for symbols with NO data at all.
On restart, symbols already had data so the backfill was skipped entirely —
leaving gaps from the downtime period unfilled until the gap checker ran
(up to 40 minutes later at :20 or :50).

Bots rely on complete, up-to-date OHLCV data for indicator calculation.
Starting the indicator engine on incomplete data produces wrong results.

## Fix

`_backfill_symbol()` now runs on every startup for every symbol:

| Condition | Action |
|-----------|--------|
| No data exists | Full historical backfill (BACKFILL_DAYS depth) |
| Data exists, gap < 2 candle periods | Skip (up to date) |
| Data exists, gap >= 2 candle periods | Fill from last candle to NOW |

`initial_backfill_done` is only set AFTER all gaps are filled,
so indicator engine always starts on complete data.

## Correct startup sequence

```
Ingestion starts
  → run_initial_backfill() for ALL 570 symbols
     → symbols with gaps: fill from last candle to NOW (fast, minutes)
     → fresh symbols: full historical backfill (slow, first run only)
  → initial_backfill_done = 'true'
  → WS fleet starts in parallel (live data)

Gap Checker:   wait_for(initial_backfill_done) → released immediately
Indicator Eng: wait_for(initial_backfill_done) → starts on complete data
```

## Performance

Restart gap fill is fast — typically only a few candles per symbol per TF.
570 symbols × checking for gaps = a few seconds on restart.
Only symbols with actual gaps make REST requests.
