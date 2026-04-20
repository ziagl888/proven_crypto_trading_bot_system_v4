# PR #47 — Fix: duplicate indicator cycles from candle_close_events

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

Indicator Engine was triggering duplicate cycles for the same timeframe:

```
16:30:08  New candle close: 30m at 14:30:07  → cycle starts
16:31:03  Cycle complete (55s)
16:31:08  New candle close: 30m at 14:30:24  → DUPLICATE! same candle, new cycle
```

Root cause: `_candle_close_event_writer` used `NOW()` as `closed_at`.
When multiple WS workers flush symbols at slightly different times,
the batch writer runs 2-3 times within the same 30m candle window,
each time writing a new `NOW()` timestamp → Indicator Engine sees it
as a new event and starts another cycle.

## Fix (`10_data_ingestion.py`)

### 1. Track actual close time per TF

Added `_CLOSE_TIMES: dict[str, datetime]` — stores the actual wall-clock
time when the FIRST symbol closed this TF in the current batch.

### 2. Use actual time in DB write (not NOW())

```python
# Before
VALUES (%s, NOW(), %s)

# After  
close_ts = snapshot_times.get(tf)   # actual time from flush
VALUES (%s, %s, %s)                  # stable timestamp
```

### 3. WHERE clause prevents time going backwards

```sql
ON CONFLICT (timeframe) DO UPDATE SET
    closed_at = EXCLUDED.closed_at,
    symbol_count = EXCLUDED.symbol_count
WHERE candle_close_events.closed_at < EXCLUDED.closed_at
```

Only updates if the new timestamp is NEWER than what's stored.
Subsequent batch-writer runs for the same candle are ignored.

## Result

Each 30m/1h/4h candle close now triggers exactly ONE indicator cycle.
