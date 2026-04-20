# PR #23 — system_state: dependency-based startup coordination

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

Gap checker and indicator engine started 5-10 seconds after ingestion,
but ingestion's initial backfill takes 60-90 minutes. Both processes
were calculating on empty/partial OHLCV tables.

## Solution: `system_state` table + wait_for()

### New: `core/system_state.py`
Simple key-value store backed by the `system_state` DB table.
- `set_state(key, value)` — upserts a state entry
- `get_state(key)` → value or None
- `is_set(key)` → True if value == 'true'
- `wait_for(key, poll_interval, log_interval, timeout)` — blocks until set

### New: `system_state` DB table (`core/schema.py`)
```sql
CREATE TABLE system_state (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Updated: `10_data_ingestion.py`
After `run_initial_backfill()` completes:
```python
set_state(KEY_BACKFILL_DONE)  # → 'initial_backfill_done' = 'true'
```

### Updated: `11_gap_checker.py`
On startup, waits before entering the scheduler loop:
```python
wait_for(KEY_BACKFILL_DONE, poll_interval=30, log_interval=120)
```

### Updated: `12_indicator_engine.py`
At the start of `poll_and_process()`:
```python
wait_for(KEY_BACKFILL_DONE, poll_interval=30, log_interval=120)
```

## Correct startup sequence

```
t=0s    Ingestion starts
          → Initial backfill (60-90 min)
          → WS fleet starts in parallel

t=5s    Gap Checker starts → waits for KEY_BACKFILL_DONE
t=10s   Indicator Engine starts → waits for KEY_BACKFILL_DONE
t=15s   Housekeeping starts (independent — no wait needed)

t=~90min  Ingestion backfill complete
            → set_state('initial_backfill_done')
          Gap Checker wakes up → enters scheduler loop
          Indicator Engine wakes up → fill_indicator_gaps() → normal cycle
```

## Subsequent restarts

On restart, `initial_backfill_done` is already set in DB from the
first run. Gap Checker and Indicator Engine find it immediately and
start without delay. Ingestion does a quick backfill check (skips
symbols that already have data) and sets the flag again within seconds.
