# PR #25 — Fix: reset backfill flag on startup

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

`initial_backfill_done` flag persists in DB between restarts.
Gap Checker and Indicator Engine found the flag from the *previous* run
and started immediately — before the current backfill completed.

From the log:
```
11:19:23  Gap Checker: "Initial backfill done" ← old flag from prev run!
11:20:00  Gap Checker: runs gap check          ← backfill still running
11:21:56  Ingestion: "Bootstrap flag set"      ← actual backfill done
```

## Fix (`10_data_ingestion.py`)

Reset the flag to `"false"` immediately on startup, before backfill begins.
Other processes see `"false"` and wait. Flag is set to `"true"` only
after `run_initial_backfill()` completes.

```python
# Reset BEFORE backfill
set_state(KEY_BACKFILL_DONE, "false")

# Run backfill (fills restart gaps)
await loop.run_in_executor(None, run_initial_backfill, symbols)

# Set AFTER backfill complete
set_state(KEY_BACKFILL_DONE, "true")
```

## Correct sequence now

```
t=0s    Ingestion: flag → "false"  (gap checker + engine must wait)
        Backfill runs for all 570 symbols (~2 min on restart)
t=2min  Backfill done: flag → "true"
        Gap Checker: wakes up, enters scheduler
        Indicator Engine: wakes up, calculates on complete data
        WS Fleet: starts streaming live data
```
