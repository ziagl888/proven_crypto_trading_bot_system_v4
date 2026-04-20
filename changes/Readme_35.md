# PR #35 — Fix deadlocks + slow indicator writes

**Branch:** `dev`
**Date:** 2026-04-20

## Problem 1: Deadlocks on `candle_close_events`

570 symbols all closing simultaneously → 570 concurrent upserts on
the same ~10 rows → PostgreSQL deadlock cascade.

**Fix (`10_data_ingestion.py`):**
- Removed `_write_candle_close_event()` from per-symbol flush
- Added `_candle_close_event_writer()` coroutine that runs every 1s
- Batches all accumulated TF counts into a single DB write per second
- On write failure: counts are restored to `_CLOSE_COUNTS` (no data loss)

## Problem 2: Indicator cycle takes 52s

Single 570-row upsert with 137 columns holds a large transaction
for 50+ seconds → lock contention + slow.

**Fix (`12_indicator_engine.py`):**
- Chunked writes: 100 rows per commit instead of all 570 at once
- Each chunk commits independently → shorter lock hold times
- Expected improvement: 52s → ~10-15s

## Summary

| Issue | Cause | Fix |
|-------|-------|-----|
| Deadlocks | 570 concurrent upserts on same rows | Batch writer every 1s |
| Slow cycle | Single 570-row transaction | Chunked 100-row commits |
