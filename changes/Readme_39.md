# PR #39 — Fix: global cycle semaphore prevents GIL contention

**Branch:** `dev`
**Date:** 2026-04-20

## Root cause of 140-160s cycle times

When 4 TF cycles trigger simultaneously (1h, 2h, 4h, 30m at candle close):
- 4 cycles × NUM_WORKERS=16 = **64 concurrent threads**
- All doing CPU-bound pandas/numpy indicator calculations
- Python GIL serializes CPU-bound threads → all 64 threads fight each other
- Result: 4 × 30s = 120-160s instead of 30s

## Fix (`12_indicator_engine.py`)

Added `_CYCLE_SEMAPHORE = threading.Semaphore(2)`:
- Max 2 cycles run concurrently at any time
- Other cycles queue up and run after (not skipped)
- 2 cycles × 32 threads instead of 4 cycles × 64 threads

## Expected timing

| Scenario | Before | After |
|----------|--------|-------|
| 1 cycle  | 30s    | 30s   |
| 2 cycles | 160s   | ~60s  |
| 4 cycles | 160s   | ~65s  |

The 2 queued cycles run immediately after the first 2 finish.
Total wall time for 4 cycles: ~65s instead of 160s.
