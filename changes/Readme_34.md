# PR #34 — Indicator engine: fix log spam + add write timing

**Branch:** `dev`
**Date:** 2026-04-20

## Fixes (`12_indicator_engine.py`)

### Log spam fix
`logger.info("New candle close detected")` was logged BEFORE the
`_RUNNING_TFS` guard — so every 10s poll printed 4 lines even when
all cycles were still running.

Moved the log AFTER the guard so it only prints when a new cycle
actually starts. Skipped events now log at DEBUG level only.

**Before:** 4 × "New candle close detected" every 10 seconds
**After:** Only logged when a new cycle starts (~every 30s per TF)

### Write timing
Added `t0 = time.time()` around `_write_indicators_batch`.
If DB write takes > 5 seconds, logs a WARNING with timing.
This helps diagnose whether the bottleneck is calculation or DB write.
