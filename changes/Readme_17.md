# PR #17 — Gap checker: trigger indicator recalculation after gap fill

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

After filling OHLCV gaps and deleting stale indicator rows, the indicator
engine would only notice the change at the next `candle_close_event` —
up to 30 minutes later. Bots would read stale/missing indicators in the
meantime.

## Fix (`11_gap_checker.py`)

### New: `_trigger_indicator_recalc(affected_tfs)`
After deleting stale indicator rows, upserts `candle_close_events` with
`closed_at = NOW()` for each affected indicator timeframe.

The indicator engine polls `candle_close_events` every 10s. Since the
new `closed_at` is newer than `_LAST_PROCESSED[tf]`, the engine
immediately starts a recalculation cycle — including `fill_indicator_gaps()`
which recalculates all missing rows.

### Updated: `run_gap_check()`
After a gap fill that deleted indicator rows (`total_ind > 0`), calls
`_trigger_indicator_recalc()` for all affected indicator timeframes.
Only triggers for `INDICATOR_TIMEFRAMES` (not 5m/15m which have no
indicator tables).

## Full gap recovery flow

```
:20 or :50 UTC
  Gap Checker:
    1. Detects OHLCV gap for symbol/TF
    2. REST backfill → fills missing candles
    3. Deletes stale indicator rows (from gap start onward)
    4. Upserts candle_close_events with NOW()

Within 10 seconds:
  Indicator Engine:
    5. Polls candle_close_events → detects new closed_at
    6. fill_indicator_gaps() → recalculates all missing rows
    7. Updates INDICATOR_CACHE
    8. Bot runner fires with fresh indicators
```

Total recovery time after gap fill: **< 2 minutes** (was up to 30 min).
