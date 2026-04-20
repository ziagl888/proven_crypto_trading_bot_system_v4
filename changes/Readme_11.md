# PR #11 — Hotfix: missing candles in OHLCV ingestion

**Branch:** `dev`
**Date:** 2026-04-19

## Root causes

Three bugs combined to cause missing candles:

### Bug 1 — `asyncio.get_event_loop()` in running async context (critical)
`asyncio.get_event_loop()` is deprecated in Python 3.10+ inside a running
coroutine. It can return a different loop or raise a DeprecationWarning that
causes the executor call to silently fail. Result: DB flush never executed,
candle lost.

**Fix:** Replaced with `asyncio.create_task(_flush_symbol_async(...))` —
proper async task scheduling via the running loop.

### Bug 2 — Fire-and-forget without concurrency control
500 coins closing simultaneously = 500 DB connections attempted at once,
exhausting the pool (max=20). Excess flushes would raise `PoolError` and
lose candles.

**Fix:** Added `_FLUSH_SEMAPHORE = asyncio.Semaphore(20)` — max 20
concurrent DB flushes at any time, others queue up safely.

### Bug 3 — Only 5m close as trigger (no fallback)
If Binance doesn't send a 5m close event (reconnect mid-candle, illiquid
coin, stream gap), the buffered data for all timeframes is never written.

**Fix:** Added `_fallback_flush_loop()` — runs every 6 minutes, flushes
any symbol whose buffered 5m candle is more than 6 minutes old.

## Changes (`10_data_ingestion.py`)

| Change | Detail |
|--------|--------|
| `_flush_symbol_async()` | New async wrapper with semaphore |
| `_FLUSH_SEMAPHORE` | `asyncio.Semaphore(20)` — concurrency cap |
| WS handler | `asyncio.create_task()` instead of `get_event_loop().run_in_executor()` |
| `_fallback_flush_loop()` | Background task, flushes stale buffers every 6min |
| `start_ws_fleet()` | Added fallback loop to gather tasks |
