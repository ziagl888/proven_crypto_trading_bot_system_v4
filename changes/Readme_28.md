# PR #28 — Indicator engine: prevent parallel cycles per timeframe

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

Poll loop runs every 10s. If a cycle takes longer than 10s (570 symbols
× ThreadPoolExecutor), the next poll detects the same unprocessed event
and spawns another thread. After 60s: 6 parallel threads all calculating
the same timeframe. CPU at 100%, same closed_at logged repeatedly.

Root cause: `_LAST_PROCESSED[tf]` is only set AFTER cycle completes,
so every poll sees a "new" event and spawns a new thread.

## Fix

Added `_RUNNING_TFS: set[str]` + `_RUNNING_TFS_LOCK`:

```
Poll detects new event for tf:
  → if tf in _RUNNING_TFS: skip (cycle already running)
  → else: add tf to _RUNNING_TFS, start thread

Thread completes (success or error):
  → finally: _RUNNING_TFS.discard(tf)  ← always released
```

This guarantees at most ONE active cycle per timeframe at any time.
The next candle close will trigger a new cycle once the current one
finishes.
