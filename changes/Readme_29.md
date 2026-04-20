# PR #29 — Graceful shutdown for all processes

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

Ctrl+C caused all threads to crash hard — DB writes could be interrupted
mid-transaction, causing partial/corrupted data.

## Solution

### New: `core/shutdown.py` — `ShutdownHandler`

Thread-safe shutdown coordinator:
- Registers SIGINT (Ctrl+C) and SIGTERM handlers
- Sets a `threading.Event` on signal
- `is_set()` — check if shutdown requested
- `sleep(seconds)` — interruptible sleep, wakes on signal
- `wait()` — block until shutdown

### Per-process changes

| File | Change |
|------|--------|
| `core/shutdown.py` | New — ShutdownHandler class |
| `11_gap_checker.py` | `while not shutdown.is_set()` + `shutdown.sleep()` |
| `12_indicator_engine.py` | Poll loop + waits for active cycles to finish |
| `23_housekeeping.py` | Scheduler loop + interruptible sleep |
| `10_data_ingestion.py` | `KeyboardInterrupt` → clean log message |
| `00_main_watchdog.py` | SIGTERM handler → clean shutdown of all subprocesses |

### Indicator Engine shutdown sequence
On Ctrl+C:
1. Poll loop exits (`while not shutdown.is_set()`)
2. Waits up to 10s for any running cycles to finish
3. Logs "stopped cleanly"

### Gap Checker + Housekeeping
`shutdown.sleep(wait)` wakes up immediately on Ctrl+C instead of
waiting for the full sleep duration (up to 40 minutes).
