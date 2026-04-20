# PR #31 — Watchdog: all sleeps interruptible on Ctrl+C

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

Watchdog had three `time.sleep()` calls that blocked on Ctrl+C:
- Line 168: stagger delay between process starts (up to 15s)
- Line 201: backoff delay after crash (up to 900s)
- Line 211: monitor poll interval (10s)

Result: `KeyboardInterrupt` traceback in watchdog on Ctrl+C.

## Fix (`00_main_watchdog.py`)

Added `_SHUTDOWN_EVENT = threading.Event()` at module level.
Replaced all three `time.sleep()` calls with `_SHUTDOWN_EVENT.wait(timeout=N)`.

`threading.Event.wait()` returns immediately when the event is set,
so Ctrl+C now causes instant clean shutdown:

| Trigger | Action |
|---------|--------|
| Ctrl+C (KeyboardInterrupt) | Sets `_SHUTDOWN_EVENT`, stops all processes |
| SIGTERM | Sets `_SHUTDOWN_EVENT`, stops all processes, `sys.exit(0)` |
| Stagger abort | Returns from startup if Ctrl+C during process stagger |
| Backoff abort | Breaks monitor loop if Ctrl+C during restart backoff |
