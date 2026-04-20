# PR #27 — Watchdog: unbuffered output + better crash detection

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

Indicator Engine started by watchdog but produced no log output and
silently disappeared. The watchdog was restarting it in a loop without
logging the crash clearly.

## Fixes (`00_main_watchdog.py`)

### Unbuffered subprocess output
Added `-u` flag and `PYTHONUNBUFFERED=1` so subprocess output flushes
immediately to log files — buffered output was causing log entries to
be lost on crash.

### Better crash detection and logging
If a process exits within 30 seconds of starting, it's flagged as a
likely import/startup crash (CRITICAL level). The restart delay and
reason are now clearly logged:
```
CRITICAL: [Indicator Engine] exited (code=1, uptime=2s) — likely import/startup crash
INFO: [Indicator Engine] waiting 15s before restart...
```
