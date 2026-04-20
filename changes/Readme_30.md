# PR #30 — Fix: wait_for() interruptible on Ctrl+C

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

`core/system_state.wait_for()` used `time.sleep(poll_interval)`.
On Ctrl+C, this blocked for up to 30 seconds before raising KeyboardInterrupt,
causing ugly tracebacks in the log:

```
File "core/system_state.py", line 94, in wait_for
    time.sleep(poll_interval)
KeyboardInterrupt
```

## Fix (`core/system_state.py`)

Replaced `time.sleep(poll_interval)` with `threading.Event.wait(timeout=poll_interval)`.

`threading.Event.wait()` responds immediately to signals — on Ctrl+C,
it raises KeyboardInterrupt which is caught and returns `False` cleanly:

```python
try:
    _wake.wait(timeout=poll_interval)
except (KeyboardInterrupt, SystemExit):
    logger.info(f"wait_for({key!r}) interrupted — shutting down.")
    return False
```

## Shutdown sequence now (all clean)

```
Ctrl+C
  Watchdog:          sends SIGTERM to all subprocesses
  Gap Checker:       wait_for() returns False → "stopped cleanly"
  Indicator Engine:  wait_for() returns False → "stopped cleanly"
  Housekeeping:      shutdown.sleep() returns → "stopped cleanly"
  Ingestion:         KeyboardInterrupt caught → "stopped (Ctrl+C)"
  Watchdog:          "System fully offline"
```

No more tracebacks on clean shutdown.
