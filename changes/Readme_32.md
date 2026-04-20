# PR #32 — Fix: absolute log paths + force=True for all processes

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

Indicator Engine (and potentially other processes) did not write to their
log files when launched by the watchdog subprocess. Two root causes:

1. **Relative log path** — `logs/indicator_engine.log` is relative to CWD.
   When watchdog launches a subprocess, the CWD may differ from the script
   directory, so the log file is created in the wrong place (or not at all).

2. **`basicConfig()` silently ignored** — Python's `logging.basicConfig()`
   is a no-op if the root logger already has handlers. When launched as a
   subprocess that imports packages which configure logging, the FileHandler
   is never registered.

## Fix (all 4 process files)

```python
# Before
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    handlers=[
        logging.FileHandler("logs/indicator_engine.log", ...),
    ],
)

# After
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR     = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)
logging.basicConfig(
    force=True,   # override any existing root logger
    handlers=[
        logging.FileHandler(os.path.join(_LOG_DIR, "indicator_engine.log"), ...),
    ],
)
```

`force=True` removes existing handlers before applying the new ones.
Absolute path ensures log files are always in the right directory.

## Files changed
- `10_data_ingestion.py`
- `11_gap_checker.py`
- `12_indicator_engine.py`
- `23_housekeeping.py`
