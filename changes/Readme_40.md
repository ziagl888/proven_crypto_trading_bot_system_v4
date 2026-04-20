# PR #40 — Log rotation for all processes

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

Plain `FileHandler` writes every log line immediately to disk and
never rotates — log files grow unbounded and cause unnecessary I/O.

## Fix (all 5 process files)

Replaced `logging.FileHandler` with `logging.handlers.RotatingFileHandler`:

```python
logging.handlers.RotatingFileHandler(
    os.path.join(_LOG_DIR, "indicator_engine.log"),
    maxBytes=10 * 1024 * 1024,   # rotate at 10 MB
    backupCount=5,                # keep 5 backups
    encoding="utf-8",
)
```

- Max file size: 10 MB per file
- Backups kept: 5 (indicator_engine.log.1 … .5)
- Max disk usage per process: 50 MB
- Total for all 5 processes: 250 MB max

## Files changed
- `00_main_watchdog.py`
- `10_data_ingestion.py`
- `11_gap_checker.py`
- `12_indicator_engine.py`
- `23_housekeeping.py`
