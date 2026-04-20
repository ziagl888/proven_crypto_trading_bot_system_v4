# PR #26 — Indicator engine: ProcessPoolExecutor → ThreadPoolExecutor

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

Python 3.14 on Windows has a known bug where spawning child processes
triggers a WMI query via `platform.machine()` which fails with:
```
OSError: [WinError -2147217358] Windows Error 0x80041032
```
This caused the indicator engine to crash on every startup because
`ProcessPoolExecutor` spawns new processes that import pandas,
which calls `platform.machine()` at import time.

## Fix (`12_indicator_engine.py`)

Replaced `ProcessPoolExecutor` with `ThreadPoolExecutor`:
- No process spawning → no WMI call → no crash
- No pickle/IPC overhead → faster on Windows
- pandas/numpy release the GIL for many operations → threads are effective
- One future per symbol (simpler than batch chunking)

Removed `_calc_symbol_batch` (was only needed for ProcessPool chunking).
`_calc_symbol` is now the sole worker function.

## Performance note

ThreadPoolExecutor is actually faster than ProcessPoolExecutor for this
workload because:
1. No serialization overhead (DataFrames are large objects to pickle)
2. No process startup cost per batch
3. numpy/pandas release GIL during computation allowing true parallelism
   for CPU-bound sections
