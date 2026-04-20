# PR #12 — Indicator engine: type fixes + warning suppression

**Branch:** `dev`
**Date:** 2026-04-19

## Verification results
- Syntax: OK
- mypy: 0 errors in 12_indicator_engine.py
- Runtime: calculate_indicators() → 600 rows × 140 cols, 0 NaN in last row

## Changes (`12_indicator_engine.py`)

### Warning suppression
- Added global `warnings.filterwarnings("ignore")` block after imports
- Covers: FutureWarning, DeprecationWarning, RuntimeWarning,
  SQLAlchemy, pandas, numpy divide-by-zero/overflow, scipy noise
- Removed redundant `import warnings` from worker functions

### Type fixes (4 issues found by mypy)

| Line | Issue | Fix |
|------|-------|-----|
| KAMA | `series.values` → ambiguous ExtensionArray type | `np.asarray(series, dtype=np.float64)` |
| KAMA | `np.full()` without dtype | `np.full(n, np.nan, dtype=np.float64)` |
| HVN/POC | `df["close"].values` → ambiguous type for np.histogram | `np.asarray(..., dtype=np.float64)` |
| groupby | `dict[Hashable, Any]` mismatch | `str(sym)` explicit cast |
| cache | `row.to_dict()` returns `dict[Hashable, Any]` | `{str(k): v for k, v in ...}` |
