# PR #8 — Hotfix: timezone bug in indicator engine

**Branch:** `dev`
**Date:** 2026-04-19

## Problem

In `calculate_indicators()`, using `.values` on a timezone-aware pandas
Series strips the UTC timezone info:

```python
# Bug: .values returns numpy datetime64 WITHOUT tzinfo
ind_df["open_time"] = df["open_time"].values
```

psycopg2 then interprets the naive datetime as **local time** instead of
UTC when writing to `TIMESTAMPTZ` columns. On a server in Europe/Bucharest
(UTC+3) this would shift all indicator timestamps 3 hours into the future.

## Fix (`12_indicator_engine.py`)

1. `calculate_indicators()`: assign Series directly — no `.values`
2. `_write_indicators_batch()`: re-apply `pd.to_datetime(..., utc=True)`
   after `pd.concat()` to guard against concat dropping tz info

## Timezone audit summary

| File | Status |
|------|--------|
| `core/schema.py` | ✅ TIMESTAMPTZ columns |
| `10_data_ingestion.py` | ✅ all datetimes UTC-aware |
| `12_indicator_engine.py` | ✅ fixed by this PR |
| `23_housekeeping.py` | ✅ all datetimes UTC-aware |
