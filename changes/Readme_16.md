# PR #16 — Gap checker: remove immediate startup run

**Branch:** `dev`
**Date:** 2026-04-20

## Problem

Gap checker ran immediately on startup, simultaneously with the data
ingestion backfill. Both made REST requests to Binance at the same time
causing unnecessary rate limit hits.

The gaps visible after a restart are already handled by the ingestion's
initial backfill (`run_initial_backfill()`).

## Fix

Removed the immediate `run_gap_check()` call from `main()`.
Gap checker now only runs at scheduled times (:20 and :50 every hour).

## Startup sequence (corrected)

```
t=0s   Ingestion → initial backfill fills restart gaps (REST)
t=5s   Gap Checker → waits silently for next :20 or :50
t=10s  Indicator Engine → fill_indicator_gaps() then normal cycle
```
