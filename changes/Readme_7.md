# PR #7 — Hotfix: indicator engine OHLCV batch query

**Branch:** `dev`
**Date:** 2026-04-19

## Problem

`_load_ohlcv_batch()` used a nested subquery to dynamically calculate the
lookback window. This was overly complex and fragile — the subquery relied
on interval arithmetic inside PostgreSQL that could fail on edge cases
(empty table, single row, etc).

## Fix (`12_indicator_engine.py`)

- Replaced dynamic subquery with a static `_TF_LOOKBACK_DAYS` dict
- Each timeframe maps to a fixed number of days that covers 500+ candles
- Single clean `SELECT ... WHERE open_time >= NOW() - INTERVAL 'N days'`
- Removed unused `LOOKBACK_CANDLES` constant (now implicit in `_TF_LOOKBACK_DAYS`)

| Timeframe | Lookback days | Candles covered |
|-----------|--------------|-----------------|
| 30m | 11 | ~528 |
| 1h | 22 | ~528 |
| 2h | 44 | ~528 |
| 4h | 85 | ~510 |
| 1d | 520 | ~520 |
| 1w | 3640 | ~520 |
