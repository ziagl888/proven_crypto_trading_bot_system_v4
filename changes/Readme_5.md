# PR #5 — Schema update + 12_indicator_engine.py

**Branch:** `dev`
**Date:** 2026-04-19

## Summary

Adds derived feature columns to the indicator schema, adds the
`candle_close_events` IPC table, extends the ingestion to write close events,
and implements the complete indicator engine with shared memory cache.

---

## Changed files

### `core/schema.py`
- Added 9 derived feature columns to `INDICATOR_COLUMNS`:
  `atr_pct`, `bb_position_relative`, `dc_position_relative`,
  `dist_close_ema9_pct`, `dist_ema9_ema21_pct`, `dist_close_kama9_pct`,
  `dist_close_ema200_pct`, `macd_hist_fast`, `macd_hist_normal`
- Added `candle_close_events` table (IPC between ingestion and engine)
- Updated `verify_schema()` to include new table

### `10_data_ingestion.py`
- Added `_CLOSED_TFS` tracker: records which timeframes closed between 5m flushes
- Added `_write_candle_close_event()`: upserts close event after each flush
- `_flush_symbol_to_db()` now accepts `closed_tfs` set and writes events
- WS handler passes closed TFs to flush function

### `00_main_watchdog.py`
- Added `12_indicator_engine.py` to PROCESSES (start_delay=10s after ingestion)

---

## New files

### `12_indicator_engine.py`

**Architecture: Single Process, Shared Memory**

| Component | Detail |
|-----------|--------|
| Trigger | Polls `candle_close_events` every 10s |
| OHLCV load | 1 batch query per timeframe (all symbols) |
| Calculation | ProcessPoolExecutor (NUM_WORKERS) — one worker per symbol |
| DB write | 1 batch upsert per timeframe via `execute_values` |
| Cache | `INDICATOR_CACHE[tf][symbol]` dict in RAM |
| Bot runner | ThreadPoolExecutor — bots read from cache, no DB reads |

**DB access pattern per cycle:**

| Step | V3 | V4 |
|------|----|----|
| OHLCV reads | 500 individual queries | **1 batch query** |
| Indicator writes | 500 individual upserts | **1 batch upsert** |
| Bot indicator reads | ~500 × 20 bots = 10,000 | **0** (from cache) |

**Cache scope:**
- `indicators_1h` + `indicators_30m` → in RAM cache (used by 8+ bots)
- `indicators_4h`, `2h`, `1d`, `1w` → written to DB only (read by detectors)

**Indicator calculation:**
- All vectorial (pandas/numpy) — no Python loops over candles
- KAMA: custom numpy loop (unavoidable) but runs in parallel workers
- Trendline, HVN/POC, S/R, Fibonacci: scipy — runs per-symbol in worker
- Derived features computed in same pass — zero extra DB reads

**Bot runner:**
- Placeholder implemented — bots added incrementally in future PRs
- Pattern: `run_bots_for_timeframe(tf)` dispatches threads with cache snapshot

---

## Indicator columns added (9 new)

All derived from base indicators — no extra OHLCV data needed:

| Column | Formula | Used by |
|--------|---------|---------|
| `atr_pct` | `atr_14 / close * 100` | 12_ats, 14_atb |
| `bb_position_relative` | `(close-boll_lower) / (boll_upper-boll_lower)` | 12_ats, 14_atb |
| `dc_position_relative` | `(close-dc_lower) / (dc_upper-dc_lower)` | 12_ats, 14_atb |
| `dist_close_ema9_pct` | `(close-ema_9) / ema_9` | 14_atb |
| `dist_ema9_ema21_pct` | `(ema_9-ema_21) / ema_21` | 14_atb, 12_ats |
| `dist_close_kama9_pct` | `(close-kama_9) / kama_9` | 14_atb, 12_ats |
| `dist_close_ema200_pct` | `(close-ema_200) / ema_200` | 11_mis, 12_ats |
| `macd_hist_fast` | `macd_dif_fast - macd_dea_fast` | 11_mis, 12_ats |
| `macd_hist_normal` | `macd_dif_normal - macd_dea_normal` | 13_rub, 24_qm |
