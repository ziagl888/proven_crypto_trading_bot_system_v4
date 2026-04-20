# PR #10 — Fibonacci fix + Swing High/Low detection

**Branch:** `dev`
**Date:** 2026-04-19

## Summary

Fixes the conceptually incorrect Fibonacci calculation and adds proper
swing high/low detection as a shared foundation for multiple bots.

---

## Fixes

### Fibonacci — `_fibonacci()` rewritten (`12_indicator_engine.py`)

**Bug:** `fib_support` and `fib_resistance` were identical — both used
`hi - diff * level` (retracement from period maximum). This is wrong:

| Column | Was | Now |
|--------|-----|-----|
| `fib_support_*` | `hi - diff * level` (ok but period-based) | `sh - diff * level` — retracement from **swing high** downward (buy zones on pullback) |
| `fib_resistance_*` | `hi - diff * level` (same as support!) | `sl + diff * level` — retracement from **swing low** upward (sell zones on rally) |
| `fib_extension_*` | `hi + diff * (ext-1)` | `sh + diff * (ext-1)` — extensions above swing high (upside targets) |

Now uses actual confirmed swing highs/lows instead of period max/min.
Falls back to period high/low if no swing is detected.

---

## New features

### Swing High/Low detection — `_swings()` (`12_indicator_engine.py`)

- Detects last 5 confirmed swing highs and swing lows per symbol/timeframe
- Uses `scipy.signal.argrelextrema` with timeframe-dependent `order`:

| Timeframe | order | Meaning |
|-----------|-------|---------|
| 30m, 1h | 5 | 5 candles left+right must be lower/higher |
| 2h | 8 | |
| 4h, 1d | 10 | More confirmation needed for larger TFs |
| 1w | 5 | Limited data, smaller order |

- Stores price + age (candles since the swing) for each swing

### 20 new columns in `core/schema.py`

```
swing_high_1 .. swing_high_5   REAL     — swing high price
swing_high_1_age .. _5_age     INTEGER  — candles since that swing
swing_low_1  .. swing_low_5    REAL     — swing low price
swing_low_1_age  .. _5_age     INTEGER  — candles since that swing
```

### Consumers of swing data

| Bot | Uses |
|-----|------|
| Fibonacci (engine) | swing_high_1, swing_low_1 |
| 7_pattern_detector | swing highs/lows for trendlines |
| 21_btc_smc | pivot detection |
| 22_ip_pattern | alternating pivots for QM pattern |
| 24_quasimodo | alternating pivot sequence |
| 25_smc_sniper | breaker block detection |
| 29_ufi1 | swing high for fib entry |
| Trade monitor | SL placement relative to swings |
