# PR #21 — 91_simulate_v3_trades.py: historical trade simulation

**Branch:** `dev`
**Date:** 2026-04-20

## Purpose

After migrating V3 trade history, verifies all closed trade outcomes by
replaying each trade through actual 5m candle data. This catches any
incorrect V3 status values (wrong outcome, wrong close price, etc.).

## Logic

**Data source:** Local `ohlcv_5m` table first, Binance REST API as fallback
(for trades older than 30 days — V3 trades go back to 2025-08-23).

**Per-candle simulation (wick-aware):**
- `high >= tp` → TP hit (TP has priority over SL in same candle)
- `low <= sl` → SL hit

**Trailing SL:**
- TP1 hit → SL stays unchanged
- TP2 hit → SL moves to TP1
- TP3 hit → SL moves to TP2, etc.

**Partial close:** each TP closes 1/N of position (matching Cornix behavior)

**weighted_pnl_r:** position-weighted average R across all partial closes

**Outcome determination:**
- All TPs hit → CLOSED_TP / WIN
- SL hit (no prior TPs) → CLOSED_SL / LOSS
- SL hit (after some TPs) → CLOSED_SL / WIN or LOSS based on weighted_pnl_r
- End of data → CLOSED_MANUAL

## Usage

```bash
# Preview first 100 trades (recommended first run)
python 91_simulate_v3_trades.py --dry-run --limit 100

# Full dry run
python 91_simulate_v3_trades.py --dry-run

# Execute simulation (overwrites V3 outcomes)
python 91_simulate_v3_trades.py
```

## Verification results (unit tests)

| Test | Expected | Result |
|------|----------|--------|
| LONG, all TPs hit, trailing SL | CLOSED_SL WIN tp=2 | ✅ |
| SHORT, SL hit immediately | CLOSED_SL LOSS | ✅ |
| TP1 hit then trailing SL = breakeven | CLOSED_SL BREAKEVEN | ✅ |
