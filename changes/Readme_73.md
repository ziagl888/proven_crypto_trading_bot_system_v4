# PR #73 — 50_pattern_trader.py + 51_trendbreaker_trader.py

**Branch:** `dev`
**Date:** 2026-04-21

## New: `50_pattern_trader.py` (BR-1)

Polls `pattern_events WHERE state='RETEST'` every 60s.

**Confirmation logic:**
- Strong candle body ≥ 0.5% closing on break side
- Cooldown per bot/symbol/direction per timeframe (6h/12h/24h/72h)
- SL/TP via S/R + Fibonacci (same as V3 calculate_smart_targets)
- Trade channel: BR_CHANNELS dict (per timeframe: BR-1h, BR-2h, BR-4h, BR-1d)

## New: `51_trendbreaker_trader.py` (ATB-1)

Polls `trendline_events WHERE state IN (DETECTED, WAITING_RETEST)` every 60s.

**Break flow:**
1. WAITING_RETEST: wait for price to return within 1.5% of trendline
2. Retest info alert sent to ATB1_INFO_CHANNEL_ID
3. Confirmation candle (≥0.5% body, closes on break side)
4. ML score ≥ threshold (0.80 LONG / 0.75 SHORT) → trade
5. If models not found: trade without ML check

**Bounce flow:**
1. DETECTED event: confirmation candle directly → ML → trade

**ML features:** calculated without pandas_ta (manual numpy).
Same feature set as V3 ATB1 model — compatible with existing .joblib files.

## Key design decisions
- No pandas_ta dependency (Python 3.14 compatible)
- State machine: DETECTED/WAITING_RETEST → CONFIRMED/EXPIRED
- All trades written to unified `trades` table
- Cooldown via `trade_cooldowns` table
