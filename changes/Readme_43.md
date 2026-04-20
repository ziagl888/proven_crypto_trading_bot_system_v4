# PR #43 — 30_pump_dump_detector.py (PD-1)

**Branch:** `dev`
**Date:** 2026-04-20

## New: `30_pump_dump_detector.py`

Market movement and volume explosion detector. Module tag: `PD-1`.
Posts alerts to `PUMP_DUMP_MARKET_CHANNEL_ID` via `telegram_outbox`.

### 3 detectors

**1. Round Level Breaks** (BTC/ETH/BNB/SOL/XRP)
- Detects price crossing key round levels (BTC: every $500, ETH: every $100, etc.)
- 3min cooldown per symbol/level

**2. Extreme Price Moves**
| Timeframe | Threshold |
|-----------|-----------|
| 1 min | ≥ 2% |
| 2 min | ≥ 3% |
| 5 min | ≥ 5% |
| 10 min | ≥ 8% |
- Dead-cat bounce / dip-in-uptrend detection
- 10min trend context shown in message

**3. Volume Explosion** (new vs V3 — 3 tiers)
| Tier | Multiplier | Label |
|------|-----------|-------|
| Mega | ≥ 20× avg | 🔥 MEGA VOLUME EXPLOSION |
| Massive | ≥ 10× avg | ⚡ MASSIVE VOLUME EXPLOSION |
| Strong | ≥ 5× avg | 📈 STRONG VOLUME EXPLOSION |
- Requires ≥ 1.5% price confirmation (no false alerts on thin markets)
- Baseline built from last 2h of valid 10s volume deltas
- 24h rollover protection (ignores negative volume deltas)

### V4 improvements vs V3
- State in DB (`trade_cooldowns`) — no more JSON file crash risk
- `ShutdownHandler` — clean Ctrl+C
- Rotating logs
- 3-tier volume thresholds (V3 had only 12×)
- Volume min price confirmation threshold

### Not in this bot
- EPD-1 (ML-based early detection) → separate bot
- AI trade signals → separate bot
