# PR #53 — 21_trade_monitor.py + watchdog updates

**Branch:** `dev`
**Date:** 2026-04-20

## New: `21_trade_monitor.py`

Unified trade monitor for ALL open trades in the `trades` table.
Polls every 10s, checks latest 5m candle (wick-aware).

### Features
- **Wick-aware** — uses high/low not just close (catches intra-candle hits)
- **TP priority** — if same candle hits TP and SL → TP wins
- **Trailing SL** — after TP2: SL → TP1, after TP3: SL → TP2, etc.
- **Up to 6 TPs** (tp1..tp6)
- **pnl_r** calculated on close (R-multiples)
- **Stale guard** — skip trades if 5m candle > 30min old
- **Graceful shutdown** via ShutdownHandler

### Close logic
```
TP hit (last TP) → CLOSED_TP, outcome=WIN
TP hit (partial) → update tp_hit + trailing SL, stay OPEN
SL hit (tp_hit=0) → CLOSED_SL, outcome=LOSS
SL hit (tp_hit>0) → CLOSED_SL, outcome=WIN (partial win)
```

## Watchdog updates (`00_main_watchdog.py`)
Added to PROCESSES:
- `30_pump_dump_detector.py` (start_delay=25)
- `31_market_tracker.py` (start_delay=30)
- `21_trade_monitor.py` (start_delay=35)
