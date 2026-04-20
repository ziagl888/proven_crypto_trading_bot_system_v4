# PR #44 — 31_market_tracker.py: hourly/half-hourly channel reports

**Branch:** `dev`
**Date:** 2026-04-20

## New: `31_market_tracker.py`

Full port of V3 `23_market_tracker.py` to V4 schema and architecture.

### All 6 jobs preserved

| Time | Job | Frequency |
|------|-----|-----------|
| XX:00:01 | Signal Summary | hourly |
| XX:00:15 | Main Volume Report (BTC/ETH/Total) | hourly |
| XX:01:00 | Gainers & Losers (1h/4h/24h) | hourly |
| XX:00:30 | Per-Bot Performance + Kelly | hourly |
| XX:00:30 & XX:30:30 | Volume Spikes | every 30min |
| XX:00:45 & XX:30:45 | Volatile Coins | every 30min |

### V4 schema changes
| V3 | V4 |
|----|----|
| `SELECT FROM "BTCUSDT_30m"` | `SELECT FROM ohlcv_30m WHERE symbol='BTCUSDT'` |
| `active_trades_master` + `closed_trades_master` + `ai_signals` | `trades` (unified) |
| `send_telegram()` direct | `INSERT INTO telegram_outbox` |

### V4 architecture improvements
- `ShutdownHandler` — clean Ctrl+C, interruptible sleep
- Rotating logs (10MB × 5 backups)
- Bot name normalization (`_normalize_bot_name`) — MIS variants unified
- `_build_chunks()` — message splitting at 4096 chars (Telegram limit)
- Kelly calculation preserved 1:1 (Half-Kelly, Safe/Pure Margin)
- Outcome classification preserves V3 logic (neutral for DELISTED/CLEANUP)
