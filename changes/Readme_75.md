# PR #75 — 32_funding_monitor.py + 33_whale_monitor.py

**Branch:** `dev`
**Date:** 2026-04-21

## New: `32_funding_monitor.py`

REST poll every 5 min → `funding_rates` DB table.

**Key improvements vs V3:**
| | V3 | V4 |
|--|----|----|
| Storage | JSON files (daily) | PostgreSQL `funding_rates` table |
| Historical lookup | bisect on RAM list (~50k entries) | SQL indexed query (single round-trip) |
| Index rebuild | Full rebuild after every poll | No rebuild — DB index always current |
| asyncio | Yes | Yes (single aiohttp session) |

**Reports:** Every 30 min at :00:10 / :30:10 UTC
- BTC/ETH rates + 1h/24h delta in basis points
- TOP20 sentiment % positive (now vs 1h ago)
- Top 5 most positive / most negative across all coins

**Extreme alerts** (15-min cooldown):
- ≥75% / ≥85% / ≥95% of TOP20 simultaneously positive or negative

## New: `33_whale_monitor.py`

aggTrade WebSocket → filters `usd_value >= $25k` → `whale_trades` DB table.

**Key improvements vs V3:**
| | V3 | V4 |
|--|----|----|
| Storage | JSON files + RAM list | PostgreSQL `whale_trades` table |
| Stats | Python loops over RAM | SQL GROUP BY aggregates |
| Write path | Direct to list | 15s write buffer → batch INSERT |
| Retention | Manual cutoff in RAM | Nightly housekeeping DELETE |

**WS architecture:** 573 streams < 600 limit → 1 connection.
Buffer flush every 15s to decouple WS handler from DB writes.

**Reports:** Every 30 min at :00:05 / :30:05 UTC
- BTC / ETH / TOP20 blocks with L/S ratio (1h, prev 1h, 4h, 24h)
- Top 5 altcoins by long/short volume (ex BTC/ETH)
- Top 5 largest individual trades (long + short)

## New DB tables (`core/schema.py`)

### `funding_rates`
```sql
symbol TEXT, ts TIMESTAMPTZ, rate REAL
PRIMARY KEY (symbol, ts)
INDEX on (symbol, ts DESC)
```
TimescaleDB hypertable with 1-day chunks (if available).

### `whale_trades`
```sql
id BIGSERIAL, symbol TEXT, ts TIMESTAMPTZ,
direction TEXT, usd_value REAL, price REAL, qty REAL
INDEX on (symbol, ts DESC), (ts DESC)
```
Retention: 3 days (housekeeping purges nightly).

## New config entry
- `MARKET_MONITOR_CHANNEL_ID` — shared channel for funding + whale reports

## `.env.example`
Added: `MARKET_MONITOR_CHANNEL_ID=-100xxxxxxxxxx`

## `00_main_watchdog.py`
Added at t+32s and t+34s (after Market Tracker):
- `32_funding_monitor.py`
- `33_whale_monitor.py`
