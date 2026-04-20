# PR #15 — Gap checker: fix rate limiting + illiquid coin false gaps

**Branch:** `dev`
**Date:** 2026-04-20

## Problems fixed

### Rate limiting (3× "Rate limited — sleeping 21s")
3 workers firing requests simultaneously caused burst rate limiting.

**Fixes:**
- `REST_INTER_REQUEST_SLEEP`: 80ms → **250ms** per worker (4 req/s each = 12 req/s total)
- `REST_WORKER_STAGGER`: stagger symbol batch submission by 1s every N symbols
  so workers don't all start REST fetches simultaneously

### False gaps on illiquid coins (HFTUSDT/5m)
Very illiquid coins genuinely have no trades for 5-10 minutes — Binance
simply does not generate a candle for that period. This is not an ingestion
failure but a real market characteristic.

**Fix:** `_GAP_TOLERANCE` is now per-timeframe:
- 5m, 15m: tolerance = **2** (up to 2 missing candles = not a gap)
- All other TFs: tolerance = **1** (1 missing candle = gap)

This prevents the gap checker from repeatedly trying to backfill candles
that don't exist on Binance either.
