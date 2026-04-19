# PR #3 — Hotfix: leverage fetch 401 (timestamp drift)

**Branch:** `dev`
**Date:** 2026-04-19

## Problem

`fetch_max_leverage()` returned a 401 Unauthorized error from Binance.

**Root cause:** Binance Futures rejects signed requests where the `timestamp`
parameter deviates more than ±1000ms from Binance server time. On Windows,
system clocks drift more than on Linux, causing the locally generated timestamp
to fall outside the acceptance window.

## Fix (`core/bootstrap.py`)

- Added a `GET /fapi/v1/time` call before the signed request to fetch the
  exact Binance server timestamp
- Use `serverTime` from that response as the `timestamp` parameter instead
  of `int(time.time() * 1000)`
- Added `recvWindow: 10000` (10 seconds) as an extra safety margin
- The time-sync call adds one lightweight HTTP request (~50ms) but makes the
  signature reliable on all platforms including Windows
