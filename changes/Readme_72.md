# PR #72 — 40_pattern_detector.py + 41_trendbreaker_detector.py

**Branch:** `dev`
**Date:** 2026-04-21

## New numbering scheme
- `3x` — Market Monitoring (PD-1, Market Tracker)
- `4x` — Detectors (no trades, info only)
- `5x` — Trading Bots

## New: `40_pattern_detector.py`
Geometric chart pattern detector (Triangle, Channel).

**V4 improvements vs V3:**
| Parameter | V3 | V4 |
|-----------|----|----|
| Pivot detection | `rolling(9).max()` | `scipy.argrelextrema(order=5)` |
| Flat threshold | `0.02%` | `0.10%` (5× stricter) |
| Retest body | `0.4%` | `0.5%` |
| State persistence | JSON file | DB (`pattern_events`) |

## New: `41_trendbreaker_detector.py`
Trendline break & bounce detector (90d, 1h candles).

**V4 improvements vs V3:**
| Parameter | V3 | V4 |
|-----------|----|----|
| Break condition | `prev in [below/near/unknown]` | **`prev below AND curr above`** (strict) |
| Break min distance | 0.8% | **0.5%** |
| Volume confirmation | none | **≥1.5× avg** |
| Bounce radar | 5% | **2%** |
| State persistence | JSON file | DB (`trendline_events`) |

## New DB tables (`core/schema.py`)
- `pattern_events` — lifecycle: BREAKOUT → WAITING_RETEST → RETEST → CONFIRMED/FAKEOUT/EXPIRED
- `trendline_events` — lifecycle: DETECTED → WAITING_RETEST → CONFIRMED/EXPIRED

## New config entries
- `ATB1_INFO_CHANNEL_ID` — trendbreaker detection alerts
- `PATTERN_INFO_CHANNEL_ID` — pattern detection alerts
