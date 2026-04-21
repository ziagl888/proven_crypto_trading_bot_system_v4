# PR #77 — Unified chart design system: pattern + trendbreaker charts

**Branch:** `dev`
**Date:** 2026-04-21

## `core/charting.py` — complete rewrite

Three chart functions with a unified design language:

### Design tokens (shared across all charts)
| Token | Color | Usage |
|-------|-------|-------|
| Background | `#080c14` | Deep navy-black |
| Panel | `#0d1421` | Subplot backgrounds |
| Grid | `#1a2540` | Subtle blue-grey grid |
| Bull | `#00e676` | Green candles / bull signals |
| Bear | `#ff1744` | Red candles / bear signals |
| Price line | `#00e5ff` | Cyan price line |
| Accent 1 | `#ff9100` | Orange: trendlines, breakouts |
| Accent 2 | `#d500f9` | Violet: pivot markers |
| Accent 3 | `#ffea00` | Yellow: pattern lower boundary |
| VBP | `#e040fb` | Pink: volume profile |
| RSI | `#ffd740` | Amber |
| TSI | `#69ff47` | Lime |

All charts share: `_draw_candles()`, `_draw_volume()`, `_draw_vbp()`,
`_draw_price_line()`, `_draw_price_tag()`, `_header_bar()`, `_style_ax()`

### `generate_chart()` — unchanged API, updated styling
Standard mini-chart for PD-1, whale alerts etc. 5m candles + VBP.

### `generate_pattern_chart()` — NEW
Replaces V3 mplfinance-based pattern chart.

Layout: main candlestick panel + volume bar panel (below) + VBP sidebar
- Upper boundary: orange line (resistance)
- Lower boundary: yellow line (support)
- Pattern fill zone: faint tint between boundaries
- +10 candle projection forward
- State badge: BREAKOUT / WAITING RETEST / RETEST / CONFIRMED / FAKEOUT
- Break price horizontal (orange dashed) + retest price (cyan dotted)
- Volume histogram below main panel

### `generate_trendbreaker_chart()` — NEW
Replaces V3 "Megageil Chart".

Layout: main panel (5) + RSI panel (1.2) + TSI panel (1.2) + VBP sidebar
- 1h candles (last 200 bars)
- 90d trendline (thick, bull/bear colored) + ±0.5% band
- Pivot markers (violet circles) via scipy.argrelextrema
- EMA9 / EMA21 / EMA200 overlays from indicators_1h table
- Break: directional arrow from trendline to break price
- Bounce: circle at trendline touch point
- Info label: Distance%, Volume ratio, ML score
- RSI-14: with overbought/oversold fill
- TSI: with signal line + bullish/bearish fill between lines

## `40_pattern_detector.py` — chart calls updated
All 3 `generate_chart()` calls replaced with `generate_pattern_chart()`
with correct `event_state` per context:
- Breakout: `event_state="BREAKOUT"`, `break_price=c_close`
- Fakeout: `event_state="FAKEOUT"`
- Retest: `event_state="RETEST"`, `retest_price=(c_high+c_low)/2`

## `41_trendbreaker_detector.py` — chart calls updated
All 4 `generate_chart()` calls replaced with `generate_trendbreaker_chart()`
with correct `event_type` per context:
- BREAK_UP, BREAK_DOWN, BOUNCE_UP, BOUNCE_DOWN
