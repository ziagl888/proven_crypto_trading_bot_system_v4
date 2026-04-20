# PR #54 — core/charting.py + chart in PD-1 alerts

**Branch:** `dev`
**Date:** 2026-04-20

## New: `core/charting.py`

Mini-chart generator for Telegram alerts.

### V4 changes vs V3
- Reads from `ohlcv_5m` table (no chart_data_service dependency)
- Thread-safe via `_CHART_LOCK`
- Unique filenames (millisecond timestamp) — no race conditions
- Charts saved to `charts/` directory

### API
```python
from core.charting import generate_chart
chart_path = generate_chart("BTCUSDT", minutes=240)
# → "charts/BTCUSDT_1745154000123_chart.png" or None
```

### Features
- 5m candlesticks with wicks and bodies
- Price line overlay (1min resolution via 5m close prices)
- Volume bars (color-coded buy/sell)
- Volume profile (VBP) on right side
- Optional spike highlight region (spike_start/spike_end)
- Dark theme (#0d0d0d background)

## Updated: `30_pump_dump_detector.py`
Both price move and volume explosion alerts now include a chart image.
