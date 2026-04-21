# PR #78–92 — Hotfixes & Improvements (2026-04-21)

**Branch:** `dev`
**Date:** 2026-04-21
**Note:** These changes were committed individually as hotfixes. Consolidated here for traceability.

---

## PR #78 — 20_telegram_bot.py: async rewrite (full V3 feature parity)

Complete rewrite from sync `requests` to `asyncio` + `python-telegram-bot`.

| Feature | Before | After |
|---------|--------|-------|
| Send model | sync `requests` | async `python-telegram-bot` |
| Per-channel rate limit | none | 3,100ms (~19/min, Telegram allows 20) |
| Global rate limit | 50ms | 50ms (~20/s, Telegram allows 30) |
| Smart FIFO | strict FIFO | skips blocked channels, sends to free ones |
| RetryAfter handling | generic retry | exact backoff from Telegram header |
| Attempt counter | marks sent on failure | max 3 attempts → `failed=TRUE` |
| Chart dedup | deletes immediately | only deletes if no other unsent row references it |
| Batch size | 10 | 50 |

Schema: adds `attempts`, `failed`, `last_error` columns to `telegram_outbox` on startup (idempotent).

---

## PR #79 — core/charting.py: fix DateFormatter import

**Bug:** `mticker.DateFormatter` does not exist — `DateFormatter` belongs to `matplotlib.dates`.

**Fix:** Added `from matplotlib.dates import DateFormatter as _DateFormatter`, replaced all `mticker.DateFormatter(` calls.

**Affected:** all 3 chart types.

---

## PR #80 — 31_market_tracker.py: close_reason → outcome

**Bug:** `close_reason` column does not exist in V4 `trades` table. V4 uses `outcome` (WIN/LOSS/BREAKEVEN).

**Fixes:**
- Signal Summary query: `close_reason` → `outcome`
- Per-Bot Performance query: `close_reason` → `outcome`
- `_classify_outcome()`: now reads V4 `outcome` column directly (`WIN`→`win`, `LOSS`→`loss`, `BREAKEVEN`→`neutral`), falls back to pnl_pct calculation for V3 trades and open trades

---

## PR #81 — core/charting.py: emoji glyphs → ASCII

**Bug:** `UserWarning: Glyph 128308 (\N{LARGE RED CIRCLE}) missing from font(s) DejaVu Sans`

**Fix:** Replaced all emoji in chart text (matplotlib render targets) with ASCII equivalents:

| Emoji | ASCII |
|-------|-------|
| 🟢 | `[BULL]` |
| 🔴 | `[BEAR]` |
| ⚡ | `>>` |
| 🔄 | `~` |
| ✅ | `OK` |
| ❌ | `XX` |

Emojis in Telegram message text (outbox) are unchanged.

---

## PR #82 — core/charting.py: mini-chart right clipping fix

**Bug:** Last candle and price line cut off at right edge.

**Fixes:**
- `xlim`: added 3% right padding beyond last candle timestamp
- Price tag: moved from right side (`x=0.05`) to left side (`ha="right"`) — no overflow
- `subplots_adjust`: `right=0.92 → 0.88`

---

## PR #83 — core/charting.py: pattern chart trendline fix

**Bug:** Trendlines drawn at completely wrong price levels (e.g. line at 1.0–2.0 when price is 1.43).

**Root cause:** `slope` and `intercept` from detector use local DataFrame indices (0..199). Chart loaded a different-sized DataFrame and applied same indices → wrong y-values.

**Fix:** Chart now recalculates trendlines from scratch using `scipy.argrelextrema` on the loaded DataFrame's own pivot points. Stored `slope`/`intercept` are ignored for drawing.

**Additional fixes:**
- Figure height: `10 → 8` (less empty space)
- Volume bar panel: `mticker.DateFormatter` → `_DateFormatter`
- Main chart x-labels hidden when volume panel is below

---

## PR #84 — core/charting.py: trendbreaker chart global index alignment

**Bug:** Same index problem as pattern chart but worse — 90d trendline drawn at wrong position.

**Root cause:** Detector computes slope/intercept on 2,160 candles (90d × 24h). Chart loaded only 200 candles with `x = [0..207]` → `slope * 0 + intercept` was way off.

**Fix:** Chart now loads full 2,200 candles, computes `display_start = n_full - 200`, draws trendline with `x = [display_start .. display_start+207]` — identical index space as detector used.

Also fixed `tl_at_last`: was `slope*(n-1)` (local), now `slope*(n_full-1)` (global).

---

## PR #85 — core/charting.py: trendbreaker RSI/TSI panel layout

**Bug:** RSI and TSI panels cut off, Y-axis labels barely visible, large empty space.

**Fixes:**
- Figure height: `12 → 13`
- `height_ratios`: `[5, 1.2, 1.2]` → `[5, 1.5, 1.5]`
- `hspace`: `0.06 → 0.12`
- `bottom` margin: `0.06 → 0.08`
- VBP sidebar: `gs[0,1]` → `gs[:,1]` — spans all 3 rows
- RSI ylabel: colored amber, `fontsize=9`
- TSI ylabel: colored lime, `fontsize=9`

---

## PR #86 — 40_pattern_detector.py: remove duplicate volume in main panel

**Bug:** Volume appeared twice — once in main candle chart (twin axis) and once in the dedicated bottom bar panel.

**Fix:** Removed `ax_vol = ax_main.twinx()` and `_draw_volume()` call from main panel. Volume now only in `ax_vbar` (bottom panel).

---

## PR #87 — 41_trendbreaker_detector.py: cooldown PostgreSQL INTERVAL fix

**Bug:** Cooldown after CONFIRMED/EXPIRED events did not work.

**Root cause:** `INTERVAL '%s hours'` — psycopg2 cannot substitute parameters inside INTERVAL string literals. The check silently failed (`except Exception: return False`), meaning cooldown was never applied.

**Fix:** `INTERVAL '1 hour' * %s` — numeric multiplication, psycopg2 handles correctly.

**Additional:** `except Exception` now logs at `DEBUG` level instead of silently passing.

---

## PR #88 — 41_trendbreaker_detector.py: group-level cooldown

**Bug:** Cooldown checked only exact `event_type` match. BREAK_UP and BREAK_DOWN are different types → a coin that just had BREAK_UP expire could immediately trigger BREAK_DOWN.

**Fix:** `_already_active()` now suppresses the entire direction group:
- `BREAK_UP` expired → blocks both `BREAK_UP` and `BREAK_DOWN` for 6h
- `BOUNCE_UP` expired → blocks both `BOUNCE_UP` and `BOUNCE_DOWN` for 4h

Uses `event_type = ANY(%s)` with the group tuple as parameter.

---

## PR #89 — 51_trendbreaker_trader.py: retest duplicate fix + indicator alignment

**Bug 1:** Retest alert sent every hourly scan while price stayed near trendline.

**Root cause:** `UPDATE` only set `retest_price` but left `state = 'WAITING_RETEST'`. Next scan: same condition → same alert.

**Fix:** `UPDATE` now transitions `state = 'RETEST'`. Local variable updated immediately. Alert fires exactly once.

**Bug 2:** Indicators (EMAs, RSI, TSI) started mid-chart — first half empty.

**Root cause:** `reindex(method="nearest", tolerance="1h")` dropped rows where `ohlcv_1h` and `indicators_1h` timestamps differed by even a few seconds.

**Fix:** Replaced with `pd.merge_asof(tolerance=pd.Timedelta("90min"), direction="nearest")` — robust to small timestamp differences.

---

## PR #90 — 12_indicator_engine.py + 40/41: event-driven scan triggers

**Before:** Pattern Detector (40) and Trendbreaker Detector (41) triggered at `:03` UTC — potentially before the Indicator Engine (12) finished writing fresh 1h indicators. All three competed for CPU simultaneously.

**After:** Event-driven cascade:

```
:00 UTC  →  candle_close_event written
             12 runs (3–8 min)
             12 writes indicators_1h_done timestamp to system_state
                    ↓
             40 polls system_state every 15s → sees new signal → runs scan
             41 polls system_state every 15s → sees new signal → runs scan
```

**Changes:**
- `core/system_state.py`: added `KEY_INDICATORS_1H_DONE = "indicators_1h_done"`
- `12_indicator_engine.py`: writes `KEY_INDICATORS_1H_DONE` after each 1h cycle
- `40_pattern_detector.py`: replaces `:03` time-trigger with `system_state` poll (15s interval)
- `41_trendbreaker_detector.py`: same

**Result:** CPU load distributed over the full hour. Detectors always scan fresh indicators.

---

## PR #91 — core/schema.py + 51_trendbreaker_trader.py: RETEST state CHECK constraint

**Bug:** `trendline_events_state_check` constraint did not include `'RETEST'`. Every `UPDATE SET state='RETEST'` failed with a PostgreSQL constraint violation. Trader crashed and retried every 60 seconds in an infinite loop.

**Fixes:**

**`core/schema.py`:**
- Added `'RETEST'` to CHECK constraint definition
- Added `'RETEST'` to partial index `idx_trendline_events_active`
- Added `DO $$` migration block that drops and recreates the constraint on existing DBs — runs automatically on next `create_all_tables()` (= bot startup)

**`51_trendbreaker_trader.py`:**
- Wrapped RETEST UPDATE in `try/except` with `conn.rollback()` + `continue`
- Prevents infinite retry loop if constraint violation occurs on older DBs

**DB migration:** Runs automatically on startup via `bootstrap.py` → `create_all_tables()`. No manual SQL required.
