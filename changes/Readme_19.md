# PR #19 — Migration script rewrite: correct V3 column mapping

**Branch:** `dev`
**Date:** 2026-04-20

## Problem with previous migration script

Analysis of actual V3 source code revealed 5 critical bugs:

| Bug | Previous (wrong) | Correct |
|-----|-----------------|---------|
| active_trades_master status | mapped 'WIN'/'LOSS' strings | V3 uses '0'=SL, '1'-'4'=TP numeric |
| closed_trades_master columns | read `opened_at`, `closed_at` | V3 has `time` (open) and `posted` (close) |
| ai_signals | read `open_time`, `status` | status is always 'OPEN', open_time correct |
| closed_ai_signals columns | read `opened_at`, `closed_at` | V3 has `open_time`, `close_time` |
| closed_ai_signals status | mapped as numeric | V3 stores free-text close_reason |

## Rewritten: `90_migrate_v3_trades.py`

### Correct V3 → V4 status mapping

**active/closed_trades_master:**
| V3 status | V4 status | outcome |
|-----------|-----------|---------|
| `'WORKING'` | `OPEN` | NULL |
| `'0'` | `CLOSED_SL` | LOSS |
| `'1'` | `CLOSED_TP` | WIN (tp_hit=1) |
| `'2'` | `CLOSED_TP` | WIN (tp_hit=2) |
| `'3'` | `CLOSED_TP` | WIN (tp_hit=3) |
| `'4'` | `CLOSED_TP` | WIN (tp_hit=4) |

**closed_ai_signals (free-text close_reason):**
| V3 reason | V4 status | outcome |
|-----------|-----------|---------|
| `'SL Hit (SL: ...)'` | `CLOSED_SL` | LOSS |
| `'ALL TARGETS HIT'` | `CLOSED_TP` | WIN |
| `'LEGACY TARGET HIT (+2.5%)'` | `CLOSED_TP` | WIN |
| `'LEGACY FALLBACK SL (-5.0%)'` | `CLOSED_SL` | LOSS |
| `''` (empty) | `CLOSED_MANUAL` | NULL |

### Correct column mapping per table

**active_trades_master:** `time`→`opened_at`, `posted`→`closed_at`
**closed_trades_master:** `time`→`opened_at`, `posted`→`closed_at`
**ai_signals:** `open_time`→`opened_at`, all targets from JSON array
**closed_ai_signals:** `open_time`→`opened_at`, `close_time`→`closed_at`

### Verified
- Syntax: OK
- All `_map_numeric_status()` cases: ✅
- All `_map_close_reason()` cases: ✅
- pnl_r calculation (LONG/SHORT, TP/SL): ✅
