# PR #22 — Simulation: include open trades, verify and correct status

**Branch:** `dev`
**Date:** 2026-04-20

## Change

Simulation now processes ALL V3 trades (open + closed), not just closed ones.

## Open trade outcomes

| Simulation result | Action |
|-------------------|--------|
| SL/TP hit during simulation | → `CLOSED_SL` or `CLOSED_TP`, full result written |
| TP1+ hit but not all TPs, no SL hit | → stays `OPEN`, tp_hit + position_pct + trailing SL updated |
| Nothing hit, trade still running | → stays `OPEN`, no changes |

## Why this matters

V3 open trades may have been orphaned (bot was down, Cornix closed them
but DB was never updated). Simulation catches these and marks them closed.
Truly open trades remain OPEN and are picked up by the V4 trade monitor.

## Verified (unit tests)

- Open trade, no SL/TP hit → OPEN ✅
- Open trade, SL hit → CLOSED_SL LOSS ✅
- Open trade, TP1 hit, TP2 not hit → OPEN tp_hit=1 pos=0.5 ✅
