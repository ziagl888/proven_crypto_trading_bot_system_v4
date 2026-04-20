# PR #37 — Indicator engine: reduce log spam + add phase timing

**Branch:** `dev`
**Date:** 2026-04-20

## Changes (`12_indicator_engine.py`)

### Gap fill log spam
Per-symbol gap fill logs downgraded from INFO to DEBUG.
Instead, a summary is logged at the end:
```
[30m] Indicator gap fill complete — 572 symbols, 572 rows written.
```

### Phase timing in cycle
Added DEBUG-level timing for each phase of the cycle:
```
[30m] OHLCV load: 2.1s (286000 rows)
[30m] Calculation: 25.3s (570 symbols)
[30m] DB write took 3.2s for 570 rows
[30m] Cycle complete — 570 rows written, 570 symbols, 30.1s.
```

This tells us exactly where the 30s comes from — suspected to be
the calculation phase (570 × pandas indicator math × ThreadPoolExecutor).
Next optimization step: increase NUM_WORKERS or pre-compute indicators
more efficiently.
