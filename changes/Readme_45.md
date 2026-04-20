# PR #45 — Bot name normalization in migration

**Branch:** `dev`
**Date:** 2026-04-20

## Changes (`90_migrate_v3_trades.py`)

### New: `_BOT_NAME_MAP` + `_normalize_bot_name()`

Maps all known V3 bot name variants to unified V4 names.
Applied to all 4 INSERT statements (active/closed trades + ai/closed_ai signals).

### V3 → V4 mapping

| V3 | V4 |
|----|----|
| Fast In And Out | FIO-1 |
| Volume Indicator | VOL-1 |
| 5 Percent | PCT-5 |
| Support Resistance | SR-1 |
| Main Channel | MAIN-1 |
| MIS1-8H / MIS1-8h_pump / MIS1-8h_dump / MSI1-8h_* | MIS-1-8h |
| MIS1-24H / MIS1-24h_pump / MIS1-24h_dump / MSI1-24h_* | MIS-1-24h |
| MIS1-72H / MIS1-72h_pump / MIS1-72h_dump / MSI1-72h_* | MIS-1-72h |
| MIS1-168H / MIS1-168h_pump / MIS1-168h_dump / MSI1-168h_* | MIS-1-168h |
| BR1H / BR2H / BR4H / BR1D | BR-1h / BR-2h / BR-4h / BR-1d |
| BB_1H / BB_4H | BB-1h / BB-4h |
| QM_1H / QM_4H | QM-1h / QM-4h |
| TD_1H / TD_4H | TD-1h / TD-4h |
| ATS1 / ATS1_Robust | ATS-1 |
| ATB1 / AIM1 / ABR1 / RUB1 | ATB-1 / AIM-1 / ABR-1 / RUB-1 |
| SRA1 | SR-1 |
| EPD1 / UFI1 / ROM1 | EPD-1 / UFI-1 / ROM-1 |
| SMC_15M / SMC_30M / SMC_4H | SMC-1 |

Unknown names log a WARNING and are kept as-is.
