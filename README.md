# Crypto Trading Bot System V4

Multi-bot Binance Futures trading system with Telegram signalling via Cornix.
30+ trading bots, PostgreSQL storage, central indicator engine, ML-based signal generation.

## ⚠️ Important

**This code trades with real money.** Never push changes directly to `main` — always use branches + PRs. Changes to trading logic require paper-trading verification before merging to `main`.

## What's new in V4 vs V3

- **Unified DB schema**: one table per timeframe (not one table per coin) — eliminates 7.500+ micro-tables
- **BRIN + composite indexes** on OHLCV and indicator tables — major I/O reduction
- **Trimmed indicator set**: only columns actually consumed by bots are stored (91 → 67 columns)
- **File naming convention**: numeric prefix for clear load order and purpose grouping
- **All config in `.env`**: no channel IDs or tokens hardcoded in source files
- **Larger connection pool**: max 20 connections (up from 8)

## Architecture

```
00_main_watchdog.py
  │
  ├─ Bootstrap (core/bootstrap.py)
  │    ├─ fetch coins.json  (Binance exchangeInfo)
  │    ├─ fetch max_leverage.json  (Binance leverageBracket)
  │    └─ init DB schema  (core/schema.py)
  │
  ├─ 10_data_ingestion.py      WebSocket + REST → ohlcv_* tables
  ├─ 11_chart_data_service.py  Chart data aggregation
  ├─ 12_indicator_engine.py    ohlcv_* → indicators_* tables (every 30 min)
  ├─ 13_detectors.py           indicators_* → telegram_outbox
  │
  ├─ 20_telegram_bot.py        Consumes telegram_outbox
  ├─ 21_trade_monitor.py       Classic trade SL/TP trailing
  ├─ 22_ai_trade_monitor.py    AI signal SL/TP management
  ├─ 23_housekeeping.py        Nightly cleanup & leverage refresh
  │
  ├─ 30–50  Trading bots (pattern, AI, SMC, regime, …)
  │
  └─ 70_dashboard.py           Web status dashboard
```

## File naming convention

| Range | Purpose |
|-------|---------|
| `00`  | Watchdog / entry point |
| `10–19` | Core data services (ingestion, indicators) |
| `20–29` | Infrastructure (telegram, trade monitors, housekeeping) |
| `30–69` | Trading bots and strategy modules |
| `70–89` | Dashboard, utilities, chart services |
| `90–99` | Backtests and model training (run manually) |

## Quickstart

```bash
# 1. Clone and enter repo
git clone <repo-url>
cd proven_crypto_trading_bot_system_v4

# 2. Virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env — fill in DB credentials, Binance keys, Telegram token + channel IDs

# 5. Create PostgreSQL database
createdb cryptodata

# 6. Start (bootstrap runs automatically)
python 00_main_watchdog.py
```

On first start the watchdog will:
1. Fetch all active Binance Futures pairs → `coins.json`
2. Fetch max leverage for each pair → `max_leverage.json`
3. Create all DB tables with correct schema and indexes
4. Start all configured processes in staggered order

## DB Schema

### OHLCV tables
One table per timeframe: `ohlcv_5m`, `ohlcv_1h`, `ohlcv_4h`, etc.

```sql
CREATE TABLE ohlcv_1h (
    symbol    TEXT        NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    open      DOUBLE PRECISION,
    high      DOUBLE PRECISION,
    low       DOUBLE PRECISION,
    close     DOUBLE PRECISION,
    volume    DOUBLE PRECISION,
    PRIMARY KEY (symbol, open_time)
);
```

### Indicator tables
One table per timeframe (indicator scope only): `indicators_1h`, `indicators_4h`, etc.
Contains only the 67 columns actually consumed by bots (audited).

## ML Models

Not in repo — too large for version control. Required files (in `models/`):
- `bt2_model_LONG.json`, `bt2_model_SHORT.json` (ABR1)
- `pump_model_*_final.pkl`, `threshold_*_final.pkl` (MIS — 8 horizons × 2 directions)
- `pump_dump_model.pkl` (Pump/Dump Detector)
- `ats_model.pkl`, `rub_model_long.pkl`, `rub_model_short.pkl`
- `atb_model.pkl`

## Branch strategy

| Branch | Purpose |
|--------|---------|
| `main` | Production (deployed) |
| `dev`  | Integration — all PRs target this branch |
| `feature/<n>` | New features |
| `fix/<desc>`  | Bug fixes |

## Change log format

Every PR includes a file `changes/Readme_<PR#>.md` listing all changes.
