# core/config.py
# Central configuration for the entire V4 system.
# All secrets and channel IDs are read from the .env file (python-dotenv)
# or environment variables. Never hardcode secrets here.

import os
from dotenv import load_dotenv

load_dotenv()


def _required(name: str) -> str:
    """Reads a required environment variable. Raises RuntimeError if missing."""
    val = os.getenv(name)
    if not val:
        raise RuntimeError(
            f"Required environment variable '{name}' is missing. "
            f"Please set it in .env (see .env.example)."
        )
    return val


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


# ── Database ──────────────────────────────────────────────────────────────────
DB_NAME     = os.getenv("DB_NAME", "cryptodata")
DB_USER     = os.getenv("DB_USER", "dbfiller")
DB_PASSWORD = _required("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = _int("DB_PORT", 5432)

# ── Binance ───────────────────────────────────────────────────────────────────
BASE_URL       = "https://fapi.binance.com"
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_SECRET  = os.getenv("BINANCE_SECRET", "")

# Timeframes stored in OHLCV tables (data ingestion scope)
# Full range from 10s to 1 month — actual ingestion targets defined per-bot
TIMEFRAMES_ALL = ["10s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]

# Timeframes actively used by the indicator engine
INDICATOR_TIMEFRAMES = ["30m", "1h", "2h", "4h", "1d", "1w"]

# Timeframes actively ingested via WebSocket + REST
INGEST_TIMEFRAMES = ["5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"]

NUM_WORKERS = _int("NUM_WORKERS", 16)

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = _required("TELEGRAM_BOT_TOKEN")

# ── Telegram Channels ────────────────────────────────────────────────────────
# Market monitoring (PD-1 Pump/Dump Detector)
PUMP_DUMP_MARKET_CHANNEL_ID = _int("PUMP_DUMP_MARKET_CHANNEL_ID", 0)
PUMP_DUMP_AI_CHANNEL_ID     = _int("PUMP_DUMP_AI_CHANNEL_ID",     0)

# Market Tracker (hourly reports, gainers/losers, volume)
SENTIMENT_CHANNEL_ID        = _int("SENTIMENT_CHANNEL_ID",        0)

# Regime Orchestrator
REGIME_TRADING_CHANNEL_ID   = _int("REGIME_TRADING_CHANNEL_ID",   0)
REGIME_STATUS_CHANNEL_ID    = _int("REGIME_STATUS_CHANNEL_ID",    0)

# Classical channel bots (FIO-1, VOL-1, PCT-5, SR-1, MAIN-1)
FIO1_CHANNEL_ID   = _int("FIO1_CHANNEL_ID",   0)   # Fast In And Out
VOL1_CHANNEL_ID   = _int("VOL1_CHANNEL_ID",   0)   # Volume Indicator
PCT5_CHANNEL_ID   = _int("PCT5_CHANNEL_ID",   0)   # 5 Percent
SR1_CHANNEL_ID    = _int("SR1_CHANNEL_ID",    0)   # Support Resistance
MAIN1_CHANNEL_ID  = _int("MAIN1_CHANNEL_ID",  0)   # Main Channel

# MIS bot channels — one per timeframe horizon
MIS_CHANNELS = {
    "8h":   _int("MIS_CHANNEL_8H",   0),
    "24h":  _int("MIS_CHANNEL_24H",  0),
    "72h":  _int("MIS_CHANNEL_72H",  0),
    "168h": _int("MIS_CHANNEL_168H", 0),
}

# AI bots
ATS1_CHANNEL_ID   = _int("ATS1_CHANNEL_ID",   0)   # ATS-1
ATB1_CHANNEL_ID         = _int("ATB1_CHANNEL_ID",         0)   # ATB-1 trade channel
ATB1_INFO_CHANNEL_ID    = _int("ATB1_INFO_CHANNEL_ID",    0)   # ATB-1 info/detection channel
PATTERN_INFO_CHANNEL_ID = _int("PATTERN_INFO_CHANNEL_ID", 0)   # Pattern Detector info channel
AIM1_CHANNEL_ID   = _int("AIM1_CHANNEL_ID",   0)   # AIM-1
ABR1_CHANNEL_ID   = _int("ABR1_CHANNEL_ID",   0)   # ABR-1
RUB1_CHANNEL_ID   = _int("RUB1_CHANNEL_ID",   0)   # RUB-1
SRA1_CHANNEL_ID   = _int("SRA1_CHANNEL_ID",   0)   # SRA-1
EPD1_CHANNEL_ID   = _int("EPD1_CHANNEL_ID",   0)   # EPD-1
UFI1_CHANNEL_ID   = _int("UFI1_CHANNEL_ID",   0)   # UFI-1

# Pattern / SMC bots
BB_CHANNELS = {
    "1h": _int("BB_CHANNEL_1H", 0),
    "4h": _int("BB_CHANNEL_4H", 0),
}
BR_CHANNELS = {
    "1h": _int("BR_CHANNEL_1H", 0),
    "2h": _int("BR_CHANNEL_2H", 0),
    "4h": _int("BR_CHANNEL_4H", 0),
    "1d": _int("BR_CHANNEL_1D", 0),
}
QM_CHANNELS = {
    "1h": _int("QM_CHANNEL_1H", 0),
    "4h": _int("QM_CHANNEL_4H", 0),
}
TD_CHANNELS = {
    "1h": _int("TD_CHANNEL_1H", 0),
    "4h": _int("TD_CHANNEL_4H", 0),
}

# ── Coin filter ───────────────────────────────────────────────────────────────
# Main Channel Bot only runs on these symbols
MAIN_CHANNEL_COINS = [
    "BTCUSDT", "ETHUSDT", "XRPUSDT", "LINKUSDT", "ADAUSDT", "BNBUSDT",
    "DOGEUSDT", "AVAXUSDT", "AAVEUSDT", "HBARUSDT", "BTCDOMUSDT", "ENSUSDT",
    "GRTUSDT", "INJUSDT", "FETUSDT", "ETHBTC", "SUIUSDT", "PENDLEUSDT",
    "SEIUSDT", "ONDOUSDT", "TONUSDT", "ETHFIUSDT", "ENAUSDT", "TAOUSDT",
    "RENDERUSDT", "BRETTUSDT", "EIGENUSDT", "IPUSDT", "HYPEUSDT", "LTCUSDT",
    "BCHUSDT", "APTUSDT", "CRVUSDT", "SOLUSDT", "UNIUSDT", "NEARUSDT",
    "JUPUSDT", "BERAUSDT",
]

# ── Dashboard ─────────────────────────────────────────────────────────────────
DASHBOARD_PORT = _int("DASHBOARD_PORT", 5000)




