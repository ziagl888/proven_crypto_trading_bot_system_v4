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

# Strategy signal channels (Cornix listens here)
TELEGRAM_CHANNELS = {
    "Fast In And Out":   _int("TELEGRAM_CHANNEL_FAST_IN_OUT", -1002928960725),
    "5 Percent":         _int("TELEGRAM_CHANNEL_5_PERCENT",   -1002528903317),
    "Main Channel":      _int("TELEGRAM_CHANNEL_MAIN",        -1002214219814),
    "Support Resistance":_int("TELEGRAM_CHANNEL_SR",          -1002767867624),
    "Volume Indicator":  _int("TELEGRAM_CHANNEL_VOLUME",      -1002995492163),
    "Pattern Detector":  _int("TELEGRAM_CHANNEL_PATTERN",     -1003808675239),
}

# Regime Orchestrator
REGIME_TRADING_CHANNEL_ID = _int("REGIME_TRADING_CHANNEL_ID", -1003963430969)
REGIME_STATUS_CHANNEL_ID  = _int("REGIME_STATUS_CHANNEL_ID",  -1003726330371)

# Individual bot channels
UFI1_CHANNEL_ID           = _int("UFI1_CHANNEL_ID",           -1003886743032)
AI_MASTER_CHANNEL_ID      = _int("AI_MASTER_CHANNEL_ID",      -1003489268014)
ATS_CHANNEL_ID            = _int("ATS_CHANNEL_ID",            -1003893440623)
RUBBERBAND_CHANNEL_ID     = _int("RUBBERBAND_CHANNEL_ID",     -1003839488401)
ATB_CHANNEL_ID            = _int("ATB_CHANNEL_ID",            -1003550392848)
ATB_INFO_CHANNEL_ID       = _int("ATB_INFO_CHANNEL_ID",       -1003152405662)
ABR1_CHANNEL_ID           = _int("ABR1_CHANNEL_ID",           -1003566031795)
BTC_SMC_CHANNEL_ID        = _int("BTC_SMC_CHANNEL_ID",        -1003833266588)
QUASIMODO_CHANNEL_ID      = _int("QUASIMODO_CHANNEL_ID",      -1003779126169)
MAYANK_CHANNEL_ID         = _int("MAYANK_CHANNEL_ID",         -1003758733404)
SMC_FOREX_XAUUSD_CHANNEL_ID = _int("SMC_FOREX_XAUUSD_CHANNEL_ID", -1003747323642)
SMC_FOREX_XAGUSD_CHANNEL_ID = _int("SMC_FOREX_XAGUSD_CHANNEL_ID", -1003749698867)

PUMP_DUMP_MARKET_CHANNEL_ID    = _int("PUMP_DUMP_MARKET_CHANNEL_ID",    -1003530985261)
PUMP_DUMP_AI_CHANNEL_ID        = _int("PUMP_DUMP_AI_CHANNEL_ID",        -1003025988001)
PUMP_DUMP_MAIN_CHANNEL_ID      = _int("PUMP_DUMP_MAIN_CHANNEL_ID",      -1003205666153)
SENTIMENT_CHANNEL_ID = _int("SENTIMENT_CHANNEL_ID", -1003726330371)

MIS_CHANNELS = {
    "1h":  _int("MIS_CHANNEL_1H",  -1003420431187),
    "2h":  _int("MIS_CHANNEL_2H",  -1003420431187),
    "4h":  _int("MIS_CHANNEL_4H",  -1003420431187),
    "8h":  _int("MIS_CHANNEL_8H",  -1003420431187),
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


