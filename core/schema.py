# core/schema.py
# Database schema initialisation for V4.
#
# Scope at this stage:
#   - OHLCV tables       (one per timeframe, 10s – 1M)
#   - Indicator tables   (one per timeframe, active TFs only)
#   - telegram_outbox    (needed as soon as any bot sends signals)
#   - trade_cooldowns    (needed by all bots for duplicate prevention)
#
# Tables deliberately NOT created here yet:
#   - Trade tracking tables  (model not yet designed)
#   - AI / ML signal tables  (model not yet designed)
#   - Regime tables          (model not yet designed)
#
# Each of these will get its own schema migration in a dedicated PR
# once the data model has been agreed upon.

from __future__ import annotations

import logging
from core.database import db_connection

logger = logging.getLogger(__name__)

# ── Timeframes ────────────────────────────────────────────────────────────────

# All timeframes for which OHLCV tables are created.
OHLCV_TIMEFRAMES = [
    "10s", "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
]

# Timeframes for which indicator tables are created.
# Subset of OHLCV_TIMEFRAMES — only what bots actually read (audited).
INDICATOR_TIMEFRAMES = ["30m", "1h", "2h", "4h", "1d", "1w"]

# ── Audited indicator columns ─────────────────────────────────────────────────
# Every column here is consumed by at least one bot (verified audit of V3 code).
# Columns NOT used by any bot have been deliberately omitted.
#
# Usage map (short form):
#   rsi_6/9/12/14/24            strat_fast_in_out, strat_5%, strat_sr, strat_main,
#                                9_ai_sr, 10_pump_dump, 11_mis, 13_rub, 24_qm, 25_sniper
#   ema_7..200                   11_mis, 12_ats, 14_atb, 15_master, 24_qm, 25_sniper,
#                                strat_5%, strat_fast_in_out
#   ma_7..200                    15_master (full normalisation)
#   wma_7..200                   15_master, strat_5%, strat_fast_in_out
#   smma_*                       15_master (full normalisation)
#   kama_7..99                   12_ats, 14_atb, 15_master, strat_5%, strat_fast_in_out
#   atr_9/14/21                  strat_fast_in_out, strat_main, 12_ats, 15_master
#   tsi_25_13_13(_signal)        24_qm, 25_sniper
#   tsi_fast_12_7_7(_signal)     strat_fast_in_out, strat_5%, 12_ats, 13_rub, 18_abr1
#   macd_dif/dea_fast_9_21_9     strat_fast_in_out, strat_5%
#   macd_dif/dea_normal_12_26_9  10_pump_dump, 12_ats, 13_rub, 15_master, 24_qm, 25_sniper
#   boll_upper/mid/lower_20      11_mis, 12_ats, 13_rub, 15_master, 18_abr1, 24_qm
#   donchian_*_4                 strat_fast_in_out, strat_5%
#   donchian_*_10/12/15          15_master
#   donchian_*_20                12_ats, 13_rub, 15_master, 18_abr1, 24_qm, 25_sniper
#   trendline_*/channel_*        12_ats, 15_master
#   mid_line, r_squared          15_master
#   trend_direction              12_ats, 15_master, 24_qm, 25_sniper
#   support_price/resistance_price  all strategy files + 9_ai_sr, 12_ats, 15_master
#   hvn_1/2/3, poc               15_master
#   fib_support/resistance_*     15_master
#   fib_extension_*              15_master

INDICATOR_COLUMNS: list[tuple[str, str]] = [
    # RSI
    ("rsi_6",  "REAL"), ("rsi_9",  "REAL"), ("rsi_12", "REAL"),
    ("rsi_14", "REAL"), ("rsi_24", "REAL"),
    # EMA
    ("ema_7",  "REAL"), ("ema_9",  "REAL"), ("ema_12", "REAL"),
    ("ema_21", "REAL"), ("ema_26", "REAL"), ("ema_34", "REAL"),
    ("ema_50", "REAL"), ("ema_55", "REAL"), ("ema_89", "REAL"),
    ("ema_99", "REAL"), ("ema_200","REAL"),
    # SMA
    ("ma_7",   "REAL"), ("ma_10",  "REAL"), ("ma_20",  "REAL"),
    ("ma_25",  "REAL"), ("ma_50",  "REAL"), ("ma_99",  "REAL"),
    ("ma_100", "REAL"), ("ma_200", "REAL"),
    # WMA
    ("wma_7",  "REAL"), ("wma_9",  "REAL"), ("wma_12", "REAL"),
    ("wma_21", "REAL"), ("wma_26", "REAL"), ("wma_34", "REAL"),
    ("wma_50", "REAL"), ("wma_55", "REAL"), ("wma_89", "REAL"),
    ("wma_99", "REAL"), ("wma_200","REAL"),
    # SMMA
    ("smma_10", "REAL"), ("smma_20",  "REAL"), ("smma_25",  "REAL"),
    ("smma_50", "REAL"), ("smma_99",  "REAL"), ("smma_100", "REAL"),
    ("smma_200","REAL"),
    # KAMA
    ("kama_7",  "REAL"), ("kama_9",  "REAL"), ("kama_12", "REAL"),
    ("kama_21", "REAL"), ("kama_26", "REAL"), ("kama_34", "REAL"),
    ("kama_50", "REAL"), ("kama_55", "REAL"), ("kama_89", "REAL"),
    ("kama_99", "REAL"),
    # ATR
    ("atr_9",  "REAL"), ("atr_14", "REAL"), ("atr_21", "REAL"),
    # TSI
    ("tsi_25_13_13",            "REAL"), ("tsi_25_13_13_signal",      "REAL"),
    ("tsi_fast_12_7_7",         "REAL"), ("tsi_fast_12_7_7_signal",   "REAL"),
    # MACD
    ("macd_dif_fast_9_21_9",    "REAL"), ("macd_dea_fast_9_21_9",    "REAL"),
    ("macd_dif_normal_12_26_9", "REAL"), ("macd_dea_normal_12_26_9", "REAL"),
    # Bollinger Bands
    ("boll_upper_20", "REAL"), ("boll_mid_20", "REAL"), ("boll_lower_20", "REAL"),
    # Donchian Channels
    ("donchian_upper_4",  "REAL"), ("donchian_lower_4",  "REAL"), ("donchian_mid_4",  "REAL"),
    ("donchian_upper_10", "REAL"), ("donchian_lower_10", "REAL"), ("donchian_mid_10", "REAL"),
    ("donchian_upper_12", "REAL"), ("donchian_lower_12", "REAL"), ("donchian_mid_12", "REAL"),
    ("donchian_upper_15", "REAL"), ("donchian_lower_15", "REAL"), ("donchian_mid_15", "REAL"),
    ("donchian_upper_20", "REAL"), ("donchian_lower_20", "REAL"), ("donchian_mid_20", "REAL"),
    # Trendline & Channel
    ("trendline_slope",     "REAL"), ("trendline_intercept", "REAL"),
    ("trendline_price",     "REAL"), ("channel_upper_price", "REAL"),
    ("channel_lower_price", "REAL"), ("mid_line",            "REAL"),
    ("r_squared",           "REAL"), ("trend_direction",     "TEXT"),
    # Support / Resistance
    ("support_price", "REAL"), ("resistance_price", "REAL"),
    # HVN / POC
    ("hvn_1", "REAL"), ("hvn_2", "REAL"), ("hvn_3", "REAL"), ("poc", "REAL"),
    # Fibonacci retracement
    ("fib_support_0_236",   "REAL"), ("fib_resistance_0_236",   "REAL"),
    ("fib_support_0_382",   "REAL"), ("fib_resistance_0_382",   "REAL"),
    ("fib_support_0_5",     "REAL"), ("fib_resistance_0_5",     "REAL"),
    ("fib_support_0_618",   "REAL"), ("fib_resistance_0_618",   "REAL"),
    ("fib_support_0_786",   "REAL"), ("fib_resistance_0_786",   "REAL"),
    # Fibonacci extensions
    ("fib_extension_1_272", "REAL"),
    ("fib_extension_1_618", "REAL"),
    ("fib_extension_2_618", "REAL"),
]


# ── DDL helpers ───────────────────────────────────────────────────────────────

def _ohlcv_table_ddl(tf: str) -> str:
    tname = f"ohlcv_{tf}"
    return f"""
        CREATE TABLE IF NOT EXISTS {tname} (
            symbol      TEXT             NOT NULL,
            open_time   TIMESTAMPTZ      NOT NULL,
            open        DOUBLE PRECISION NOT NULL,
            high        DOUBLE PRECISION NOT NULL,
            low         DOUBLE PRECISION NOT NULL,
            close       DOUBLE PRECISION NOT NULL,
            volume      DOUBLE PRECISION NOT NULL,
            PRIMARY KEY (symbol, open_time)
        );
        CREATE INDEX IF NOT EXISTS idx_{tname}_time_brin
            ON {tname} USING BRIN (open_time);
        CREATE INDEX IF NOT EXISTS idx_{tname}_sym_time
            ON {tname} (symbol, open_time DESC);
    """


def _indicator_table_ddl(tf: str) -> str:
    tname = f"indicators_{tf}"
    col_defs = ",\n            ".join(
        f"{col}  {dtype}" for col, dtype in INDICATOR_COLUMNS
    )
    return f"""
        CREATE TABLE IF NOT EXISTS {tname} (
            symbol      TEXT        NOT NULL,
            open_time   TIMESTAMPTZ NOT NULL,
            close       REAL        NOT NULL,
            {col_defs},
            PRIMARY KEY (symbol, open_time)
        );
        CREATE INDEX IF NOT EXISTS idx_{tname}_time_brin
            ON {tname} USING BRIN (open_time);
        CREATE INDEX IF NOT EXISTS idx_{tname}_sym_time
            ON {tname} (symbol, open_time DESC);
    """


def _infrastructure_tables_ddl() -> str:
    """
    Minimal infrastructure tables needed from day one.
    Trade / AI / regime tables are NOT included here — they will be
    added in dedicated PRs once the data models have been designed.
    """
    return """
        -- Telegram outbox: all bots write here, 20_telegram_bot.py consumes it.
        CREATE TABLE IF NOT EXISTS telegram_outbox (
            id          SERIAL PRIMARY KEY,
            channel_id  BIGINT       NOT NULL,
            message     TEXT         NOT NULL,
            image_path  TEXT,
            sent        BOOLEAN      NOT NULL DEFAULT FALSE,
            created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_tg_outbox_unsent
            ON telegram_outbox (sent, id)
            WHERE sent = FALSE;

        -- Per-bot cooldowns: prevents duplicate signals.
        CREATE TABLE IF NOT EXISTS trade_cooldowns (
            module          TEXT        NOT NULL,
            coin            TEXT        NOT NULL,
            direction       TEXT        NOT NULL,
            last_posted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (module, coin, direction)
        );
    """


# ── TimescaleDB support ───────────────────────────────────────────────────────

_TIMESCALE_CHUNK_INTERVALS: dict[str, str] = {
    "10s": "1 day",   "1m":  "3 days",  "3m":  "7 days",
    "5m":  "7 days",  "15m": "14 days", "30m": "14 days",
    "1h":  "30 days", "2h":  "30 days", "4h":  "60 days",
    "6h":  "60 days", "8h":  "60 days", "12h": "90 days",
    "1d":  "180 days","3d":  "180 days","1w":  "365 days",
    "1M":  "365 days",
}


def _timescaledb_available(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM pg_extension WHERE extname = 'timescaledb'"
        )
        return cur.fetchone()[0] > 0


def _is_hypertable(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM timescaledb_information.hypertables "
            "WHERE hypertable_name = %s",
            (table_name,),
        )
        return cur.fetchone()[0] > 0


def _convert_to_hypertables(conn) -> None:
    """
    Converts all OHLCV and indicator tables to TimescaleDB hypertables.
    Safe to call repeatedly — skips tables already converted.
    Also enables native compression with a 7-day compress_after policy.
    """
    tables = (
        [(f"ohlcv_{tf}", tf) for tf in OHLCV_TIMEFRAMES]
        + [(f"indicators_{tf}", tf) for tf in INDICATOR_TIMEFRAMES]
    )
    converted = 0
    for table_name, tf in tables:
        if _is_hypertable(conn, table_name):
            logger.debug(f"  Already hypertable: {table_name}")
            continue
        chunk_interval = _TIMESCALE_CHUNK_INTERVALS.get(tf, "30 days")
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT create_hypertable("
                f"  '{table_name}', 'open_time',"
                f"  chunk_time_interval => INTERVAL '{chunk_interval}',"
                f"  migrate_data => TRUE,"
                f"  if_not_exists => TRUE"
                f");"
            )
            cur.execute(
                f"ALTER TABLE {table_name} SET ("
                f"  timescaledb.compress,"
                f"  timescaledb.compress_segmentby = 'symbol',"
                f"  timescaledb.compress_orderby = 'open_time DESC'"
                f");"
            )
            cur.execute(
                f"SELECT add_compression_policy("
                f"  '{table_name}',"
                f"  INTERVAL '7 days',"
                f"  if_not_exists => TRUE"
                f");"
            )
        conn.commit()
        converted += 1
        logger.info(f"  Hypertable: {table_name}  (chunk={chunk_interval})")

    logger.info(
        f"TimescaleDB: {converted} tables converted, "
        f"{len(tables) - converted} already done."
    )


# ── Public API ────────────────────────────────────────────────────────────────

def create_all_tables() -> None:
    """
    Creates OHLCV tables, indicator tables, and minimal infrastructure tables.
    Safe to call repeatedly — all DDL uses IF NOT EXISTS.

    TimescaleDB: if the extension is present, OHLCV and indicator tables are
    automatically converted to hypertables with compression policies.
    If not installed, this step is silently skipped.
    """
    with db_connection() as conn:
        with conn.cursor() as cur:
            logger.info(f"Creating {len(OHLCV_TIMEFRAMES)} OHLCV tables...")
            for tf in OHLCV_TIMEFRAMES:
                cur.execute(_ohlcv_table_ddl(tf))

            logger.info(f"Creating {len(INDICATOR_TIMEFRAMES)} indicator tables...")
            for tf in INDICATOR_TIMEFRAMES:
                cur.execute(_indicator_table_ddl(tf))

            logger.info("Creating infrastructure tables (outbox, cooldowns)...")
            cur.execute(_infrastructure_tables_ddl())

        conn.commit()

        if _timescaledb_available(conn):
            logger.info("TimescaleDB detected — converting to hypertables...")
            _convert_to_hypertables(conn)
        else:
            logger.info(
                "TimescaleDB not installed — using standard PostgreSQL tables. "
                "See README Prerequisites for optional installation instructions."
            )

    logger.info(
        f"Schema ready — "
        f"{len(OHLCV_TIMEFRAMES)} OHLCV + "
        f"{len(INDICATOR_TIMEFRAMES)} indicator + "
        f"2 infrastructure tables."
    )


def verify_schema() -> dict:
    """
    Verifies all expected tables exist.
    Returns dict with 'ok' and 'missing' lists.
    """
    expected = (
        [f"ohlcv_{tf}"       for tf in OHLCV_TIMEFRAMES]
        + [f"indicators_{tf}" for tf in INDICATOR_TIMEFRAMES]
        + ["telegram_outbox", "trade_cooldowns"]
    )
    missing, ok = [], []
    with db_connection() as conn:
        with conn.cursor() as cur:
            for tname in expected:
                cur.execute("SELECT to_regclass(%s)", (tname,))
                (missing if cur.fetchone()[0] is None else ok).append(tname)
    return {"ok": ok, "missing": missing}
