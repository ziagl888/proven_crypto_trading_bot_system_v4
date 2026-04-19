# core/schema.py
# Database schema initialisation for V4.
#
# Design principles vs V3:
#   - ONE partitioned table per timeframe for OHLCV (not one table per coin)
#   - ONE partitioned table per timeframe for indicators
#   - BRIN indexes on open_time (far cheaper than B-tree for time-series)
#   - B-tree index on (symbol, open_time) for point lookups
#   - Indicator columns: ONLY what is actually consumed by bots (audited)
#
# Supported timeframes (data stored but indicator calc scope is configured
# separately in config.py):
#   10s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M

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
# Subset of OHLCV_TIMEFRAMES — only what bots actually read.
INDICATOR_TIMEFRAMES = ["30m", "1h", "2h", "4h", "1d", "1w"]

# ── Audited indicator columns ─────────────────────────────────────────────────
# Every column here is consumed by at least one bot (verified audit of V3 code).
# Columns NOT used by any bot have been deliberately omitted.
#
# Usage map (short form):
#   rsi_6/9/12/14/24          → strat_fast_in_out, strat_5%, strat_sr, strat_main,
#                                9_ai_sr, 10_pump_dump, 11_mis, 13_rub, 24_qm, 25_sniper
#   ema_7..200                → 11_mis, 12_ats, 14_atb, 15_master, 24_qm, 25_sniper,
#                                strat_5%, strat_fast_in_out
#   ma_7..200                 → 15_master (full normalisation)
#   wma_7..200                → 15_master, strat_5%, strat_fast_in_out
#   smma_*                    → 15_master (full normalisation)
#   kama_7..99                → 12_ats, 14_atb, 15_master, strat_5%, strat_fast_in_out
#   atr_9/14/21               → strat_fast_in_out, strat_main, 12_ats, 15_master
#   tsi_25_13_13(_signal)     → 24_qm, 25_sniper
#   tsi_fast_12_7_7(_signal)  → strat_fast_in_out, strat_5%, 12_ats, 13_rub, 18_abr1
#   macd_dif/dea_fast_9_21_9  → strat_fast_in_out, strat_5%
#   macd_dif/dea_normal_12_26_9 → 10_pump_dump, 12_ats, 13_rub, 15_master, 24_qm, 25_sniper
#   boll_upper/mid/lower_20   → 11_mis, 12_ats, 13_rub, 15_master, 18_abr1, 24_qm
#   donchian_upper/mid/lower_4  → strat_fast_in_out, strat_5%
#   donchian_upper/mid/lower_10 → 15_master
#   donchian_upper/mid/lower_12 → 15_master
#   donchian_upper/mid/lower_15 → 15_master
#   donchian_upper/mid/lower_20 → 12_ats, 13_rub, 15_master, 18_abr1, 24_qm, 25_sniper
#   trendline_slope/intercept/price → 12_ats, 15_master
#   channel_upper/lower_price → 15_master
#   mid_line                  → 15_master
#   r_squared                 → 15_master
#   trend_direction           → 12_ats, 15_master, 24_qm, 25_sniper
#   support_price             → strat_fast_in_out, strat_5%, strat_sr, strat_main,
#                                9_ai_sr, 12_ats, 15_master
#   resistance_price          → (same as support_price)
#   hvn_1/2/3, poc            → 15_master
#   fib_support/resistance_*  → 15_master
#   fib_extension_*           → 15_master

INDICATOR_COLUMNS: list[tuple[str, str]] = [
    # ── RSI ──────────────────────────────────────────────────────────
    ("rsi_6",  "REAL"), ("rsi_9",  "REAL"), ("rsi_12", "REAL"),
    ("rsi_14", "REAL"), ("rsi_24", "REAL"),

    # ── EMA ──────────────────────────────────────────────────────────
    ("ema_7",  "REAL"), ("ema_9",  "REAL"), ("ema_12", "REAL"),
    ("ema_21", "REAL"), ("ema_26", "REAL"), ("ema_34", "REAL"),
    ("ema_50", "REAL"), ("ema_55", "REAL"), ("ema_89", "REAL"),
    ("ema_99", "REAL"), ("ema_200","REAL"),

    # ── MA (SMA) ──────────────────────────────────────────────────────
    ("ma_7",   "REAL"), ("ma_10",  "REAL"), ("ma_20",  "REAL"),
    ("ma_25",  "REAL"), ("ma_50",  "REAL"), ("ma_99",  "REAL"),
    ("ma_100", "REAL"), ("ma_200", "REAL"),

    # ── WMA ───────────────────────────────────────────────────────────
    ("wma_7",  "REAL"), ("wma_9",  "REAL"), ("wma_12", "REAL"),
    ("wma_21", "REAL"), ("wma_26", "REAL"), ("wma_34", "REAL"),
    ("wma_50", "REAL"), ("wma_55", "REAL"), ("wma_89", "REAL"),
    ("wma_99", "REAL"), ("wma_200","REAL"),

    # ── SMMA ──────────────────────────────────────────────────────────
    ("smma_10", "REAL"), ("smma_20",  "REAL"), ("smma_25",  "REAL"),
    ("smma_50", "REAL"), ("smma_99",  "REAL"), ("smma_100", "REAL"),
    ("smma_200","REAL"),

    # ── KAMA ──────────────────────────────────────────────────────────
    ("kama_7",  "REAL"), ("kama_9",  "REAL"), ("kama_12", "REAL"),
    ("kama_21", "REAL"), ("kama_26", "REAL"), ("kama_34", "REAL"),
    ("kama_50", "REAL"), ("kama_55", "REAL"), ("kama_89", "REAL"),
    ("kama_99", "REAL"),

    # ── ATR ───────────────────────────────────────────────────────────
    ("atr_9",  "REAL"), ("atr_14", "REAL"), ("atr_21", "REAL"),

    # ── TSI ───────────────────────────────────────────────────────────
    ("tsi_25_13_13",        "REAL"), ("tsi_25_13_13_signal",      "REAL"),
    ("tsi_fast_12_7_7",     "REAL"), ("tsi_fast_12_7_7_signal",   "REAL"),

    # ── MACD ──────────────────────────────────────────────────────────
    ("macd_dif_fast_9_21_9",    "REAL"), ("macd_dea_fast_9_21_9",    "REAL"),
    ("macd_dif_normal_12_26_9", "REAL"), ("macd_dea_normal_12_26_9", "REAL"),

    # ── Bollinger Bands (period 20) ────────────────────────────────────
    ("boll_upper_20", "REAL"), ("boll_mid_20", "REAL"), ("boll_lower_20", "REAL"),

    # ── Donchian Channels ─────────────────────────────────────────────
    ("donchian_upper_4",  "REAL"), ("donchian_lower_4",  "REAL"), ("donchian_mid_4",  "REAL"),
    ("donchian_upper_10", "REAL"), ("donchian_lower_10", "REAL"), ("donchian_mid_10", "REAL"),
    ("donchian_upper_12", "REAL"), ("donchian_lower_12", "REAL"), ("donchian_mid_12", "REAL"),
    ("donchian_upper_15", "REAL"), ("donchian_lower_15", "REAL"), ("donchian_mid_15", "REAL"),
    ("donchian_upper_20", "REAL"), ("donchian_lower_20", "REAL"), ("donchian_mid_20", "REAL"),

    # ── Trendline & Channel ───────────────────────────────────────────
    ("trendline_slope",     "REAL"), ("trendline_intercept", "REAL"),
    ("trendline_price",     "REAL"), ("channel_upper_price", "REAL"),
    ("channel_lower_price", "REAL"), ("mid_line",            "REAL"),
    ("r_squared",           "REAL"), ("trend_direction",     "TEXT"),

    # ── Support / Resistance ──────────────────────────────────────────
    ("support_price",    "REAL"), ("resistance_price",  "REAL"),

    # ── HVN / POC (Volume Profile) ────────────────────────────────────
    ("hvn_1", "REAL"), ("hvn_2", "REAL"), ("hvn_3", "REAL"), ("poc", "REAL"),

    # ── Fibonacci retracement levels ─────────────────────────────────
    ("fib_support_0_236",    "REAL"), ("fib_resistance_0_236",    "REAL"),
    ("fib_support_0_382",    "REAL"), ("fib_resistance_0_382",    "REAL"),
    ("fib_support_0_5",      "REAL"), ("fib_resistance_0_5",      "REAL"),
    ("fib_support_0_618",    "REAL"), ("fib_resistance_0_618",    "REAL"),
    ("fib_support_0_786",    "REAL"), ("fib_resistance_0_786",    "REAL"),

    # ── Fibonacci extension levels ────────────────────────────────────
    ("fib_extension_1_272",  "REAL"),
    ("fib_extension_1_618",  "REAL"),
    ("fib_extension_2_618",  "REAL"),
]

# ── DDL helpers ───────────────────────────────────────────────────────────────

def _ohlcv_table_ddl(tf: str) -> str:
    """Returns CREATE TABLE + index DDL for the given timeframe."""
    tname = f"ohlcv_{tf}"
    return f"""
        CREATE TABLE IF NOT EXISTS {tname} (
            symbol      TEXT                     NOT NULL,
            open_time   TIMESTAMPTZ              NOT NULL,
            open        DOUBLE PRECISION         NOT NULL,
            high        DOUBLE PRECISION         NOT NULL,
            low         DOUBLE PRECISION         NOT NULL,
            close       DOUBLE PRECISION         NOT NULL,
            volume      DOUBLE PRECISION         NOT NULL,
            PRIMARY KEY (symbol, open_time)
        );
        CREATE INDEX IF NOT EXISTS idx_{tname}_time_brin
            ON {tname} USING BRIN (open_time);
        CREATE INDEX IF NOT EXISTS idx_{tname}_sym_time
            ON {tname} (symbol, open_time DESC);
    """


def _indicator_table_ddl(tf: str) -> str:
    """Returns CREATE TABLE + index DDL for the indicator table."""
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


def _operational_tables_ddl() -> str:
    """DDL for all operational (non-OHLCV) tables."""
    return """
        -- Active trades managed by the classic trade monitor
        CREATE TABLE IF NOT EXISTS active_trades_master (
            id          SERIAL PRIMARY KEY,
            strategy    TEXT,
            time        TIMESTAMPTZ DEFAULT NOW(),
            coin        TEXT,
            direction   TEXT,
            lev         TEXT,
            target1     REAL,
            target2     REAL DEFAULT 0,
            target3     REAL DEFAULT 0,
            target4     REAL DEFAULT 0,
            sl          REAL,
            entry       REAL,
            posted      TIMESTAMPTZ DEFAULT NOW(),
            status      TEXT DEFAULT 'WORKING'
        );
        CREATE INDEX IF NOT EXISTS idx_atm_status   ON active_trades_master (status);
        CREATE INDEX IF NOT EXISTS idx_atm_coin     ON active_trades_master (coin);

        -- Closed trades history
        CREATE TABLE IF NOT EXISTS closed_trades_master (
            id          SERIAL PRIMARY KEY,
            strategy    TEXT,
            coin        TEXT,
            direction   TEXT,
            entry       REAL,
            close_price REAL,
            status      TEXT,
            opened_at   TIMESTAMPTZ,
            closed_at   TIMESTAMPTZ DEFAULT NOW()
        );

        -- AI/ML signals (bots 9–18, 24, 25, 28)
        CREATE TABLE IF NOT EXISTS ai_signals (
            id              SERIAL PRIMARY KEY,
            symbol          TEXT,
            model           TEXT,
            direction       TEXT,
            entry1          REAL,
            price           REAL,
            sl              REAL,
            targets         JSONB,
            confidence      REAL,
            current_target_hit INTEGER DEFAULT 0,
            open_time       TIMESTAMPTZ DEFAULT NOW(),
            status          TEXT DEFAULT 'OPEN'
        );
        CREATE INDEX IF NOT EXISTS idx_ai_signals_sym   ON ai_signals (symbol);
        CREATE INDEX IF NOT EXISTS idx_ai_signals_status ON ai_signals (status);

        -- Closed AI signals
        CREATE TABLE IF NOT EXISTS closed_ai_signals (
            id          SERIAL PRIMARY KEY,
            symbol      TEXT,
            model       TEXT,
            direction   TEXT,
            entry       REAL,
            close_price REAL,
            status      TEXT,
            opened_at   TIMESTAMPTZ,
            closed_at   TIMESTAMPTZ DEFAULT NOW()
        );

        -- Telegram outbox (consumed by 4_telegram_bot)
        CREATE TABLE IF NOT EXISTS telegram_outbox (
            id          SERIAL PRIMARY KEY,
            channel_id  BIGINT,
            message     TEXT,
            image_path  TEXT,
            sent        BOOLEAN DEFAULT FALSE,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_tg_outbox_sent ON telegram_outbox (sent, id);

        -- Per-bot cooldowns
        CREATE TABLE IF NOT EXISTS trade_cooldowns (
            module          TEXT,
            coin            TEXT,
            direction       TEXT,
            last_posted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (module, coin, direction)
        );

        -- ML prediction log
        CREATE TABLE IF NOT EXISTS ml_predictions_master (
            id          SERIAL PRIMARY KEY,
            trade_id    INTEGER,
            model_name  TEXT,
            direction   TEXT,
            confidence  REAL,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_ml_pred_trade ON ml_predictions_master (trade_id);

        -- Master AI processed signals (dedup for 15_ai_master_bot)
        CREATE TABLE IF NOT EXISTS master_ai_processed_signals (
            signal_type TEXT,
            signal_id   INTEGER,
            processed_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (signal_type, signal_id)
        );

        -- Regime Orchestrator tables
        CREATE TABLE IF NOT EXISTS regime_history (
            id              SERIAL PRIMARY KEY,
            regime          TEXT,
            alt_context     TEXT,
            btc_atr_1h_pct  REAL,
            btc_atr_4h_pct  REAL,
            recorded_at     TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_regime_hist_at ON regime_history (recorded_at DESC);

        CREATE TABLE IF NOT EXISTS regime_current (
            id              INTEGER PRIMARY KEY DEFAULT 1,
            regime          TEXT,
            alt_context     TEXT,
            since           TIMESTAMPTZ,
            alt_context_since TIMESTAMPTZ,
            confidence      REAL,
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        );
        INSERT INTO regime_current (id) VALUES (1) ON CONFLICT DO NOTHING;

        CREATE TABLE IF NOT EXISTS bot_regime_performance (
            bot_name    TEXT,
            regime      TEXT,
            alt_context TEXT,
            direction   TEXT,
            n_trades    INTEGER DEFAULT 0,
            win_rate    REAL DEFAULT 0,
            updated_at  TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (bot_name, regime, alt_context, direction)
        );

        CREATE TABLE IF NOT EXISTS bot_regime_whitelist (
            bot_name    TEXT,
            direction   TEXT,
            regime      TEXT,
            alt_context TEXT,
            whitelisted BOOLEAN DEFAULT FALSE,
            reason      TEXT,
            updated_at  TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (bot_name, direction, regime, alt_context)
        );

        CREATE TABLE IF NOT EXISTS orchestrator_open_trades (
            id          SERIAL PRIMARY KEY,
            coin        TEXT,
            direction   TEXT,
            bot_name    TEXT,
            opened_at   TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS orchestrator_suppressed_signals (
            id          SERIAL PRIMARY KEY,
            coin        TEXT,
            direction   TEXT,
            bot_name    TEXT,
            reason      TEXT,
            suppressed_at TIMESTAMPTZ DEFAULT NOW()
        );
    """


# ── TimescaleDB support ───────────────────────────────────────────────────────

# Chunk interval per timeframe — shorter TFs produce more rows, use smaller chunks
# so that recent-data queries hit as few chunks as possible.
_TIMESCALE_CHUNK_INTERVALS: dict[str, str] = {
    "10s": "1 day",
    "1m":  "3 days",
    "3m":  "7 days",
    "5m":  "7 days",
    "15m": "14 days",
    "30m": "14 days",
    "1h":  "30 days",
    "2h":  "30 days",
    "4h":  "60 days",
    "6h":  "60 days",
    "8h":  "60 days",
    "12h": "90 days",
    "1d":  "180 days",
    "3d":  "180 days",
    "1w":  "365 days",
    "1M":  "365 days",
}


def _timescaledb_available(conn) -> bool:
    """Returns True if the TimescaleDB extension is installed in this database."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM pg_extension WHERE extname = 'timescaledb'"
        )
        return cur.fetchone()[0] > 0


def _is_hypertable(conn, table_name: str) -> bool:
    """Returns True if the table has already been converted to a hypertable."""
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

    - Safe to call repeatedly: skips tables that are already hypertables.
    - Each timeframe gets a tuned chunk_time_interval (see _TIMESCALE_CHUNK_INTERVALS).
    - Also enables native compression with a 7-day compress_after policy.
    - Called automatically by create_all_tables() when TimescaleDB is detected.
    """
    tables_to_convert = (
        [(f"ohlcv_{tf}", tf) for tf in OHLCV_TIMEFRAMES]
        + [(f"indicators_{tf}", tf) for tf in INDICATOR_TIMEFRAMES]
    )

    converted = 0
    for table_name, tf in tables_to_convert:
        if _is_hypertable(conn, table_name):
            logger.debug(f"  Already hypertable: {table_name}")
            continue

        chunk_interval = _TIMESCALE_CHUNK_INTERVALS.get(tf, "30 days")
        with conn.cursor() as cur:
            # create_hypertable migrates existing rows and sets up chunking.
            # migrate_data=TRUE handles tables that already contain rows.
            cur.execute(
                f"SELECT create_hypertable("
                f"  '{table_name}', 'open_time',"
                f"  chunk_time_interval => INTERVAL '{chunk_interval}',"
                f"  migrate_data => TRUE,"
                f"  if_not_exists => TRUE"
                f");"
            )
            # Enable compression: segment by symbol so each chunk is sorted
            # by symbol + time, maximising compression ratios.
            cur.execute(
                f"ALTER TABLE {table_name} SET ("
                f"  timescaledb.compress,"
                f"  timescaledb.compress_segmentby = 'symbol',"
                f"  timescaledb.compress_orderby = 'open_time DESC'"
                f");"
            )
            # Compress chunks older than 7 days automatically.
            cur.execute(
                f"SELECT add_compression_policy("
                f"  '{table_name}',"
                f"  INTERVAL '7 days',"
                f"  if_not_exists => TRUE"
                f");"
            )
        conn.commit()
        converted += 1
        logger.info(f"  ✓ Hypertable: {table_name}  (chunk={chunk_interval})")

    logger.info(
        f"TimescaleDB: {converted} tables converted, "
        f"{len(tables_to_convert) - converted} already done."
    )


# ── Public API ────────────────────────────────────────────────────────────────

def create_all_tables() -> None:
    """
    Creates all OHLCV tables, indicator tables, and operational tables.
    Safe to call repeatedly — all DDL uses IF NOT EXISTS.

    TimescaleDB auto-detection:
      If the timescaledb extension is present in the database, all OHLCV and
      indicator tables are automatically converted to hypertables with tuned
      chunk intervals and native compression enabled.
      If TimescaleDB is not installed, the step is silently skipped and all
      tables remain standard PostgreSQL tables.
    """
    with db_connection() as conn:
        with conn.cursor() as cur:

            # OHLCV tables for all supported timeframes
            logger.info(f"Creating {len(OHLCV_TIMEFRAMES)} OHLCV tables...")
            for tf in OHLCV_TIMEFRAMES:
                cur.execute(_ohlcv_table_ddl(tf))
                logger.debug(f"  ✓ ohlcv_{tf}")

            # Indicator tables for active timeframes only
            logger.info(f"Creating {len(INDICATOR_TIMEFRAMES)} indicator tables...")
            for tf in INDICATOR_TIMEFRAMES:
                cur.execute(_indicator_table_ddl(tf))
                logger.debug(f"  ✓ indicators_{tf}")

            # Operational tables
            logger.info("Creating operational tables...")
            cur.execute(_operational_tables_ddl())

        conn.commit()

        # TimescaleDB hypertable conversion (automatic, optional)
        if _timescaledb_available(conn):
            logger.info("TimescaleDB detected — converting tables to hypertables...")
            _convert_to_hypertables(conn)
        else:
            logger.info(
                "TimescaleDB not installed — using standard PostgreSQL tables. "
                "See README Prerequisites for optional installation instructions."
            )

    logger.info(
        f"Schema initialisation complete — "
        f"{len(OHLCV_TIMEFRAMES)} OHLCV + {len(INDICATOR_TIMEFRAMES)} indicator "
        f"+ operational tables ready."
    )


def verify_schema() -> dict:
    """
    Verifies that all expected tables exist.
    Returns {'missing': [...], 'ok': [...]} dict.
    """
    expected = (
        [f"ohlcv_{tf}" for tf in OHLCV_TIMEFRAMES]
        + [f"indicators_{tf}" for tf in INDICATOR_TIMEFRAMES]
        + [
            "active_trades_master", "closed_trades_master",
            "ai_signals", "closed_ai_signals",
            "telegram_outbox", "trade_cooldowns",
            "ml_predictions_master", "master_ai_processed_signals",
            "regime_history", "regime_current",
            "bot_regime_performance", "bot_regime_whitelist",
            "orchestrator_open_trades", "orchestrator_suppressed_signals",
        ]
    )
    missing = []
    ok = []
    with db_connection() as conn:
        with conn.cursor() as cur:
            for tname in expected:
                cur.execute("SELECT to_regclass(%s)", (tname,))
                if cur.fetchone()[0] is None:
                    missing.append(tname)
                else:
                    ok.append(tname)
    return {"missing": missing, "ok": ok}

