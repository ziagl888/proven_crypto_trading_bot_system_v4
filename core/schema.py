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

    # ── Derived features ─────────────────────────────────────────────
    # Pre-computed by indicator engine so bots need zero extra math.
    # Used by: 14_ai_atb_bot, 12_ai_ats_bot, 13_ai_rub_bot, 11_ai_mis_bot
    ("atr_pct",               "REAL"),  # atr_14 / close * 100
    ("bb_position_relative",  "REAL"),  # (close-boll_lower_20) / (boll_upper_20-boll_lower_20)
    ("dc_position_relative",  "REAL"),  # (close-donchian_lower_20) / (donchian_upper_20-donchian_lower_20)
    ("dist_close_ema9_pct",   "REAL"),  # (close - ema_9) / ema_9
    ("dist_ema9_ema21_pct",   "REAL"),  # (ema_9 - ema_21) / ema_21
    ("dist_close_kama9_pct",  "REAL"),  # (close - kama_9) / kama_9
    ("dist_close_ema200_pct", "REAL"),  # (close - ema_200) / ema_200
    ("macd_hist_fast",        "REAL"),  # macd_dif_fast_9_21_9 - macd_dea_fast_9_21_9
    ("macd_hist_normal",      "REAL"),  # macd_dif_normal_12_26_9 - macd_dea_normal_12_26_9

    # ── Swing Highs / Lows ────────────────────────────────────────────
    # Last 5 confirmed swing highs and lows (price + age in candles).
    # Used by: fibonacci, 7_pattern_detector, 21_btc_smc, 22_ip_pattern,
    #          24_quasimodo, 25_smc_sniper, 29_ufi1, trade monitor
    # order parameter (candles left+right) is timeframe-dependent — see engine.
    ("swing_high_1",     "REAL"),    # most recent swing high price
    ("swing_high_1_age", "INTEGER"), # candles since swing_high_1
    ("swing_high_2",     "REAL"),
    ("swing_high_2_age", "INTEGER"),
    ("swing_high_3",     "REAL"),
    ("swing_high_3_age", "INTEGER"),
    ("swing_high_4",     "REAL"),
    ("swing_high_4_age", "INTEGER"),
    ("swing_high_5",     "REAL"),
    ("swing_high_5_age", "INTEGER"),
    ("swing_low_1",      "REAL"),    # most recent swing low price
    ("swing_low_1_age",  "INTEGER"), # candles since swing_low_1
    ("swing_low_2",      "REAL"),
    ("swing_low_2_age",  "INTEGER"),
    ("swing_low_3",      "REAL"),
    ("swing_low_3_age",  "INTEGER"),
    ("swing_low_4",      "REAL"),
    ("swing_low_4_age",  "INTEGER"),
    ("swing_low_5",      "REAL"),
    ("swing_low_5_age",  "INTEGER"),
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

        -- Candle close events: written by ingestion, polled by indicator engine.
        -- Lightweight IPC mechanism — avoids any file-based or socket-based signalling.
        -- Ingestion upserts a row on every candle close.
        -- Indicator engine polls this table every 10s to detect new closes.
        CREATE TABLE IF NOT EXISTS candle_close_events (
            timeframe   TEXT        NOT NULL PRIMARY KEY,
            closed_at   TIMESTAMPTZ NOT NULL,
            symbol_count INTEGER    NOT NULL DEFAULT 0
        );

        -- System state: key-value store for inter-process coordination.
        -- Used to signal readiness between processes (e.g. backfill_done).
        -- Keys:
        --   initial_backfill_done  'true' once ingestion completes first backfill
        CREATE TABLE IF NOT EXISTS system_state (
            key        TEXT        NOT NULL PRIMARY KEY,
            value      TEXT        NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """


def _trade_tables_ddl() -> str:
    """
    Trade tracking tables for V4.

    Design decisions:
      - Single `trades` table for all bots (classic + AI/ML)
      - 6 targets (tp1-tp6), nullable — tp1 required, rest optional
      - Trailing SL: starts at TP1 after TP2 hit, then follows each TP
      - Partial close: each TP closes 1/N of position (Cornix handles automatically)
      - Fast bot: only tp1, full close, no trailing
      - ml_confidence: real model score for AI bots, rolling WR for classic bots
      - pnl_r: (close_price - entry) / (entry - sl) for LONG,
               (entry - close_price) / (sl - entry) for SHORT
      - Reconstruction: uses 5m candles, max 30 days back
    """
    return """
        -- ── Main trades table ────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS trades (
            id                  SERIAL PRIMARY KEY,

            -- Origin
            bot_name            TEXT        NOT NULL,
            bot_version         TEXT        NOT NULL DEFAULT 'v4',
            timeframe           TEXT,

            -- Signal
            symbol              TEXT        NOT NULL,
            direction           TEXT        NOT NULL CHECK (direction IN ('LONG','SHORT')),
            entry               REAL        NOT NULL,
            tp1                 REAL        NOT NULL,
            tp2                 REAL,
            tp3                 REAL,
            tp4                 REAL,
            tp5                 REAL,
            tp6                 REAL,
            sl                  REAL        NOT NULL,
            sl_initial          REAL        NOT NULL,   -- original SL, never changes
            leverage            INTEGER     NOT NULL DEFAULT 20,
            tp_count            INTEGER     NOT NULL DEFAULT 1, -- total TPs set at signal time

            -- Partial close tracking
            -- tp_hit: highest TP reached so far (0=none, 1=tp1, 2=tp2, ...)
            tp_hit              INTEGER     NOT NULL DEFAULT 0,
            -- position_pct: remaining open position as fraction (1.0=100%, 0.5=50%)
            position_pct        REAL        NOT NULL DEFAULT 1.0,

            -- ML / Confidence
            ml_model            TEXT,           -- NULL for classic bots
            ml_confidence       REAL,           -- model score OR rolling win rate

            -- Status & Result
            status              TEXT        NOT NULL DEFAULT 'OPEN'
                                CHECK (status IN (
                                    'OPEN',
                                    'CLOSED_TP',
                                    'CLOSED_SL',
                                    'CLOSED_MANUAL',
                                    'CLOSED_EXPIRED'
                                )),
            outcome             TEXT        CHECK (outcome IN ('WIN','LOSS','BREAKEVEN')),
            close_price         REAL,           -- price at close
            pnl_r               REAL,           -- result in R-multiples
            weighted_pnl_r      REAL,           -- position-weighted R (accounts for partial closes)

            -- Timestamps
            opened_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            closed_at           TIMESTAMPTZ,
            last_updated        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            -- Telegram (for future close notifications)
            telegram_message_id BIGINT
        );

        CREATE INDEX IF NOT EXISTS idx_trades_status
            ON trades (status) WHERE status = 'OPEN';
        CREATE INDEX IF NOT EXISTS idx_trades_symbol
            ON trades (symbol, status);
        CREATE INDEX IF NOT EXISTS idx_trades_bot
            ON trades (bot_name, opened_at DESC);
        CREATE INDEX IF NOT EXISTS idx_trades_opened
            ON trades (opened_at DESC);

        -- ── Signal log (all signals incl. filtered ones) ──────────────────────
        -- Records every signal a bot generates, whether posted or not.
        -- Used for: filter calibration, shadow analysis, ML feedback.
        CREATE TABLE IF NOT EXISTS signal_log (
            id                  SERIAL PRIMARY KEY,

            -- Origin
            bot_name            TEXT        NOT NULL,
            symbol              TEXT        NOT NULL,
            direction           TEXT        NOT NULL,
            timeframe           TEXT,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            -- Signal data (as generated by bot, before any adjustment)
            entry               REAL,
            sl                  REAL,
            tp1                 REAL,
            tp2                 REAL,
            tp3                 REAL,
            tp4                 REAL,
            tp5                 REAL,
            tp6                 REAL,
            ml_model            TEXT,
            ml_confidence       REAL,

            -- Filter decision
            was_posted          BOOLEAN     NOT NULL DEFAULT TRUE,
            filter_reason       TEXT,       -- NULL if posted, else: COOLDOWN,
                                            -- CONFIDENCE_TOO_LOW, TRADE_ALREADY_OPEN,
                                            -- REGIME_FILTER, DUPLICATE_DIRECTION
            trade_id            INTEGER REFERENCES trades(id) ON DELETE SET NULL,

            -- Shadow outcome (calculated by housekeeping using 5m candles)
            -- Answers: "what would have happened if this signal had been posted?"
            shadow_outcome      TEXT        CHECK (shadow_outcome IN ('WIN','LOSS','BREAKEVEN')),
            shadow_close_price  REAL,
            shadow_pnl_r        REAL,
            shadow_calculated   BOOLEAN     NOT NULL DEFAULT FALSE
        );

        CREATE INDEX IF NOT EXISTS idx_signal_log_bot
            ON signal_log (bot_name, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_signal_log_posted
            ON signal_log (was_posted, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_signal_log_shadow
            ON signal_log (shadow_calculated) WHERE shadow_calculated = FALSE;

        -- ── Bot performance cache ─────────────────────────────────────────────
        -- Updated daily by housekeeping.
        -- win_rate used as ml_confidence for classic bots on new signals.
        CREATE TABLE IF NOT EXISTS bot_performance (
            bot_name            TEXT        PRIMARY KEY,
            total_trades        INTEGER     NOT NULL DEFAULT 0,
            winning_trades      INTEGER     NOT NULL DEFAULT 0,
            losing_trades       INTEGER     NOT NULL DEFAULT 0,
            win_rate            REAL,           -- winning_trades / total_trades
            avg_pnl_r           REAL,           -- average R per trade
            avg_weighted_pnl_r  REAL,           -- position-weighted average R
            trades_in_window    INTEGER,        -- trades used for calculation
            window_days         INTEGER,        -- lookback in days (90 or all)
            last_calculated     TIMESTAMPTZ
        );

        -- ── V3 migration staging tables ───────────────────────────────────────
        -- Temporary views that map V3 table structure to V4.
        -- Used by the migration script — dropped after migration is complete.
        -- Named with v3_ prefix so they're clearly identifiable.
        CREATE TABLE IF NOT EXISTS v3_migration_log (
            id                  SERIAL PRIMARY KEY,
            source_table        TEXT        NOT NULL,
            source_id           INTEGER     NOT NULL,
            target_id           INTEGER     REFERENCES trades(id),
            migrated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            notes               TEXT,
            UNIQUE (source_table, source_id)
        );

        -- Pump/Dump detector events (written by 30_pump_dump_detector.py)
        CREATE TABLE IF NOT EXISTS pump_dump_events (
            id               SERIAL PRIMARY KEY,
            symbol           TEXT        NOT NULL,
            detected_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            event_type       TEXT        NOT NULL,
            price            NUMERIC,
            price_change_pct NUMERIC,
            volume_ratio     NUMERIC,
            module           TEXT        NOT NULL DEFAULT 'PD-1'
        );
        CREATE INDEX IF NOT EXISTS idx_pde_symbol_time
            ON pump_dump_events (symbol, detected_at DESC);

        -- ── Pattern Detector events (written by 40_pattern_detector.py) ──────
        CREATE TABLE IF NOT EXISTS pattern_events (
            id              SERIAL PRIMARY KEY,
            symbol          TEXT        NOT NULL,
            timeframe       TEXT        NOT NULL,
            pattern         TEXT        NOT NULL,
            direction       TEXT,
            state           TEXT        NOT NULL
                            CHECK (state IN (
                                'BREAKOUT','WAITING_RETEST','RETEST',
                                'CONFIRMED','FAKEOUT','EXPIRED'
                            )),
            break_price     REAL,
            break_time      TIMESTAMPTZ,
            slope_high      REAL,
            intercept_high  REAL,
            slope_low       REAL,
            intercept_low   REAL,
            retest_price    REAL,
            retest_time     TIMESTAMPTZ,
            trade_triggered BOOLEAN     DEFAULT FALSE,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_pattern_events_active
            ON pattern_events (state, symbol)
            WHERE state IN ('BREAKOUT','WAITING_RETEST','RETEST');
        CREATE INDEX IF NOT EXISTS idx_pattern_events_symbol
            ON pattern_events (symbol, created_at DESC);

        -- ── Trendline Detector events (written by 41_trendbreaker_detector.py) ─
        CREATE TABLE IF NOT EXISTS trendline_events (
            id              SERIAL PRIMARY KEY,
            symbol          TEXT        NOT NULL,
            trend_direction TEXT        NOT NULL,
            event_type      TEXT        NOT NULL
                            CHECK (event_type IN (
                                'BREAK_UP','BREAK_DOWN',
                                'BOUNCE_UP','BOUNCE_DOWN'
                            )),
            state           TEXT        NOT NULL
                            CHECK (state IN (
                                'DETECTED','WAITING_RETEST',
                                'CONFIRMED','EXPIRED'
                            )),
            slope           REAL,
            intercept       REAL,
            trend_value     REAL,
            break_price     REAL,
            break_time      TIMESTAMPTZ,
            distance_pct    REAL,
            volume_ratio    REAL,
            retest_price    REAL,
            retest_time     TIMESTAMPTZ,
            ml_score        REAL,
            trade_triggered BOOLEAN     DEFAULT FALSE,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_trendline_events_active
            ON trendline_events (state, symbol)
            WHERE state IN ('DETECTED','WAITING_RETEST');
        CREATE INDEX IF NOT EXISTS idx_trendline_events_symbol
            ON trendline_events (symbol, created_at DESC);

        -- ── Funding rates (written by 32_funding_monitor.py) ────────────────
        -- Stores every 5-min funding rate snapshot for all coins.
        -- 573 coins × 288 polls/day = ~165k rows/day, ~500k rows per 3 days.
        -- TimescaleDB hypertable with 1-day chunks + compression recommended.
        CREATE TABLE IF NOT EXISTS funding_rates (
            symbol      TEXT        NOT NULL,
            ts          TIMESTAMPTZ NOT NULL,
            rate        REAL        NOT NULL,   -- raw rate, e.g. 0.0001 = 0.01%
            PRIMARY KEY (symbol, ts)
        );
        CREATE INDEX IF NOT EXISTS idx_funding_symbol_ts
            ON funding_rates (symbol, ts DESC);

        -- ── Whale trades (written by 33_whale_monitor.py) ───────────────────
        -- Stores individual aggTrade records above MIN_USD threshold ($25k).
        -- Estimated volume: depends on market activity.
        -- Retention: 3 days (housekeeping purges older rows nightly).
        CREATE TABLE IF NOT EXISTS whale_trades (
            id          BIGSERIAL   PRIMARY KEY,
            symbol      TEXT        NOT NULL,
            ts          TIMESTAMPTZ NOT NULL,
            direction   TEXT        NOT NULL CHECK (direction IN ('LONG','SHORT')),
            usd_value   REAL        NOT NULL,   -- notional USD value
            price       REAL        NOT NULL,
            qty         REAL        NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_whale_symbol_ts
            ON whale_trades (symbol, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_whale_ts
            ON whale_trades (ts DESC);
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

            logger.info("Creating trade tables (trades, signal_log, bot_performance)...")
            cur.execute(_trade_tables_ddl())

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
        f"infrastructure + trade tables."
    )


def migrate_schema() -> None:
    """
    Adds any columns that exist in INDICATOR_COLUMNS but are missing
    from the actual DB tables. Safe to run repeatedly — uses
    IF NOT EXISTS for each ALTER TABLE.

    Also ensures detector tables (pattern_events, trendline_events) and
    trade tables exist — safe to call on running systems.

    Call this after create_all_tables() to handle schema upgrades
    without dropping and recreating tables (which would lose all data).
    """
    # Ensure all non-OHLCV tables exist (idempotent — uses IF NOT EXISTS)
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_infrastructure_tables_ddl())
            cur.execute(_trade_tables_ddl())
        conn.commit()
        logger.info("Infrastructure + trade + detector tables ensured.")

    with db_connection() as conn:
        with conn.cursor() as cur:
            for tf in INDICATOR_TIMEFRAMES:
                tname = f"indicators_{tf}"

                # Get existing columns from DB
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                      AND table_schema = 'public'
                    """,
                    (tname,),
                )
                existing = {row[0] for row in cur.fetchall()}

                # Add any missing columns
                added = []
                for col, dtype in INDICATOR_COLUMNS:
                    if col not in existing:
                        cur.execute(
                            f"ALTER TABLE {tname} "
                            f"ADD COLUMN IF NOT EXISTS {col} {dtype}"
                        )
                        added.append(col)

                if added:
                    logger.info(
                        f"Migration: added {len(added)} columns to {tname}: "
                        f"{added}"
                    )
                else:
                    logger.debug(f"Migration: {tname} is up to date.")

        conn.commit()

    logger.info("Schema migration complete.")


def verify_schema() -> dict:
    """
    Verifies all expected tables exist.
    Returns dict with 'ok' and 'missing' lists.
    """
    expected = (
        [f"ohlcv_{tf}"       for tf in OHLCV_TIMEFRAMES]
        + [f"indicators_{tf}" for tf in INDICATOR_TIMEFRAMES]
        + [
            "telegram_outbox", "trade_cooldowns", "candle_close_events",
            "system_state",
            "trades", "signal_log", "bot_performance", "v3_migration_log",
            "pump_dump_events",
            "pattern_events", "trendline_events",
            "funding_rates", "whale_trades",
        ]
    )
    missing, ok = [], []
    with db_connection() as conn:
        with conn.cursor() as cur:
            for tname in expected:
                cur.execute("SELECT to_regclass(%s)", (tname,))
                (missing if cur.fetchone()[0] is None else ok).append(tname)
    return {"ok": ok, "missing": missing}








