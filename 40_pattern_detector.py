# 40_pattern_detector.py
# Geometric Chart Pattern Detector V4
#
# Detects Triangle and Channel patterns on 1h/2h/4h/1d candles.
# Writes events to pattern_events table — does NOT generate trades.
# Trading is handled separately by 50_pattern_trader.py.
#
# Patterns detected:
#   Symmetrical Triangle  — converging highs (down) + lows (up)
#   Ascending Triangle    — flat highs + rising lows
#   Descending Triangle   — falling highs + flat lows
#   Ascending Channel     — parallel rising highs + lows
#   Descending Channel    — parallel falling highs + lows
#
# Flow per pattern:
#   1. BREAKOUT detected → info alert → state = WAITING_RETEST
#   2. RETEST detected (price returns to breakout level) → info alert
#   3. CONFIRMED (strong close back on break side) → trader bot picks up
#   OR FAKEOUT (deep return into pattern) → expired
#   OR EXPIRED (too many candles without retest)
#
# Key improvements vs V3:
#   - scipy.signal.argrelextrema instead of rolling().max() for cleaner pivots
#   - flat_th raised 0.02 → 0.10 (5× stricter pattern detection)
#   - Retest body confirmation raised 0.4% → 0.5%
#   - State persisted in DB (pattern_events) not JSON file

from __future__ import annotations

import datetime
import json
import logging
import logging.handlers
import os
import sys
import time

import numpy as np
import pandas as pd
import scipy.signal
import scipy.stats as stats

from dotenv import load_dotenv
load_dotenv()

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR     = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - PAT_DETECT - %(levelname)s - %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            os.path.join(_LOG_DIR, "pattern_detector.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

from core.database import db_connection
from core.schema import verify_schema
from core.shutdown import ShutdownHandler
from core.charting import generate_chart

# ── Constants ─────────────────────────────────────────────────────────────────

# Timeframes to scan
PATTERN_TIMEFRAMES = ["1h", "2h", "4h", "1d"]

# Pivot detection — scipy argrelextrema order (candles left+right)
PIVOT_ORDER = {
    "1h": 5, "2h": 5, "4h": 5, "1d": 3,
}
MIN_PIVOTS = 3   # need at least 3 pivots to draw a trendline

# Pattern classification — slope % per candle vs average price
# V4: raised from 0.02 → 0.10 (much stricter — fewer false positives)
FLAT_THRESHOLD = 0.10    # below this slope % = "flat"

# Breakout confirmation
MIN_CANDLES = 50         # skip if less data

# Retest tracking
RETEST_TOUCH_PCT  = 0.008   # 0.8% — "touching" the breakout level
RETEST_FAIL_PCT   = 0.022   # 2.2% — deep failure = fakeout
RETEST_BODY_PCT   = 0.005   # 0.5% — min candle body for confirmation (V3 was 0.4%)

# Expiry: candles since breakout
EXPIRY_CANDLES = {
    "1h": 30, "2h": 20, "4h": 15, "1d": 8,
}

# Info channel (pattern alerts, no trades)
_INFO_CHANNELS: dict[str, int] = {}   # loaded from config at startup


# ── DB helpers ────────────────────────────────────────────────────────────────

def _load_ohlcv(conn, symbol: str, tf: str, limit: int = 200) -> pd.DataFrame:
    try:
        tbl = f"ohlcv_{tf}"
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT open_time, open, high, low, close, volume
                FROM {tbl}
                WHERE symbol = %s
                ORDER BY open_time DESC
                LIMIT %s
                """,
                (symbol, limit),
            )
            rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume"])
        for c in ("open","high","low","close","volume"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        return df.dropna().sort_values("open_time").reset_index(drop=True)
    except Exception as e:
        logger.debug(f"OHLCV load error {symbol}/{tf}: {e}")
        conn.rollback()
        return pd.DataFrame()


def _active_pattern_ids(conn) -> set[str]:
    """Returns set of pattern_ids currently active (not expired/confirmed)."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol || '_' || timeframe || '_' || pattern || '_' ||
                       TO_CHAR(break_time, 'YYYYMMDDHH24MI')
                FROM pattern_events
                WHERE state IN ('BREAKOUT','WAITING_RETEST','RETEST')
                  AND created_at >= NOW() - INTERVAL '14 days'
                """
            )
            return {row[0] for row in cur.fetchall()}
    except Exception:
        return set()


def _insert_breakout(conn, symbol: str, tf: str, pattern: str,
                     direction: str, break_price: float,
                     slope_h: float, int_h: float,
                     slope_l: float, int_l: float) -> int | None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pattern_events (
                    symbol, timeframe, pattern, direction, state,
                    break_price, break_time,
                    slope_high, intercept_high, slope_low, intercept_low
                ) VALUES (%s,%s,%s,%s,'WAITING_RETEST',
                          %s,NOW(),%s,%s,%s,%s)
                RETURNING id
                """,
                (symbol, tf, pattern, direction,
                 float(break_price),
                 float(slope_h), float(int_h),
                 float(slope_l), float(int_l)),
            )
            row = cur.fetchone()
        conn.commit()
        return row[0] if row else None
    except Exception as e:
        logger.error(f"Insert breakout error {symbol}/{tf}: {e}")
        conn.rollback()
        return None


def _mark_fakeout(conn, event_id: int) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pattern_events SET state='FAKEOUT', updated_at=NOW() WHERE id=%s",
                (event_id,),
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"Mark fakeout error: {e}")


def _mark_retest(conn, event_id: int, retest_price: float) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pattern_events SET
                    state='RETEST', retest_price=%s, retest_time=NOW(),
                    updated_at=NOW()
                WHERE id=%s
                """,
                (retest_price, event_id),
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"Mark retest error: {e}")


def _expire_old(conn) -> None:
    """Expire old WAITING_RETEST events based on timeframe candle count."""
    try:
        with conn.cursor() as cur:
            for tf, max_c in EXPIRY_CANDLES.items():
                minutes = {"1h":60,"2h":120,"4h":240,"1d":1440}.get(tf, 60)
                cur.execute(
                    """
                    UPDATE pattern_events SET state='EXPIRED', updated_at=NOW()
                    WHERE state IN ('WAITING_RETEST','BREAKOUT')
                      AND timeframe = %s
                      AND break_time < NOW() - INTERVAL '%s minutes'
                    """,
                    (tf, max_c * minutes),
                )
        conn.commit()
    except Exception as e:
        logger.warning(f"Expire old patterns error: {e}")


def _send_alert(conn, channel_id: int, message: str,
                image_path: str | None = None) -> None:
    try:
        with conn.cursor() as cur:
            if image_path:
                cur.execute(
                    "INSERT INTO telegram_outbox (channel_id,message,image_path) VALUES (%s,%s,%s)",
                    (channel_id, message, image_path),
                )
            else:
                cur.execute(
                    "INSERT INTO telegram_outbox (channel_id,message) VALUES (%s,%s)",
                    (channel_id, message),
                )
        conn.commit()
    except Exception as e:
        logger.error(f"Alert send error: {e}")


# ── Pattern detection ─────────────────────────────────────────────────────────

def _find_pivots(df: pd.DataFrame, order: int) -> tuple[pd.Index, pd.Index]:
    """Returns (high_pivot_indices, low_pivot_indices) using argrelextrema."""
    highs = df["high"].values
    lows  = df["low"].values
    hi_idx = scipy.signal.argrelextrema(highs, np.greater, order=order)[0]
    lo_idx = scipy.signal.argrelextrema(lows,  np.less,   order=order)[0]
    return hi_idx, lo_idx


def _classify_pattern(slope_h_pct: float, slope_l_pct: float) -> str | None:
    """
    Classifies a pattern based on normalised slope percentages.
    V4: FLAT_THRESHOLD raised to 0.10 — much stricter than V3 (0.02).
    """
    fh = abs(slope_h_pct) <= FLAT_THRESHOLD
    fl = abs(slope_l_pct) <= FLAT_THRESHOLD

    if slope_h_pct < -FLAT_THRESHOLD and slope_l_pct > FLAT_THRESHOLD:
        return "Symmetrical Triangle"
    if fh and slope_l_pct > FLAT_THRESHOLD:
        return "Ascending Triangle"
    if slope_h_pct < -FLAT_THRESHOLD and fl:
        return "Descending Triangle"
    if (slope_h_pct > FLAT_THRESHOLD and slope_l_pct > FLAT_THRESHOLD
            and abs(slope_h_pct - slope_l_pct) < FLAT_THRESHOLD * 2):
        return "Ascending Channel"
    if (slope_h_pct < -FLAT_THRESHOLD and slope_l_pct < -FLAT_THRESHOLD
            and abs(slope_h_pct - slope_l_pct) < FLAT_THRESHOLD * 2):
        return "Descending Channel"
    return None


def _scan_symbol_tf(conn, symbol: str, tf: str,
                    active_ids: set, info_ch: int) -> None:
    df = _load_ohlcv(conn, symbol, tf, limit=200)
    if len(df) < MIN_CANDLES:
        return

    order   = PIVOT_ORDER.get(tf, 5)
    hi_idx, lo_idx = _find_pivots(df, order)

    # Need at least MIN_PIVOTS of each
    if len(hi_idx) < MIN_PIVOTS or len(lo_idx) < MIN_PIVOTS:
        return

    # Use last 3 pivots for regression
    recent_hi = hi_idx[-3:]
    recent_lo = lo_idx[-3:]

    slope_h, int_h, _, _, _ = stats.linregress(
        recent_hi.astype(float), df["high"].values[recent_hi]
    )
    slope_l, int_l, _, _, _ = stats.linregress(
        recent_lo.astype(float), df["low"].values[recent_lo]
    )
    # Cast numpy scalar → Python float to avoid psycopg2 "schema np" error
    slope_h = float(slope_h)
    int_h   = float(int_h)
    slope_l = float(slope_l)
    int_l   = float(int_l)

    avg_price   = float(df["close"].mean())
    slope_h_pct = (slope_h / avg_price) * 100
    slope_l_pct = (slope_l / avg_price) * 100

    pattern = _classify_pattern(slope_h_pct, slope_l_pct)
    if pattern is None:
        return

    # Use pivot time for stable pattern_id
    pivot_time = df["open_time"].iloc[recent_hi[-1]].strftime("%Y%m%d%H%M")
    pattern_id = f"{symbol}_{tf}_{pattern.replace(' ','_')}_{pivot_time}"

    curr_idx = len(df) - 2   # last confirmed candle
    prev_idx = curr_idx - 1

    up_curr = float(slope_h * curr_idx + int_h)
    lo_curr = float(slope_l * curr_idx + int_l)
    up_prev = float(slope_h * prev_idx + int_h)
    lo_prev = float(slope_l * prev_idx + int_l)

    c_open  = float(df["open"].iloc[curr_idx])
    c_close = float(df["close"].iloc[curr_idx])
    c_high  = float(df["high"].iloc[curr_idx])
    c_low   = float(df["low"].iloc[curr_idx])
    p_close = float(df["close"].iloc[prev_idx])

    # ── BREAKOUT CHECK ────────────────────────────────────────────────────────
    breakout_dir = None
    if c_close > up_curr and p_close <= up_prev:
        breakout_dir = "BULLISH"
    elif c_close < lo_curr and p_close >= lo_prev:
        breakout_dir = "BEARISH"

    if breakout_dir and pattern_id not in active_ids:
        ev_id = _insert_breakout(
            conn, symbol, tf, pattern, breakout_dir,
            c_close, slope_h, int_h, slope_l, int_l,
        )
        if ev_id:
            active_ids.add(pattern_id)
            chart = generate_chart(symbol, minutes=240)
            coin  = symbol.replace("USDT","")
            emoji = "🟢" if breakout_dir == "BULLISH" else "🔴"
            msg   = (
                f"<pre><b>{emoji} PATTERN BREAKOUT</b>\n"
                f"<b>{coin}/USDT</b>  [{tf}]\n"
                f"→ Pattern: {pattern}\n"
                f"→ Direction: <b>{breakout_dir}</b>\n"
                f"→ Price: <code>${c_close:,.4f}</code>\n"
                f"⏳ Waiting for retest...</pre>"
            )
            _send_alert(conn, info_ch, msg, chart)
            logger.info(f"BREAKOUT {symbol}/{tf} {pattern} {breakout_dir}")
        return

    # ── RETEST CHECK (for active patterns) ────────────────────────────────────
    # Find matching active event in DB
    if pattern_id not in active_ids:
        return

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, direction, state, retest_price
                FROM pattern_events
                WHERE symbol=%s AND timeframe=%s AND pattern=%s
                  AND state IN ('WAITING_RETEST','RETEST')
                ORDER BY created_at DESC LIMIT 1
                """,
                (symbol, tf, pattern),
            )
            row = cur.fetchone()
    except Exception:
        return

    if not row:
        return

    ev_id, direction, state, _ = row
    is_bullish = direction == "BULLISH"
    line = up_curr if is_bullish else lo_curr

    # Fakeout — deep break back into pattern
    if ((is_bullish and c_close < line * (1 - RETEST_FAIL_PCT)) or
            (not is_bullish and c_close > line * (1 + RETEST_FAIL_PCT))):
        _mark_fakeout(conn, ev_id)
        active_ids.discard(pattern_id)
        chart = generate_chart(symbol, minutes=240)
        coin  = symbol.replace("USDT","")
        msg   = (
            f"<pre><b>❌ FAKEOUT</b>\n"
            f"<b>{coin}/USDT</b>  [{tf}]\n"
            f"→ Pattern: {pattern}\n"
            f"→ Price returned deep into pattern</pre>"
        )
        _send_alert(conn, info_ch, msg, chart)
        logger.info(f"FAKEOUT {symbol}/{tf} {pattern}")
        return

    # Retest touch
    touched = (
        (is_bullish and c_low <= line * (1 + RETEST_TOUCH_PCT)) or
        (not is_bullish and c_high >= line * (1 - RETEST_TOUCH_PCT))
    )
    closed_correct = (
        (is_bullish and c_close > line) or
        (not is_bullish and c_close < line)
    )

    if touched and closed_correct and state == "WAITING_RETEST":
        _mark_retest(conn, ev_id, float((c_high + c_low) / 2))
        chart = generate_chart(symbol, minutes=240)
        coin  = symbol.replace("USDT","")
        retest_price = (c_high + c_low) / 2
        msg   = (
            f"<pre><b>🔄 RETEST IN PROGRESS</b>\n"
            f"<b>{coin}/USDT</b>  [{tf}]\n"
            f"→ Pattern: {pattern}\n"
            f"→ Retest @ <code>${retest_price:,.4f}</code>\n"
            f"⏳ Waiting for confirmation...</pre>"
        )
        _send_alert(conn, info_ch, msg, chart)
        logger.info(f"RETEST {symbol}/{tf} {pattern} @ ${retest_price:.4f}")


# ── Main loop ─────────────────────────────────────────────────────────────────

def _load_coins() -> list[str]:
    try:
        with open(os.path.join(_SCRIPT_DIR, "coins.json")) as f:
            data = json.load(f)
        coins = data if isinstance(data, list) else data.get("coins", [])
        return [c.upper() for c in coins if c.upper().endswith("USDT")]
    except Exception as e:
        logger.error(f"Could not load coins.json: {e}")
        return []


def _get_info_channel() -> int:
    """Returns the pattern info channel ID from config."""
    try:
        from core.config import PATTERN_INFO_CHANNEL_ID
        return PATTERN_INFO_CHANNEL_ID
    except ImportError:
        # Fallback — add PATTERN_INFO_CHANNEL_ID to config + .env
        logger.warning("PATTERN_INFO_CHANNEL_ID not in config — using 0")
        return 0


def _run_scan() -> None:
    coins   = _load_coins()
    info_ch = _get_info_channel()
    if not coins:
        return

    logger.info(f"Pattern scan — {len(coins)} symbols × {len(PATTERN_TIMEFRAMES)} TFs...")
    t0 = time.time()

    with db_connection() as conn:
        _expire_old(conn)
        active_ids = _active_pattern_ids(conn)

        for symbol in coins:
            for tf in PATTERN_TIMEFRAMES:
                try:
                    _scan_symbol_tf(conn, symbol, tf, active_ids, info_ch)
                except Exception as e:
                    logger.debug(f"{symbol}/{tf} error: {e}")

    elapsed = time.time() - t0
    logger.info(f"Pattern scan complete — {elapsed:.0f}s")


def main() -> None:
    logger.info("=" * 60)
    logger.info("Pattern Detector V4 — starting")
    logger.info("=" * 60)

    schema = verify_schema()
    if schema["missing"]:
        logger.error(f"Missing tables: {schema['missing']}")
        sys.exit(1)
    logger.info(f"Schema OK — {len(schema['ok'])} tables verified.")

    shutdown = ShutdownHandler("PAT_DETECT")
    logger.info("Waiting for :03 of each hour to scan...")

    while not shutdown.is_set():
        now = datetime.datetime.now(datetime.timezone.utc)
        if now.minute == 3:
            _run_scan()
            shutdown.sleep(90)
        else:
            shutdown.sleep(10)

    logger.info("Pattern Detector stopped.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Pattern Detector stopped (Ctrl+C).")

