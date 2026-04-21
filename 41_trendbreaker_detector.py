# 41_trendbreaker_detector.py
# Trendline Break & Bounce Detector V4 (ATB-1 Detection)
#
# Detects trendline breaks and bounces on 1h candles (90-day lookback).
# Writes events to trendline_events table — does NOT generate trades.
# Trading is handled separately by 51_trendbreaker_trader.py.
#
# Break logic (V4 — stricter than V3):
#   BREAK_UP:   prev_close was BELOW trendline, curr_close is ABOVE
#               AND distance >= BREAK_MIN_PCT (0.5%)
#               AND volume >= VOL_BREAK_RATIO × avg_volume (1.5×)
#   BREAK_DOWN: opposite
#
# Bounce logic:
#   BOUNCE_UP:   price approaches from above, touches within BOUNCE_RADAR_PCT (2%)
#                then closes back above — confirmed bounce
#   BOUNCE_DOWN: opposite
#
# Key improvements vs V3:
#   - No "near" or "unknown" as break origin → prevents mass-alerts on restart
#   - Strict volume confirmation for breaks
#   - BOUNCE_RADAR_PCT reduced from 5% → 2%
#   - Trendline R² filter — only trades strong trends (R² ≥ MIN_R_SQUARED)
#   - State persisted in DB (trendline_events) not JSON file

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
    format="%(asctime)s - ATB_DETECT - %(levelname)s - %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            os.path.join(_LOG_DIR, "trendbreaker_detector.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

from core.config import ATB1_CHANNEL_ID
from core.database import db_connection
from core.schema import verify_schema
from core.shutdown import ShutdownHandler
from core.charting import generate_trendbreaker_chart

# ── Constants ─────────────────────────────────────────────────────────────────

SCAN_INTERVAL_S    = 3600          # run every hour (at minute :03)
LOOKBACK_DAYS      = 90            # trendline lookback
MIN_CANDLES        = 50            # skip symbol if less data
PIVOT_DISTANCE     = 8             # scipy find_peaks distance param

# Trendline quality filter
MIN_R_VALUE        = 0.3           # minimum abs(r_value) for valid trendline

# Break detection
BREAK_MIN_PCT      = 0.005         # 0.5% — min distance beyond trendline to call a break
VOL_BREAK_RATIO    = 1.5           # volume must be 1.5× the 20-period average

# Bounce detection
BOUNCE_RADAR_PCT   = 0.02          # 2% — only check coins within 2% of trendline
BOUNCE_TOUCH_PCT   = 0.012         # 1.2% — "touching" the line

# Event expiry
BREAK_EXPIRY_H     = 72            # expire WAITING_RETEST after 72h
BOUNCE_EXPIRY_H    = 24            # expire DETECTED bounce after 24h

# Info channel (alerts without trades)
INFO_CHANNEL_ID    = ATB1_CHANNEL_ID   # reuse ATB1 info channel


# ── DB helpers ────────────────────────────────────────────────────────────────

def _load_ohlcv(conn, symbol: str) -> pd.DataFrame:
    """Loads 90d of 1h candles for a symbol."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT open_time, open, high, low, close, volume
                FROM ohlcv_1h
                WHERE symbol = %s
                  AND open_time >= NOW() - INTERVAL '%s days'
                ORDER BY open_time ASC
                """,
                (symbol, LOOKBACK_DAYS + 5),
            )
            rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume"])
        for c in ("open","high","low","close","volume"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        return df.dropna().reset_index(drop=True)
    except Exception as e:
        logger.debug(f"OHLCV load error {symbol}: {e}")
        conn.rollback()
        return pd.DataFrame()


def _insert_event(conn, symbol: str, trend_dir: str, event_type: str,
                   slope: float, intercept: float, trend_value: float,
                   break_price: float, distance_pct: float,
                   volume_ratio: float) -> int | None:
    """Inserts a new trendline event and returns its id."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trendline_events (
                    symbol, trend_direction, event_type, state,
                    slope, intercept, trend_value,
                    break_price, break_time, distance_pct, volume_ratio
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s)
                RETURNING id
                """,
                (
                    symbol, trend_dir, event_type,
                    "WAITING_RETEST" if "BREAK" in event_type else "DETECTED",
                    slope, intercept, trend_value,
                    break_price, distance_pct, volume_ratio,
                ),
            )
            row = cur.fetchone()
        conn.commit()
        return row[0] if row else None
    except Exception as e:
        logger.error(f"Insert event error {symbol}: {e}")
        conn.rollback()
        return None


def _expire_old_events(conn) -> None:
    """Marks old waiting events as EXPIRED."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE trendline_events SET state = 'EXPIRED', updated_at = NOW()
                WHERE state = 'WAITING_RETEST'
                  AND break_time < NOW() - INTERVAL '%s hours'
                """,
                (BREAK_EXPIRY_H,),
            )
            cur.execute(
                """
                UPDATE trendline_events SET state = 'EXPIRED', updated_at = NOW()
                WHERE state = 'DETECTED'
                  AND event_type IN ('BOUNCE_UP','BOUNCE_DOWN')
                  AND created_at < NOW() - INTERVAL '%s hours'
                """,
                (BOUNCE_EXPIRY_H,),
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"Expire events error: {e}")
        conn.rollback()


# Cooldown after a CONFIRMED or EXPIRED event — prevents re-alerting
# the same coin/direction within a short time window.
COOLDOWN_AFTER_BREAK_H  = 6    # hours cooldown after any break event expires
COOLDOWN_AFTER_BOUNCE_H = 4    # hours cooldown after any bounce event expires


def _already_active(conn, symbol: str, event_type: str) -> bool:
    """
    Returns True if this symbol+event_type should be suppressed.

    Two independent checks:

    1. EXACT MATCH — same symbol + same event_type is still active
       (DETECTED or WAITING_RETEST) → never create a duplicate

    2. SAME DIRECTION cooldown — any event of the same broad direction
       (BREAK or BOUNCE) on this symbol was recently CONFIRMED or EXPIRED
       → prevents rapid re-alerting after an event resolves

       Direction grouping:
         BREAK_UP / BREAK_DOWN  → "BREAK" group
         BOUNCE_UP / BOUNCE_DOWN → "BOUNCE" group

       Within-group cooldown: if BTCUSDT just had a BREAK_DOWN expire,
       a new BREAK_UP is also blocked for cooldown_h hours.
       This catches the common case of a coin oscillating around its
       trendline and triggering alternating UP/DOWN alerts repeatedly.
    """
    is_break   = "BREAK" in event_type
    cooldown_h = COOLDOWN_AFTER_BREAK_H if is_break else COOLDOWN_AFTER_BOUNCE_H
    # Match any event_type in the same group
    group_types = ("BREAK_UP", "BREAK_DOWN") if is_break else ("BOUNCE_UP", "BOUNCE_DOWN")

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM trendline_events
                WHERE symbol = %s
                  AND (
                    -- Check 1: exact match still active
                    (event_type = %s AND state IN ('DETECTED','WAITING_RETEST'))
                    OR
                    -- Check 2: any same-group event recently expired/confirmed
                    (event_type = ANY(%s)
                     AND state IN ('CONFIRMED','EXPIRED')
                     AND updated_at >= NOW() - INTERVAL '1 hour' * %s)
                  )
                LIMIT 1
                """,
                (symbol, event_type, list(group_types), cooldown_h),
            )
            return cur.fetchone() is not None
    except Exception as e:
        logger.debug(f"_already_active error: {e}")
        return False


def _send_alert(conn, message: str, image_path: str | None = None) -> None:
    try:
        with conn.cursor() as cur:
            if image_path:
                cur.execute(
                    "INSERT INTO telegram_outbox (channel_id,message,image_path) VALUES (%s,%s,%s)",
                    (INFO_CHANNEL_ID, message, image_path),
                )
            else:
                cur.execute(
                    "INSERT INTO telegram_outbox (channel_id,message) VALUES (%s,%s)",
                    (INFO_CHANNEL_ID, message),
                )
        conn.commit()
    except Exception as e:
        logger.error(f"Alert send error: {e}")


# ── Trendline math ────────────────────────────────────────────────────────────

def _detect_trendline(df: pd.DataFrame) -> tuple[str, float, float, float] | None:
    """
    Detects the dominant trendline (UP or DOWN) using pivot points.

    Returns (direction, slope, intercept, r_value) or None if no valid trend.

    UP trend:  regression on swing LOWS  → slope > 0
    DOWN trend: regression on swing HIGHS → slope < 0
    """
    highs = df["high"].values
    lows  = df["low"].values
    n     = len(df)

    high_peaks = scipy.signal.find_peaks(highs,  distance=PIVOT_DISTANCE)[0]
    low_peaks  = scipy.signal.find_peaks(-lows,  distance=PIVOT_DISTANCE)[0]

    def _regress(indices, values):
        if len(indices) < 3:
            return None
        x = indices.astype(float)
        y = values[indices]
        slope, intercept, r_value, _, _ = stats.linregress(x, y)
        if abs(r_value) < MIN_R_VALUE:
            return None
        return float(slope), float(intercept), float(r_value)

    # Try DOWN trend (descending highs)
    res_down = _regress(high_peaks, highs)
    if res_down and res_down[0] < 0:
        return ("DOWN", res_down[0], res_down[1], res_down[2])

    # Try UP trend (ascending lows)
    res_up = _regress(low_peaks, lows)
    if res_up and res_up[0] > 0:
        return ("UP", res_up[0], res_up[1], res_up[2])

    return None


def _vol_ratio(df: pd.DataFrame, window: int = 20) -> float:
    """Returns current volume / average of last N periods."""
    if len(df) < window + 1:
        return 0.0
    avg = float(df["volume"].iloc[-(window+1):-1].mean())
    curr = float(df["volume"].iloc[-1])
    return curr / avg if avg > 0 else 0.0


# ── Per-symbol scan ───────────────────────────────────────────────────────────

def _scan_symbol(conn, symbol: str) -> None:
    df = _load_ohlcv(conn, symbol)
    if len(df) < MIN_CANDLES:
        return

    result = _detect_trendline(df)
    if result is None:
        return

    trend_dir, slope, intercept, r_val = result

    last_idx   = len(df) - 1
    prev_idx   = last_idx - 1

    trend_last = slope * last_idx + intercept
    trend_prev = slope * prev_idx + intercept

    last_close = float(df["close"].iloc[-1])
    prev_close = float(df["close"].iloc[-2])

    # Distance from trendline (positive = above, negative = below)
    dist_last = (last_close - trend_last) / last_close
    dist_prev = (prev_close - trend_prev) / prev_close

    vol_r = _vol_ratio(df)
    now   = datetime.datetime.now(datetime.timezone.utc)

    # ── BREAK DETECTION ───────────────────────────────────────────────────────
    # Strict: previous close on one side, current close on OTHER side
    # + minimum 0.5% distance + volume confirmation

    if (dist_prev < 0 and dist_last >= BREAK_MIN_PCT
            and vol_r >= VOL_BREAK_RATIO
            and not _already_active(conn, symbol, "BREAK_UP")):
        # BREAK UP — was below, now above with momentum
        event_id = _insert_event(
            conn, symbol, trend_dir, "BREAK_UP",
            slope, intercept, trend_last,
            last_close, dist_last * 100, vol_r,
        )
        if event_id:
            chart = generate_trendbreaker_chart(
                symbol, trend_dir, slope, intercept, "BREAK_UP",
                distance_pct=dist_last*100, volume_ratio=vol_r,
            )
            coin  = symbol.replace("USDT","")
            msg   = (
                f"<pre><b>🚀 TRENDLINE BREAK UP</b>\n"
                f"<b>{coin}/USDT</b>  [{trend_dir} trend]\n"
                f"→ Close: <code>${last_close:,.4f}</code>\n"
                f"→ Trendline: <code>${trend_last:,.4f}</code>\n"
                f"→ Distance: <b>+{dist_last*100:.2f}%</b>\n"
                f"→ Volume: <b>{vol_r:.1f}× avg</b>\n"
                f"→ R²: {r_val**2:.2f}\n"
                f"⏳ Waiting for retest...</pre>"
            )
            _send_alert(conn, msg, chart)
            logger.info(f"BREAK_UP {symbol} +{dist_last*100:.2f}% vol={vol_r:.1f}× R²={r_val**2:.2f}")

    elif (dist_prev > 0 and dist_last <= -BREAK_MIN_PCT
            and vol_r >= VOL_BREAK_RATIO
            and not _already_active(conn, symbol, "BREAK_DOWN")):
        # BREAK DOWN — was above, now below with momentum
        event_id = _insert_event(
            conn, symbol, trend_dir, "BREAK_DOWN",
            slope, intercept, trend_last,
            last_close, dist_last * 100, vol_r,
        )
        if event_id:
            chart = generate_trendbreaker_chart(
                symbol, trend_dir, slope, intercept, "BREAK_DOWN",
                distance_pct=dist_last*100, volume_ratio=vol_r,
            )
            coin  = symbol.replace("USDT","")
            msg   = (
                f"<pre><b>💥 TRENDLINE BREAK DOWN</b>\n"
                f"<b>{coin}/USDT</b>  [{trend_dir} trend]\n"
                f"→ Close: <code>${last_close:,.4f}</code>\n"
                f"→ Trendline: <code>${trend_last:,.4f}</code>\n"
                f"→ Distance: <b>{dist_last*100:.2f}%</b>\n"
                f"→ Volume: <b>{vol_r:.1f}× avg</b>\n"
                f"→ R²: {r_val**2:.2f}\n"
                f"⏳ Waiting for retest...</pre>"
            )
            _send_alert(conn, msg, chart)
            logger.info(f"BREAK_DOWN {symbol} {dist_last*100:.2f}% vol={vol_r:.1f}× R²={r_val**2:.2f}")

    # ── BOUNCE DETECTION ──────────────────────────────────────────────────────
    # Coin is within BOUNCE_RADAR_PCT of trendline → check for bounce

    elif abs(dist_last) <= BOUNCE_RADAR_PCT:

        if (dist_prev > BOUNCE_TOUCH_PCT
                and abs(dist_last) <= BOUNCE_TOUCH_PCT
                and last_close > prev_close
                and not _already_active(conn, symbol, "BOUNCE_UP")):
            # BOUNCE UP — came from above, touched line, closed back up
            event_id = _insert_event(
                conn, symbol, trend_dir, "BOUNCE_UP",
                slope, intercept, trend_last,
                last_close, dist_last * 100, vol_r,
            )
            if event_id:
                chart = generate_trendbreaker_chart(
                    symbol, trend_dir, slope, intercept, "BOUNCE_UP",
                    distance_pct=dist_last*100, volume_ratio=vol_r,
                )
                coin  = symbol.replace("USDT","")
                msg   = (
                    f"<pre><b>🔄 TRENDLINE BOUNCE UP</b>\n"
                    f"<b>{coin}/USDT</b>  [{trend_dir} trend]\n"
                    f"→ Close: <code>${last_close:,.4f}</code>\n"
                    f"→ Trendline: <code>${trend_last:,.4f}</code>\n"
                    f"→ Distance: <b>{dist_last*100:+.2f}%</b>\n"
                    f"→ Volume: <b>{vol_r:.1f}× avg</b></pre>"
                )
                _send_alert(conn, msg, chart)
                logger.info(f"BOUNCE_UP {symbol} dist={dist_last*100:.2f}%")

        elif (dist_prev < -BOUNCE_TOUCH_PCT
                and abs(dist_last) <= BOUNCE_TOUCH_PCT
                and last_close < prev_close
                and not _already_active(conn, symbol, "BOUNCE_DOWN")):
            # BOUNCE DOWN — came from below, touched line, closed back down
            event_id = _insert_event(
                conn, symbol, trend_dir, "BOUNCE_DOWN",
                slope, intercept, trend_last,
                last_close, dist_last * 100, vol_r,
            )
            if event_id:
                chart = generate_trendbreaker_chart(
                    symbol, trend_dir, slope, intercept, "BOUNCE_DOWN",
                    distance_pct=dist_last*100, volume_ratio=vol_r,
                )
                coin  = symbol.replace("USDT","")
                msg   = (
                    f"<pre><b>🔄 TRENDLINE BOUNCE DOWN</b>\n"
                    f"<b>{coin}/USDT</b>  [{trend_dir} trend]\n"
                    f"→ Close: <code>${last_close:,.4f}</code>\n"
                    f"→ Trendline: <code>${trend_last:,.4f}</code>\n"
                    f"→ Distance: <b>{dist_last*100:+.2f}%</b>\n"
                    f"→ Volume: <b>{vol_r:.1f}× avg</b></pre>"
                )
                _send_alert(conn, msg, chart)
                logger.info(f"BOUNCE_DOWN {symbol} dist={dist_last*100:.2f}%")


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


def _run_scan() -> None:
    coins = _load_coins()
    if not coins:
        return

    logger.info(f"Trendbreaker scan — {len(coins)} symbols...")
    t0      = time.time()
    found   = 0
    skipped = 0

    with db_connection() as conn:
        _expire_old_events(conn)

        for symbol in coins:
            try:
                before = found
                _scan_symbol(conn, symbol)
            except Exception as e:
                logger.debug(f"{symbol} error: {e}")
                skipped += 1

    elapsed = time.time() - t0
    logger.info(
        f"Trendbreaker scan complete — {len(coins)} symbols, "
        f"{elapsed:.0f}s, {skipped} skipped"
    )


def main() -> None:
    logger.info("=" * 60)
    logger.info("Trendbreaker Detector V4 (ATB-1) — starting")
    logger.info("=" * 60)

    schema = verify_schema()
    if schema["missing"]:
        logger.error(f"Missing tables: {schema['missing']}")
        sys.exit(1)
    logger.info(f"Schema OK — {len(schema['ok'])} tables verified.")

    shutdown = ShutdownHandler("ATB_DETECT")
    logger.info("Waiting for :03 of each hour to scan...")

    while not shutdown.is_set():
        now = datetime.datetime.now(datetime.timezone.utc)
        if now.minute == 3:
            _run_scan()
            # Sleep past the trigger minute
            shutdown.sleep(90)
        else:
            shutdown.sleep(10)

    logger.info("Trendbreaker Detector stopped.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Trendbreaker Detector stopped (Ctrl+C).")




