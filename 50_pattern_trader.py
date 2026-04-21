# 50_pattern_trader.py
# Pattern Trader V4 (BR-1)
#
# Polls pattern_events table for RETEST state and generates trades
# when retest is confirmed with a strong candle.
#
# Flow:
#   1. 40_pattern_detector.py writes WAITING_RETEST events
#   2. When detector marks state=RETEST, this bot picks it up
#   3. Checks current candle for confirmation (strong body close on break side)
#   4. If confirmed: calculates SL/TP via calculate_smart_targets, sends trade
#
# Bot name: BR-{tf}  (e.g. BR-1h, BR-4h)
# Channels: BR_CHANNELS dict from config (per timeframe)

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

from dotenv import load_dotenv
load_dotenv()

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR     = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - PAT_TRADER - %(levelname)s - %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            os.path.join(_LOG_DIR, "pattern_trader.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

from core.config import BR_CHANNELS
from core.database import db_connection, get_db_connection
from core.schema import verify_schema
from core.shutdown import ShutdownHandler
from core.bootstrap import load_coins
from core.charting import generate_chart

# ── Constants ─────────────────────────────────────────────────────────────────

POLL_INTERVAL_S  = 60        # check every 60 seconds
BOT_PREFIX       = "BR"      # BR-1h, BR-2h, BR-4h, BR-1d
COOLDOWN_H       = {         # per timeframe
    "1h": 6, "2h": 12, "4h": 24, "1d": 72,
}
LEVERAGE_DEFAULT = 20

# Confirmation: min candle body size to confirm retest
CONFIRM_BODY_PCT = 0.005     # 0.5%

# Max age of RETEST event before giving up (in hours per TF)
RETEST_MAX_AGE_H = {
    "1h": 4, "2h": 6, "4h": 10, "1d": 36,
}


# ── Trade helpers ─────────────────────────────────────────────────────────────

def _get_atr(df: pd.DataFrame, period: int = 14) -> float:
    hl  = df["high"] - df["low"]
    hc  = (df["high"] - df["close"].shift()).abs()
    lc  = (df["low"]  - df["close"].shift()).abs()
    tr  = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    atr = float(tr.rolling(period).mean().iloc[-1])
    return atr if np.isfinite(atr) else float(df["close"].iloc[-1]) * 0.02


def _calculate_targets(conn, symbol: str, direction: str,
                        entry: float) -> dict:
    """
    Calculates SL and TPs using S/R levels + Fibonacci.
    Wrapper around V3 calculate_smart_targets logic — adapted for V4.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT open_time, open, high, low, close, volume
                FROM ohlcv_1h
                WHERE symbol = %s
                  AND open_time >= NOW() - INTERVAL '42 days'
                ORDER BY open_time ASC
                """,
                (symbol,),
            )
            rows = cur.fetchall()

        if len(rows) < 100:
            raise ValueError("Insufficient data")

        df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume"])
        for c in ("open","high","low","close","volume"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna().reset_index(drop=True)

        is_long = direction == "LONG"
        atr     = _get_atr(df)
        atr     = min(atr, entry * 0.04)   # cap at 4%

        highs = df["high"].values
        lows  = df["low"].values

        # S/R via argrelextrema
        res_idx = scipy.signal.argrelextrema(highs, np.greater, order=20)[0]
        sup_idx = scipy.signal.argrelextrema(lows,  np.less,    order=20)[0]
        resistances = [float(highs[i]) for i in res_idx]
        supports    = [float(lows[i])  for i in sup_idx]

        # Fibonacci
        sh = float(np.max(highs[-300:]))
        sl = float(np.min(lows[-300:]))
        rng = sh - sl
        fibs = []
        if rng > 0:
            for x in [0.236, 0.382, 0.5, 0.618, 0.786]:
                fibs.append(sh - rng * x)
            for x in [1.272, 1.618, 2.0, 2.618]:
                fibs.append(sl + rng * x)

        all_levels = sorted(set(supports + resistances + fibs))

        min_sl_dist  = atr * 3.0
        min_tp1_dist = atr * 2.0
        min_tp_space = atr * 1.0
        max_sl_pct   = 0.15

        if is_long:
            sl_cands = [x for x in all_levels
                        if x < (entry - min_sl_dist)
                        and x >= entry * (1 - max_sl_pct)]
            sl_price = max(sl_cands) if sl_cands else max(
                entry - min_sl_dist, entry * (1 - max_sl_pct))

            tp_cands = sorted([x for x in all_levels
                                if x >= entry + min_tp1_dist
                                and x <= entry * 3.0])
        else:
            sl_cands = [x for x in all_levels
                        if x > (entry + min_sl_dist)
                        and x <= entry * (1 + max_sl_pct)]
            sl_price = min(sl_cands) if sl_cands else min(
                entry + min_sl_dist, entry * (1 + max_sl_pct))

            tp_cands = sorted([x for x in all_levels
                                if x <= entry - min_tp1_dist
                                and x >= entry * 0.1], reverse=True)

        # Filter TPs: maintain spacing
        tps   = []
        last  = entry
        for t in tp_cands:
            if abs(t - last) >= min_tp_space:
                tps.append(round(t, 8))
                last = t
            if len(tps) >= 6:
                break

        # Fallback TP
        if not tps:
            tps = [round(entry * 1.05 if is_long else entry * 0.95, 8)]

        return {
            "entry": round(entry, 8),
            "sl":    round(sl_price, 8),
            "tps":   tps,
        }

    except Exception as e:
        logger.warning(f"Target calc fallback for {symbol}: {e}")
        is_long = direction == "LONG"
        return {
            "entry": round(entry, 8),
            "sl":    round(entry * 0.93 if is_long else entry * 1.07, 8),
            "tps":   [round(entry * 1.05 if is_long else entry * 0.95, 8)],
        }


def _build_cornix_message(symbol: str, direction: str, leverage: int,
                           setup: dict, bot_name: str) -> str:
    lines = [
        f"📈 Signal for {symbol} 📈",
        f"🚨 Direction: {direction}",
        f"🚨 Leverage: {leverage}x",
        f"🚨 Margin: Cross",
        f"🏦 Entry: $ {setup['entry']:.8f}",
    ]
    for i, tp in enumerate(setup["tps"], 1):
        lines.append(f"💰 TP{i}: $ {tp:.8f}")
    lines += [
        f"💸 Stop Loss: $ {setup['sl']:.8f}",
        f"🧠 Trade by {bot_name} V4",
    ]
    return "\n".join(lines)


def _get_max_leverage(symbol: str, default: int = 20) -> int:
    try:
        from core.bootstrap import load_coins
        # leverage is stored in coins metadata — simple lookup
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT max_leverage FROM coins WHERE symbol=%s LIMIT 1",
                    (symbol,),
                )
                row = cur.fetchone()
        if row and row[0]:
            return min(int(row[0]), default)
    except Exception:
        pass
    return default


def _cooldown_active(conn, bot_name: str, symbol: str,
                      direction: str, hours: int) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM trade_cooldowns
                WHERE bot_name=%s AND symbol=%s AND direction=%s
                  AND expires_at > NOW()
                LIMIT 1
                """,
                (bot_name, symbol, direction),
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def _set_cooldown(conn, bot_name: str, symbol: str,
                   direction: str, hours: int) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trade_cooldowns (bot_name, symbol, direction, expires_at)
                VALUES (%s,%s,%s, NOW() + INTERVAL '%s hours')
                ON CONFLICT (bot_name, symbol, direction)
                DO UPDATE SET expires_at = NOW() + INTERVAL '%s hours'
                """,
                (bot_name, symbol, direction, hours, hours),
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"Cooldown set error: {e}")


def _post_trade(conn, symbol: str, direction: str, bot_name: str,
                tf: str, channel_id: int, setup: dict,
                event_id: int, ml_confidence: float | None,
                chart_path: str | None) -> None:
    """Writes trade to trades table + telegram_outbox."""
    leverage = _get_max_leverage(symbol, LEVERAGE_DEFAULT)
    tps      = setup["tps"]
    tp_count = len(tps)

    # Build cornix message
    cornix = _build_cornix_message(symbol, direction, leverage, setup, bot_name)

    # HTML info caption
    emoji = "🚀" if direction == "LONG" else "💥"
    caption = (
        f"<pre><b>{emoji} {bot_name} {direction}</b>\n"
        f"<b>{symbol.replace('USDT','')}/USDT</b>  [{tf}]\n"
        f"→ Entry: <code>${setup['entry']:,.4f}</code>\n"
        f"→ SL:    <code>${setup['sl']:,.4f}</code>\n"
        f"→ TP1:   <code>${tps[0]:,.4f}</code>\n"
        + (f"→ ML Confidence: <b>{ml_confidence:.1%}</b>\n" if ml_confidence else "")
        + f"→ TPs: {tp_count}</pre>"
    )

    tp_cols = {f"tp{i+1}": tps[i] for i in range(min(len(tps), 6))}
    # Pad missing TPs with None
    for i in range(len(tps), 6):
        tp_cols[f"tp{i+1}"] = None

    try:
        with conn.cursor() as cur:
            # Trade record
            cur.execute(
                """
                INSERT INTO trades (
                    bot_name, timeframe, symbol, direction,
                    entry, tp1, tp2, tp3, tp4, tp5, tp6,
                    sl, sl_initial, leverage, tp_count,
                    ml_confidence, status
                ) VALUES (
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,'OPEN'
                ) RETURNING id
                """,
                (
                    bot_name, tf, symbol, direction,
                    setup["entry"],
                    tp_cols["tp1"], tp_cols["tp2"], tp_cols["tp3"],
                    tp_cols["tp4"], tp_cols["tp5"], tp_cols["tp6"],
                    setup["sl"], setup["sl"], leverage, tp_count,
                    ml_confidence,
                ),
            )
            trade_id = cur.fetchone()[0]

            # Cornix message (text only, no image)
            cur.execute(
                "INSERT INTO telegram_outbox (channel_id,message) VALUES (%s,%s)",
                (channel_id, cornix),
            )
            # HTML caption + chart
            if chart_path:
                cur.execute(
                    "INSERT INTO telegram_outbox (channel_id,message,image_path) VALUES (%s,%s,%s)",
                    (channel_id, caption, chart_path),
                )
            else:
                cur.execute(
                    "INSERT INTO telegram_outbox (channel_id,message) VALUES (%s,%s)",
                    (channel_id, caption),
                )

            # Mark pattern event as confirmed + trade triggered
            cur.execute(
                """
                UPDATE pattern_events SET
                    state='CONFIRMED', trade_triggered=TRUE, updated_at=NOW()
                WHERE id=%s
                """,
                (event_id,),
            )

        conn.commit()
        logger.info(f"✅ {bot_name} trade #{trade_id} posted: {symbol} {direction} @ {setup['entry']:.4f}")

    except Exception as e:
        logger.error(f"Post trade error {symbol}: {e}")
        conn.rollback()


# ── Main poller ───────────────────────────────────────────────────────────────

def _get_current_candle(conn, symbol: str, tf: str) -> dict | None:
    """Returns the latest completed candle for a symbol/tf."""
    try:
        tbl = f"ohlcv_{tf}"
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT open, high, low, close, volume
                FROM {tbl}
                WHERE symbol=%s
                ORDER BY open_time DESC
                LIMIT 2
                """,
                (symbol,),
            )
            rows = cur.fetchall()
        if len(rows) < 2:
            return None
        # rows[0] = latest (may still be forming), rows[1] = confirmed
        r = rows[1]
        return {
            "open":   float(r[0]),
            "high":   float(r[1]),
            "low":    float(r[2]),
            "close":  float(r[3]),
            "volume": float(r[4]),
        }
    except Exception:
        return None


def _poll_once(conn) -> None:
    """Check all RETEST events and confirm/skip them."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, symbol, timeframe, pattern, direction,
                       retest_time, slope_high, intercept_high,
                       slope_low, intercept_low
                FROM pattern_events
                WHERE state = 'RETEST'
                  AND trade_triggered = FALSE
                ORDER BY retest_time ASC
                """
            )
            events = cur.fetchall()
    except Exception as e:
        logger.error(f"Poll error: {e}")
        return

    if not events:
        return

    now = datetime.datetime.now(datetime.timezone.utc)

    for (ev_id, symbol, tf, pattern, direction,
         retest_time, slope_h, int_h, slope_l, int_l) in events:

        # Check max age of retest
        max_age_h = RETEST_MAX_AGE_H.get(tf, 6)
        if retest_time and (now - retest_time.replace(tzinfo=datetime.timezone.utc)).total_seconds() > max_age_h * 3600:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE pattern_events SET state='EXPIRED', updated_at=NOW() WHERE id=%s",
                    (ev_id,),
                )
            conn.commit()
            logger.info(f"EXPIRED {symbol}/{tf} {pattern} — retest too old")
            continue

        candle = _get_current_candle(conn, symbol, tf)
        if not candle:
            continue

        is_bullish  = direction == "BULLISH"
        c_open      = candle["open"]
        c_close     = candle["close"]
        c_high      = candle["high"]
        c_low       = candle["low"]

        # Rough trendline value — use intercept as current level estimate
        # (exact index not known here, but intercept is close enough for confirmation)
        line = int_h if is_bullish else int_l

        body_pct = abs(c_close - c_open) / c_open if c_open > 0 else 0

        # Strong confirmation candle
        strong_bull = (c_close > c_open and body_pct >= CONFIRM_BODY_PCT
                       and c_close > line)
        strong_bear = (c_close < c_open and body_pct >= CONFIRM_BODY_PCT
                       and c_close < line)

        confirmed = (is_bullish and strong_bull) or (not is_bullish and strong_bear)

        if not confirmed:
            continue

        # Channel lookup per timeframe
        channel_id = BR_CHANNELS.get(tf, 0)
        if not channel_id:
            logger.warning(f"No channel for {tf} — skipping {symbol}")
            continue

        trade_dir = "LONG" if is_bullish else "SHORT"
        bot_name  = f"BR-{tf}"

        if _cooldown_active(conn, bot_name, symbol, trade_dir,
                             COOLDOWN_H.get(tf, 6)):
            logger.info(f"Cooldown active: {symbol} {bot_name} {trade_dir}")
            # Mark expired so we don't keep checking
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE pattern_events SET state='EXPIRED', updated_at=NOW() WHERE id=%s",
                    (ev_id,),
                )
            conn.commit()
            continue

        setup     = _calculate_targets(conn, symbol, trade_dir, c_close)
        chart     = generate_chart(symbol, minutes=240)

        _post_trade(
            conn, symbol, trade_dir, bot_name, tf,
            channel_id, setup, ev_id,
            ml_confidence=None,
            chart_path=chart,
        )
        _set_cooldown(conn, bot_name, symbol, trade_dir, COOLDOWN_H.get(tf, 6))


def main() -> None:
    logger.info("=" * 60)
    logger.info("Pattern Trader V4 (BR-1) — starting")
    logger.info("=" * 60)

    schema = verify_schema()
    if schema["missing"]:
        logger.error(f"Missing tables: {schema['missing']}")
        sys.exit(1)
    logger.info(f"Schema OK — {len(schema['ok'])} tables verified.")

    shutdown = ShutdownHandler("PAT_TRADER")
    logger.info(f"Polling pattern_events every {POLL_INTERVAL_S}s...")

    while not shutdown.is_set():
        try:
            with db_connection() as conn:
                _poll_once(conn)
        except Exception as e:
            logger.error(f"Main loop error: {e}")

        shutdown.sleep(POLL_INTERVAL_S)

    logger.info("Pattern Trader stopped.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Pattern Trader stopped (Ctrl+C).")
