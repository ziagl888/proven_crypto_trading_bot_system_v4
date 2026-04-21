# 51_trendbreaker_trader.py
# Trendbreaker Trader V4 (ATB-1)
#
# Polls trendline_events for WAITING_RETEST state.
# When price returns to the trendline (retest), waits for confirmation,
# runs ML model, and generates trade if score >= threshold.
#
# Flow:
#   1. 41_trendbreaker_detector.py writes WAITING_RETEST events (BREAK_UP/DOWN)
#      and DETECTED events (BOUNCE_UP/DOWN)
#   2. This bot polls every 60s for active events
#   3. For BREAK events: detects retest (price returns to trendline ± RETEST_BAND)
#      + confirmation candle → ML → trade
#   4. For BOUNCE events: confirmation candle → ML → trade (no retest needed)
#
# ML models: long_trend_prediction_model.joblib / short_trend_prediction_model.joblib
# If models not found: bot runs without ML (all confirmed breaks/bounces get a trade)
#
# Bot name: ATB-1
# Trade channel: ATB1_CHANNEL_ID
# Info channel:  ATB1_INFO_CHANNEL_ID

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
    format="%(asctime)s - ATB_TRADER - %(levelname)s - %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            os.path.join(_LOG_DIR, "trendbreaker_trader.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

from core.config import ATB1_CHANNEL_ID, ATB1_INFO_CHANNEL_ID
from core.database import db_connection, get_db_connection
from core.schema import verify_schema
from core.shutdown import ShutdownHandler
from core.charting import generate_chart

# ── Constants ─────────────────────────────────────────────────────────────────

POLL_INTERVAL_S  = 60
BOT_NAME         = "ATB-1"
LEVERAGE_DEFAULT = 20
COOLDOWN_H       = 4

# ML model paths
MODEL_LONG_PATH  = os.path.join(_SCRIPT_DIR, "long_trend_prediction_model.joblib")
MODEL_SHORT_PATH = os.path.join(_SCRIPT_DIR, "short_trend_prediction_model.joblib")

# ML thresholds
ML_THRESH_LONG   = 0.80
ML_THRESH_SHORT  = 0.75

# Retest detection: price must return within this band of trendline
RETEST_BAND_PCT  = 0.015    # 1.5% — price touches trendline ± 1.5%

# Confirmation candle
CONFIRM_BODY_PCT = 0.005    # 0.5%

# Event expiry
BREAK_EXPIRY_H   = 72
BOUNCE_EXPIRY_H  = 24

# ── Model loading ─────────────────────────────────────────────────────────────

_MODELS: dict[str, object] = {"LONG": None, "SHORT": None}


def _load_models() -> None:
    try:
        import joblib
        if os.path.exists(MODEL_LONG_PATH):
            _MODELS["LONG"] = joblib.load(MODEL_LONG_PATH)
            logger.info("✅ LONG model loaded")
        else:
            logger.warning(f"LONG model not found: {MODEL_LONG_PATH}")

        if os.path.exists(MODEL_SHORT_PATH):
            _MODELS["SHORT"] = joblib.load(MODEL_SHORT_PATH)
            logger.info("✅ SHORT model loaded")
        else:
            logger.warning(f"SHORT model not found: {MODEL_SHORT_PATH}")
    except ImportError:
        logger.warning("joblib not available — running without ML models")
    except Exception as e:
        logger.error(f"Model load error: {e}")


# ── ML prediction ─────────────────────────────────────────────────────────────

def _get_ml_score(df: pd.DataFrame, direction: str, slope: float,
                   entry: float) -> float | None:
    """
    Calculates ML confidence score for a trendline break/bounce.
    Uses same features as V3 ATB1 model.
    Returns float 0-1 or None if models unavailable.
    """
    model = _MODELS.get(direction)
    if model is None:
        return None

    try:
        # Avoid pandas_ta dependency — calculate features manually
        c  = df["close"].values.astype(float)
        h  = df["high"].values.astype(float)
        l  = df["low"].values.astype(float)
        v  = df["volume"].values.astype(float)
        n  = len(df)

        if n < 30:
            return None

        # EMA helper
        def _ema(arr, span):
            alpha = 2.0 / (span + 1)
            out   = np.zeros(len(arr))
            out[0] = arr[0]
            for i in range(1, len(arr)):
                out[i] = alpha * arr[i] + (1 - alpha) * out[i-1]
            return out

        ema9   = _ema(c, 9)
        ema21  = _ema(c, 21)
        ema50  = _ema(c, 50)
        ema200 = _ema(c, 200)

        last_c   = c[-1]
        last_v   = v[-1]
        vol_avg  = float(np.mean(v[-21:-1])) if n >= 21 else float(np.mean(v))
        vol_ratio = last_v / vol_avg if vol_avg > 0 else 0.0

        # RSI 14
        delta = np.diff(c)
        gain  = np.where(delta > 0, delta, 0.0)
        loss  = np.where(delta < 0, -delta, 0.0)
        avg_g = np.mean(gain[-14:]) if len(gain) >= 14 else 0.0
        avg_l = np.mean(loss[-14:]) if len(loss) >= 14 else 1e-9
        rsi   = 100.0 - 100.0 / (1.0 + avg_g / avg_l)

        # ATR 14
        hl    = h - l
        hc    = np.abs(h[1:] - c[:-1])
        lc    = np.abs(l[1:] - c[:-1])
        tr    = np.maximum(hl[1:], np.maximum(hc, lc))
        atr   = float(np.mean(tr[-14:])) if len(tr) >= 14 else last_c * 0.02
        atr_pct = atr / last_c if last_c > 0 else 0.0

        dist_ema9_pct   = (last_c - ema9[-1])  / ema9[-1]  if ema9[-1]  > 0 else 0.0
        dist_ema200_pct = (last_c - ema200[-1]) / ema200[-1] if ema200[-1] > 0 else 0.0
        dist_ema9_21    = (ema9[-1] - ema21[-1]) / ema21[-1] if ema21[-1] > 0 else 0.0

        # KAMA-9 (simplified)
        kama = c[-1]   # fallback — full KAMA needs more history

        # Slope as % per day (24 candles for 1h data)
        slope_pct_day = (slope * 24) / last_c if last_c > 0 else 0.0

        # Bollinger Bands (20)
        if n >= 20:
            bb_mid  = float(np.mean(c[-20:]))
            bb_std  = float(np.std(c[-20:]))
            bb_upper = bb_mid + 2 * bb_std
            bb_lower = bb_mid - 2 * bb_std
            bb_pos   = (last_c - bb_lower) / (bb_upper - bb_lower) if (bb_upper - bb_lower) > 0 else 0.5
            dist_bb_lower = (last_c - bb_lower) / last_c
            dist_bb_upper = (last_c - bb_upper) / last_c
        else:
            bb_pos = 0.5; dist_bb_lower = 0.0; dist_bb_upper = 0.0

        # Donchian (20)
        if n >= 20:
            dc_upper = float(np.max(h[-20:]))
            dc_lower = float(np.min(l[-20:]))
            dc_pos   = (last_c - dc_lower) / (dc_upper - dc_lower) if (dc_upper - dc_lower) > 0 else 0.5
            dist_dc_lower = (last_c - dc_lower) / last_c
            dist_dc_upper = (last_c - dc_upper) / last_c
        else:
            dc_pos = 0.5; dist_dc_lower = 0.0; dist_dc_upper = 0.0

        hour_of_day = datetime.datetime.now(datetime.timezone.utc).hour

        X = pd.DataFrame([{
            "vol_ratio":               vol_ratio,
            "rsi":                     rsi,
            "atr_pct":                 atr_pct,
            "dist_ema200":             dist_ema200_pct,
            "slope_trend":             slope_pct_day,
            "hour_of_day":             hour_of_day,
            "dist_close_ema9_pct":     dist_ema9_pct,
            "dist_ema9_ema21_pct":     dist_ema9_21,
            "dist_close_kama9_pct":    (last_c - kama) / kama if kama > 0 else 0.0,
            "MACD_Line":               ema9[-1] - ema21[-1],
            "MACD_Signal":             ema9[-1] - ema21[-1],  # simplified
            "TSI_Line":                0.0,
            "TSI_Signal":              0.0,
            "dist_close_bb_lower_pct": dist_bb_lower,
            "dist_close_bb_upper_pct": dist_bb_upper,
            "bb_position_relative":    bb_pos,
            "dist_close_dc_lower_pct": dist_dc_lower,
            "dist_close_dc_upper_pct": dist_dc_upper,
            "dc_position_relative":    dc_pos,
        }]).astype(float)

        X = X.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        score = float(model.predict_proba(X)[0][1])
        return score

    except Exception as e:
        logger.debug(f"ML score error: {e}")
        return None


# ── Trade helpers ─────────────────────────────────────────────────────────────

def _calculate_targets(conn, symbol: str, direction: str,
                        entry: float) -> dict:
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
        for col in ("open","high","low","close","volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna().reset_index(drop=True)

        is_long = direction == "LONG"
        highs   = df["high"].values
        lows    = df["low"].values

        # ATR
        hl  = df["high"] - df["low"]
        hc  = (df["high"] - df["close"].shift()).abs()
        lc  = (df["low"]  - df["close"].shift()).abs()
        tr  = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        atr = float(tr.rolling(14).mean().iloc[-1])
        if not np.isfinite(atr) or atr == 0:
            atr = entry * 0.02
        atr = min(atr, entry * 0.04)

        res_idx = scipy.signal.argrelextrema(highs, np.greater, order=20)[0]
        sup_idx = scipy.signal.argrelextrema(lows,  np.less,    order=20)[0]
        resistances = [float(highs[i]) for i in res_idx]
        supports    = [float(lows[i])  for i in sup_idx]

        sh = float(np.max(highs[-300:]))
        sl = float(np.min(lows[-300:]))
        rng = sh - sl
        fibs = []
        if rng > 0:
            for x in [0.236, 0.382, 0.5, 0.618, 0.786]:
                fibs.append(sh - rng * x)
            for x in [1.272, 1.618, 2.0]:
                fibs.append(sl + rng * x)

        all_levels = sorted(set(supports + resistances + fibs))

        min_sl_dist = atr * 3.0
        max_sl_pct  = 0.15

        if is_long:
            sl_cands = [x for x in all_levels
                        if x < entry - min_sl_dist
                        and x >= entry * (1 - max_sl_pct)]
            sl_price = max(sl_cands) if sl_cands else max(
                entry - min_sl_dist, entry * (1 - max_sl_pct))
            tp_cands = sorted([x for x in all_levels
                                if x >= entry + atr * 2
                                and x <= entry * 3.0])
        else:
            sl_cands = [x for x in all_levels
                        if x > entry + min_sl_dist
                        and x <= entry * (1 + max_sl_pct)]
            sl_price = min(sl_cands) if sl_cands else min(
                entry + min_sl_dist, entry * (1 + max_sl_pct))
            tp_cands = sorted([x for x in all_levels
                                if x <= entry - atr * 2
                                and x >= entry * 0.1], reverse=True)

        tps  = []
        last = entry
        for t in tp_cands:
            if abs(t - last) >= atr:
                tps.append(round(t, 8))
                last = t
            if len(tps) >= 6:
                break

        if not tps:
            tps = [round(entry * 1.05 if is_long else entry * 0.95, 8)]

        return {"entry": round(entry, 8), "sl": round(sl_price, 8), "tps": tps}

    except Exception as e:
        logger.warning(f"Target calc fallback {symbol}: {e}")
        is_long = direction == "LONG"
        return {
            "entry": round(entry, 8),
            "sl":    round(entry * 0.93 if is_long else entry * 1.07, 8),
            "tps":   [round(entry * 1.05 if is_long else entry * 0.95, 8)],
        }


def _build_cornix(symbol: str, direction: str, leverage: int,
                   setup: dict, ml_score: float | None) -> str:
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
        f"🧠 Trade by {BOT_NAME} V4"
        + (f" | ML: {ml_score:.1%}" if ml_score is not None else ""),
    ]
    return "\n".join(lines)


def _cooldown_active(conn, symbol: str, direction: str) -> bool:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM trade_cooldowns
                WHERE bot_name=%s AND symbol=%s AND direction=%s
                  AND expires_at > NOW()
                LIMIT 1
                """,
                (BOT_NAME, symbol, direction),
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def _set_cooldown(conn, symbol: str, direction: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trade_cooldowns (bot_name,symbol,direction,expires_at)
                VALUES (%s,%s,%s, NOW() + INTERVAL '%s hours')
                ON CONFLICT (bot_name,symbol,direction)
                DO UPDATE SET expires_at = NOW() + INTERVAL '%s hours'
                """,
                (BOT_NAME, symbol, direction, COOLDOWN_H, COOLDOWN_H),
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"Cooldown set error: {e}")


def _post_trade(conn, symbol: str, direction: str,
                setup: dict, event_id: int,
                ml_score: float | None,
                event_type: str,
                chart_path: str | None) -> None:
    leverage = LEVERAGE_DEFAULT
    tps      = setup["tps"]
    tp_cols  = {f"tp{i+1}": tps[i] if i < len(tps) else None for i in range(6)}

    cornix  = _build_cornix(symbol, direction, leverage, setup, ml_score)
    emoji   = "🚀" if direction == "LONG" else "💥"
    caption = (
        f"<pre><b>{emoji} {BOT_NAME} {direction}</b>\n"
        f"<b>{symbol.replace('USDT','')}/USDT</b>\n"
        f"→ Event: {event_type}\n"
        f"→ Entry: <code>${setup['entry']:,.4f}</code>\n"
        f"→ SL:    <code>${setup['sl']:,.4f}</code>\n"
        f"→ TP1:   <code>${tps[0]:,.4f}</code>\n"
        + (f"→ ML: <b>{ml_score:.1%}</b>\n" if ml_score is not None else "")
        + "</pre>"
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trades (
                    bot_name, symbol, direction,
                    entry, tp1, tp2, tp3, tp4, tp5, tp6,
                    sl, sl_initial, leverage, tp_count,
                    ml_confidence, status
                ) VALUES (
                    %s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,'OPEN'
                ) RETURNING id
                """,
                (
                    BOT_NAME, symbol, direction,
                    setup["entry"],
                    tp_cols["tp1"], tp_cols["tp2"], tp_cols["tp3"],
                    tp_cols["tp4"], tp_cols["tp5"], tp_cols["tp6"],
                    setup["sl"], setup["sl"], leverage, len(tps),
                    ml_score,
                ),
            )
            trade_id = cur.fetchone()[0]

            cur.execute(
                "INSERT INTO telegram_outbox (channel_id,message) VALUES (%s,%s)",
                (ATB1_CHANNEL_ID, cornix),
            )
            if chart_path:
                cur.execute(
                    "INSERT INTO telegram_outbox (channel_id,message,image_path) VALUES (%s,%s,%s)",
                    (ATB1_CHANNEL_ID, caption, chart_path),
                )
            else:
                cur.execute(
                    "INSERT INTO telegram_outbox (channel_id,message) VALUES (%s,%s)",
                    (ATB1_CHANNEL_ID, caption),
                )

            # Mark event confirmed
            cur.execute(
                """
                UPDATE trendline_events SET
                    state='CONFIRMED', ml_score=%s,
                    trade_triggered=TRUE, updated_at=NOW()
                WHERE id=%s
                """,
                (ml_score, event_id),
            )

        conn.commit()
        logger.info(
            f"✅ {BOT_NAME} trade #{trade_id}: {symbol} {direction} "
            f"@ {setup['entry']:.4f}"
            + (f" ML={ml_score:.1%}" if ml_score else "")
        )
    except Exception as e:
        logger.error(f"Post trade error {symbol}: {e}")
        conn.rollback()


# ── Main poller ───────────────────────────────────────────────────────────────

def _load_recent_1h(conn, symbol: str) -> pd.DataFrame:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT open_time, open, high, low, close, volume
                FROM ohlcv_1h
                WHERE symbol=%s
                  AND open_time >= NOW() - INTERVAL '95 days'
                ORDER BY open_time ASC
                """,
                (symbol,),
            )
            rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["open_time","open","high","low","close","volume"])
        for col in ("open","high","low","close","volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.dropna().reset_index(drop=True)
    except Exception:
        conn.rollback()
        return pd.DataFrame()


def _poll_once(conn) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, symbol, trend_direction, event_type, state,
                       slope, intercept, trend_value,
                       break_price, break_time, distance_pct
                FROM trendline_events
                WHERE state IN ('DETECTED','WAITING_RETEST','RETEST')
                  AND trade_triggered = FALSE
                ORDER BY created_at ASC
                """
            )
            events = cur.fetchall()
    except Exception as e:
        logger.error(f"Poll error: {e}")
        return

    if not events:
        return

    for (ev_id, symbol, trend_dir, event_type, state,
         slope, intercept, trend_value,
         break_price, break_time, dist_pct) in events:

        # Check expiry
        if break_time:
            bt = break_time.replace(tzinfo=datetime.timezone.utc)
            max_age = BREAK_EXPIRY_H if "BREAK" in event_type else BOUNCE_EXPIRY_H
            if (now - bt).total_seconds() > max_age * 3600:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE trendline_events SET state='EXPIRED', updated_at=NOW() WHERE id=%s",
                        (ev_id,),
                    )
                conn.commit()
                continue

        df = _load_recent_1h(conn, symbol)
        if len(df) < 50:
            continue

        last_idx   = len(df) - 1
        trend_now  = float(slope) * last_idx + float(intercept)
        last_close = float(df["close"].iloc[-1])
        prev_close = float(df["close"].iloc[-2])
        last_open  = float(df["open"].iloc[-1])
        last_high  = float(df["high"].iloc[-1])
        last_low   = float(df["low"].iloc[-1])

        dist_now   = (last_close - trend_now) / last_close
        body_pct   = abs(last_close - last_open) / last_open if last_open > 0 else 0

        is_up    = event_type in ("BREAK_UP", "BOUNCE_UP")
        direction = "LONG" if is_up else "SHORT"

        confirmed = False

        if "BREAK" in event_type:
            # For breaks: wait for retest (price returns near trendline)
            # then confirm with strong candle back on break side
            near_line = abs(dist_now) <= RETEST_BAND_PCT

            if near_line:
                # RETEST ALERT — fires ONCE only by transitioning state.
                # WAITING_RETEST → RETEST prevents duplicate alerts on
                # subsequent hourly scans while price stays near the line.
                if state == "WAITING_RETEST":
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE trendline_events SET
                                state='RETEST',
                                retest_price=%s, retest_time=NOW(),
                                updated_at=NOW()
                            WHERE id=%s
                            """,
                            (last_close, ev_id),
                        )
                    conn.commit()
                    state = "RETEST"  # update local var for rest of this iteration

                    coin = symbol.replace("USDT","")
                    msg  = (
                        f"<pre><b>🔄 TRENDLINE RETEST</b>\n"
                        f"<b>{coin}/USDT</b>\n"
                        f"→ Event: {event_type}\n"
                        f"→ Retest @ <code>${last_close:,.4f}</code>\n"
                        f"→ Trendline: <code>${trend_now:,.4f}</code>\n"
                        f"⏳ Waiting for confirmation...</pre>"
                    )
                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                "INSERT INTO telegram_outbox (channel_id,message) VALUES (%s,%s)",
                                (ATB1_INFO_CHANNEL_ID, msg),
                            )
                        conn.commit()
                    except Exception:
                        pass

                # Strong confirmation candle back on break side
                strong_bull = (is_up and last_close > last_open
                               and body_pct >= CONFIRM_BODY_PCT
                               and dist_now > 0)
                strong_bear = (not is_up and last_close < last_open
                               and body_pct >= CONFIRM_BODY_PCT
                               and dist_now < 0)
                confirmed = strong_bull or strong_bear

        else:
            # BOUNCE: confirmation candle on same side
            strong_bull = (is_up and last_close > last_open
                           and body_pct >= CONFIRM_BODY_PCT
                           and dist_now > 0)
            strong_bear = (not is_up and last_close < last_open
                           and body_pct >= CONFIRM_BODY_PCT
                           and dist_now < 0)
            confirmed = strong_bull or strong_bear

        if not confirmed:
            continue

        if _cooldown_active(conn, symbol, direction):
            logger.info(f"Cooldown active: {symbol} {BOT_NAME} {direction}")
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE trendline_events SET state='EXPIRED', updated_at=NOW() WHERE id=%s",
                    (ev_id,),
                )
            conn.commit()
            continue

        # ML score
        ml_score  = _get_ml_score(df, direction, float(slope), last_close)
        threshold = ML_THRESH_LONG if direction == "LONG" else ML_THRESH_SHORT

        if ml_score is not None and ml_score < threshold:
            logger.info(
                f"ML below threshold: {symbol} {direction} "
                f"score={ml_score:.2f} thresh={threshold:.2f} — skipping"
            )
            # Keep event active in case score improves next candle
            continue

        setup     = _calculate_targets(conn, symbol, direction, last_close)
        chart     = generate_chart(symbol, minutes=240)

        _post_trade(conn, symbol, direction, setup, ev_id,
                    ml_score, event_type, chart)
        _set_cooldown(conn, symbol, direction)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Trendbreaker Trader V4 (ATB-1) — starting")
    logger.info("=" * 60)

    schema = verify_schema()
    if schema["missing"]:
        logger.error(f"Missing tables: {schema['missing']}")
        sys.exit(1)
    logger.info(f"Schema OK — {len(schema['ok'])} tables verified.")

    _load_models()

    shutdown = ShutdownHandler("ATB_TRADER")
    logger.info(f"Polling trendline_events every {POLL_INTERVAL_S}s...")

    while not shutdown.is_set():
        try:
            with db_connection() as conn:
                _poll_once(conn)
        except Exception as e:
            logger.error(f"Main loop error: {e}")

        shutdown.sleep(POLL_INTERVAL_S)

    logger.info("Trendbreaker Trader stopped.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Trendbreaker Trader stopped (Ctrl+C).")

