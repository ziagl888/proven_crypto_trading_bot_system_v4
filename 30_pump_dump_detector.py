# 30_pump_dump_detector.py
# Market Movement & Volume Explosion Detector — V4
#
# Detects and posts to Telegram:
#   1. Extreme price moves  (>2% / 1min, >3% / 2min, >5% / 5min, >8% / 10min)
#   2. Volume explosions    (>5x / >10x / >20x avg volume + price confirmation)
#   3. Round level breaks   (BTC/ETH/BNB/SOL crossing key price levels)
#
# Uses Binance /fapi/v1/ticker/24hr REST endpoint every 10 seconds.
# Writes alerts to telegram_outbox → delivered by 20_telegram_bot.py
# Writes all events to pump_dump_events table for analytics.
#
# Module tag: PD-1 (Market alerts, no ML, no trade signals)
# EPD-1 (ML-based early detection) is a separate bot — not here.
#
# Cooldowns are stored in trade_cooldowns table (persistent across restarts).
# 10s candle history is kept in RAM (deque, max 1440 entries = 4 hours).

from __future__ import annotations

import datetime
import json
import logging
import logging.handlers
import os
import sys
import time
from collections import deque

import requests
from dotenv import load_dotenv

load_dotenv()

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR     = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - PD1 - %(levelname)s - %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            os.path.join(_LOG_DIR, "pump_dump_detector.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

from core.config import (
    PUMP_DUMP_MARKET_CHANNEL_ID,
    SENTIMENT_CHANNEL_ID,
)
from core.database import db_connection
from core.schema import verify_schema
from core.charting import generate_chart
from core.shutdown import ShutdownHandler

# ── Constants ─────────────────────────────────────────────────────────────────

MODULE_TAG   = "PD-1"
POLL_EVERY   = 10      # seconds between REST calls
MAX_HISTORY  = 1440    # 10s buckets = 4 hours of history per symbol

# Price move thresholds: (lookback_seconds, min_pct_change, label)
PRICE_THRESHOLDS = [
    (60,   2.0,  "1min"),
    (120,  3.0,  "2min"),
    (300,  5.0,  "5min"),
    (600,  8.0,  "10min"),
]

# Volume thresholds: (multiplier, label, extended_cooldown)
VOLUME_THRESHOLDS = [
    (20.0, "🔥 MEGA",    True),
    (10.0, "⚡ MASSIVE", True),
    ( 5.0, "📈 STRONG",  False),
]
VOLUME_MIN_PRICE_PCT = 1.5   # min price move % to confirm volume signal
VOLUME_BASELINE_MIN  = 180   # need at least 30 min of history for reliable baseline

# Cooldowns
COOLDOWN_NORMAL_S  = 300    # 5 min between alerts for same symbol
COOLDOWN_EXTEND_S  = 900    # 15 min for big moves (>10% or mega volume)

# Round level config: symbols and their step sizes
ROUND_LEVEL_CONFIG = {
    "BTCUSDT":    {"step":  500, "decimals": 0},
    "ETHUSDT":    {"step":  100, "decimals": 0},
    "BNBUSDT":    {"step":   50, "decimals": 0},
    "SOLUSDT":    {"step":   10, "decimals": 1},
    "XRPUSDT":    {"step":  0.1, "decimals": 3},
    "BTCDOMUSDT": {"step":  100, "decimals": 0},
}
ROUND_LEVEL_COOLDOWN_S = 180   # 3 min between round level alerts per symbol

# ── In-memory state ───────────────────────────────────────────────────────────
# {symbol: deque of {"t": iso_str, "p": float, "v": float, "v_valid": bool, "cum_vol": float}}
_CANDLES: dict[str, deque] = {}

# Per-symbol volume baseline samples (only valid volume deltas)
# {symbol: deque of float}
_VOL_SAMPLES: dict[str, deque] = {}

# Per-symbol last alert timestamp (from DB cooldowns, cached in RAM)
# {symbol: datetime}
_LAST_ALERT: dict[str, datetime.datetime] = {}

# Round level break state {symbol: {"last_level": float, "last_time": datetime}}
_ROUND_STATE: dict[str, dict] = {}


# ── DB helpers ────────────────────────────────────────────────────────────────

def _send_outbox(channel_id: int, message: str, image_path: str | None = None) -> None:
    """Inserts a message into telegram_outbox."""
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                if image_path:
                    cur.execute(
                        "INSERT INTO telegram_outbox (channel_id, message, image_path) VALUES (%s, %s, %s)",
                        (channel_id, message, image_path),
                    )
                else:
                    cur.execute(
                        "INSERT INTO telegram_outbox (channel_id, message) VALUES (%s, %s)",
                        (channel_id, message),
                    )
            conn.commit()
    except Exception as e:
        logger.error(f"Outbox insert error: {e}")


def _log_event(symbol: str, event_type: str, price: float,
               price_change_pct: float, volume_ratio: float) -> None:
    """Logs a detected event to pump_dump_events for analytics."""
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pump_dump_events
                        (symbol, detected_at, event_type, price,
                         price_change_pct, volume_ratio, module)
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s)
                    """,
                    (symbol, event_type, price, price_change_pct,
                     volume_ratio, MODULE_TAG),
                )
            conn.commit()
    except Exception as e:
        logger.debug(f"Event log error (non-critical): {e}")


def _get_cooldown(symbol: str) -> datetime.datetime:
    """Returns last alert time for symbol from DB. Falls back to epoch."""
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT last_posted_at FROM trade_cooldowns
                    WHERE module = %s AND coin = %s AND direction = 'ALERT'
                    """,
                    (MODULE_TAG, symbol),
                )
                row = cur.fetchone()
                return row[0] if row else epoch
    except Exception:
        return epoch


def _set_cooldown(symbol: str) -> None:
    """Upserts cooldown for symbol in DB and cache."""
    now = datetime.datetime.now(datetime.timezone.utc)
    _LAST_ALERT[symbol] = now
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO trade_cooldowns (module, coin, direction, last_posted_at)
                    VALUES (%s, %s, 'ALERT', NOW())
                    ON CONFLICT (module, coin, direction) DO UPDATE
                        SET last_posted_at = NOW()
                    """,
                    (MODULE_TAG, symbol),
                )
            conn.commit()
    except Exception as e:
        logger.error(f"Cooldown set error: {e}")


def _ensure_events_table() -> None:
    """Creates pump_dump_events if not exists."""
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
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
                """)
            conn.commit()
    except Exception as e:
        logger.error(f"pump_dump_events table create error: {e}")


# ── Bucket helpers ────────────────────────────────────────────────────────────

def _bucket_ts(entry: dict) -> datetime.datetime | None:
    try:
        return datetime.datetime.fromisoformat(entry["t"].replace("Z", "+00:00"))
    except Exception:
        return None


def _find_bucket_before(data: list, now: datetime.datetime,
                        seconds_ago: int, tolerance: int = 20) -> dict | None:
    """
    Finds the bucket closest to `seconds_ago` seconds in the past.
    Time-based (not index-based) — robust after restarts with stale history.
    """
    target = now - datetime.timedelta(seconds=seconds_ago)
    tol    = datetime.timedelta(seconds=tolerance)
    for entry in reversed(data):
        ts = _bucket_ts(entry)
        if ts is None:
            continue
        if abs(ts - target) <= tol:
            return entry
        if ts < target - tol:
            return None
    return None


def _find_bucket_range(data: list, now: datetime.datetime,
                       seconds_ago: int, tolerance: int = 20) -> list:
    """Returns all buckets within the last `seconds_ago` seconds."""
    cutoff = now - datetime.timedelta(seconds=seconds_ago + tolerance)
    result = []
    for entry in reversed(data):
        ts = _bucket_ts(entry)
        if ts is None:
            continue
        if ts < cutoff:
            break
        result.append(entry)
    return list(reversed(result))


# ── Detection logic ───────────────────────────────────────────────────────────

def _check_round_levels(symbol: str, current_price: float,
                        prev_price: float) -> None:
    """Detects and alerts on round level breaks for configured symbols."""
    if symbol not in ROUND_LEVEL_CONFIG:
        return

    cfg  = ROUND_LEVEL_CONFIG[symbol]
    step = cfg["step"]
    decs = cfg["decimals"]

    prev_bucket = int(prev_price / step)
    curr_bucket = int(current_price / step)
    if prev_bucket == curr_bucket:
        return

    direction     = "upwards" if current_price > prev_price else "downwards"
    crossed_level = curr_bucket * step if direction == "upwards" else prev_bucket * step

    # Cooldown check — suppress if ANY level was crossed recently for this symbol
    # This prevents flip-flopping alerts when price hovers around a round level
    state = _ROUND_STATE.get(symbol, {})
    now   = datetime.datetime.now(datetime.timezone.utc)
    last  = state.get("last_time", datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc))
    if (now - last).total_seconds() < ROUND_LEVEL_COOLDOWN_S:
        return

    emoji = "🚀" if direction == "upwards" else "💥"
    msg = (
        f"<pre><b>{emoji} ROUND LEVEL BREAK</b>\n"
        f"<b>{symbol.replace('USDT','')}/USDT</b> breaks "
        f"<b>{crossed_level:,.{decs}f}</b> <b>{direction.upper()}</b>\n"
        f"<b>→ Price:</b> <code>${current_price:,.{decs}f}</code>\n"
        f"<b>→ Time:</b> {now.strftime('%H:%M:%S')} UTC\n"
        f"</pre>"
    )

    chart_path = generate_chart(symbol, minutes=240)
    _send_outbox(PUMP_DUMP_MARKET_CHANNEL_ID, msg, chart_path)
    logger.info(f"ROUND LEVEL: {symbol} crossed {crossed_level} {direction}")

    _ROUND_STATE[symbol] = {"last_level": crossed_level, "last_time": now}


def _check_price_moves(symbol: str, data: list, now: datetime.datetime,
                       current_price: float) -> bool:
    """
    Checks for extreme price moves across multiple timeframes.
    Returns True if an alert was sent.

    CONTINUITY CHECK: Only alerts if the price data in the lookback window
    is continuous (no gaps > 30s). This prevents false alerts after restarts
    or reconnects where the RAM buffer has stale data mixed with fresh data.
    """
    for seconds_back, min_pct, t_label in PRICE_THRESHOLDS:
        ref = _find_bucket_before(data, now, seconds_back)
        if ref is None:
            continue

        ref_price = float(ref["p"])
        if ref_price <= 0:
            continue

        # Continuity check: verify no gaps in the lookback window
        # Get all buckets in the window and check for gaps > 30s
        window_buckets = _find_bucket_range(data, now, seconds_back + 30)
        if len(window_buckets) < 2:
            continue
        has_gap = False
        for i in range(1, len(window_buckets)):
            t_prev = _bucket_ts(window_buckets[i-1])
            t_curr = _bucket_ts(window_buckets[i])
            if t_prev and t_curr:
                gap = (t_curr - t_prev).total_seconds()
                if gap > 30:  # more than 3× normal poll interval
                    has_gap = True
                    break
        if has_gap:
            continue

        chg_pct = (current_price / ref_price - 1) * 100
        if abs(chg_pct) < min_pct:
            continue

        direction = "PUMP" if chg_pct > 0 else "DUMP"
        emoji     = "🚀" if chg_pct > 0 else "💥"

        # Check 10min trend for dead-cat / dip context
        chg_10m = None
        ref_10m = _find_bucket_before(data, now, 600)
        if ref_10m:
            p10 = float(ref_10m["p"])
            if p10 > 0:
                chg_10m = (current_price / p10 - 1) * 100

        dead_cat = (chg_10m is not None and
                    abs(chg_10m) >= 3.0 and
                    (chg_10m > 0) != (chg_pct > 0))

        extra = ""
        if chg_10m is not None:
            extra += f"\n<b>→ 10m trend: {chg_10m:+.2f}%</b>"
        if dead_cat:
            extra += (
                "\n<b>⚠ DEAD CAT BOUNCE (bounce in downtrend)</b>"
                if chg_pct > 0 else
                "\n<b>⚠ DIP IN UPTREND (pullback in uptrend)</b>"
            )

        msg = (
            f"<pre>"
            f"<b>{emoji} {direction} DETECTED</b>\n"
            f"<b>{symbol.replace('USDT','')}/USDT</b>\n"
            f"<b>→ {chg_pct:+.2f}% in {t_label}</b>\n"
            f"<b>→ Price: <code>${current_price:,.8f}</code></b>"
            f"{extra}\n"
            f"</pre>"
        )

        chart_path = generate_chart(symbol, minutes=240)
        _send_outbox(PUMP_DUMP_MARKET_CHANNEL_ID, msg, chart_path)

        extend = abs(chg_pct) >= 10.0
        _log_event(symbol, f"PRICE_{direction}_{t_label}", current_price, chg_pct, 0.0)
        logger.info(f"PRICE ALERT: {symbol} {chg_pct:+.2f}% in {t_label}")
        return True  # one alert per cycle per symbol

    return False


def _check_volume_explosion(symbol: str, data: list, now: datetime.datetime,
                             current_price: float, vol_samples: deque) -> bool:
    """
    Detects abnormal volume spikes with price confirmation.
    Returns True if an alert was sent.
    """
    if len(vol_samples) < VOLUME_BASELINE_MIN:
        return False

    # 3-minute volume sum (18 × 10s buckets)
    recent = _find_bucket_range(data, now, 180)
    if len(recent) < 6:
        return False

    recent_valid = [float(e["v"]) for e in recent if e.get("v_valid", True)]
    if not recent_valid:
        return False

    recent_vol_sum = sum(recent_valid)
    baseline_avg   = sum(list(vol_samples)[-360:]) / min(len(vol_samples), 360)
    if baseline_avg <= 0:
        return False

    # Normalize: compare 3min sum vs expected 3min (18 × avg_10s)
    vol_ratio = recent_vol_sum / (baseline_avg * len(recent_valid))

    # Price move over same window (must confirm volume direction)
    p_start = float(recent[0]["p"])
    p_chg   = (current_price / p_start - 1) * 100 if p_start > 0 else 0

    if abs(p_chg) < VOLUME_MIN_PRICE_PCT:
        return False

    # Find the matching threshold
    matched_label  = None
    matched_extend = False
    for threshold, label, extend in VOLUME_THRESHOLDS:
        if vol_ratio >= threshold:
            matched_label  = label
            matched_extend = extend
            break

    if matched_label is None:
        return False

    pressure = "BUY PRESSURE" if p_chg > 0 else "SELL PRESSURE"
    emoji    = "🟢" if p_chg > 0 else "🔴"

    msg = (
        f"<pre>"
        f"<b>{matched_label} VOLUME EXPLOSION</b>\n"
        f"<b>{symbol.replace('USDT','')}/USDT</b>\n"
        f"<b>→ {vol_ratio:.1f}× avg volume (3 min)</b>\n"
        f"<b>→ {pressure} {emoji} {p_chg:+.2f}%</b>\n"
        f"<b>→ Price: <code>${current_price:,.8f}</code></b>\n"
        f"<b>→ Time: {now.strftime('%H:%M')} UTC</b>\n"
        f"</pre>"
    )

    chart_path = generate_chart(symbol, minutes=240)
    _send_outbox(PUMP_DUMP_MARKET_CHANNEL_ID, msg, chart_path)
    _log_event(symbol, f"VOLUME_{matched_label.split()[0]}", current_price, p_chg, vol_ratio)
    logger.info(f"VOLUME ALERT: {symbol} {vol_ratio:.1f}x avg (price {p_chg:+.2f}%)")
    return True


def _process_symbol(symbol: str, price: float, vol_delta: float,
                    vol_valid: bool, cum_vol: float) -> None:
    """
    Main per-symbol processing:
    1. Update candle history
    2. Check round levels
    3. Check price moves + volume explosion (with cooldown)
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    ts  = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Init buffers
    if symbol not in _CANDLES:
        _CANDLES[symbol]     = deque(maxlen=MAX_HISTORY)
        _VOL_SAMPLES[symbol] = deque(maxlen=720)  # 2 hours of valid 10s samples

    data = _CANDLES[symbol]

    # Round level check (needs previous price)
    if data:
        prev_price = float(data[-1]["p"])
        _check_round_levels(symbol, price, prev_price)

    # Append new bucket
    data.append({
        "t": ts, "p": price,
        "v": vol_delta, "v_valid": vol_valid,
        "cum_vol": cum_vol,
    })

    # Accumulate volume baseline
    if vol_valid and vol_delta > 0:
        _VOL_SAMPLES[symbol].append(vol_delta)

    # Stale data guard: skip if latest bucket is too old
    latest_ts = _bucket_ts(data[-1])
    if latest_ts and (now - latest_ts).total_seconds() > 60:
        return

    # Cooldown check (RAM cache, refreshed from DB at startup)
    last_alert = _LAST_ALERT.get(symbol)
    if last_alert is None:
        last_alert = _get_cooldown(symbol)
        _LAST_ALERT[symbol] = last_alert

    elapsed = (now - last_alert).total_seconds()
    if elapsed < COOLDOWN_NORMAL_S:
        return

    data_list = list(data)
    if len(data_list) < 36:  # need at least 6 min of history
        return

    # Run detectors
    alerted = _check_price_moves(symbol, data_list, now, price)
    if not alerted:
        alerted = _check_volume_explosion(symbol, data_list, now, price, _VOL_SAMPLES[symbol])

    if alerted:
        _set_cooldown(symbol)


# ── Main loop ─────────────────────────────────────────────────────────────────

def _load_coins() -> list[str]:
    coins_file = os.path.join(_SCRIPT_DIR, "coins.json")
    try:
        with open(coins_file, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Could not load coins.json: {e}")
        return []


def main() -> None:
    logger.info("=" * 60)
    logger.info("Pump/Dump Detector V4 (PD-1) — starting")
    logger.info("=" * 60)

    schema = verify_schema()
    if schema["missing"]:
        logger.error(f"Missing tables: {schema['missing']}")
        sys.exit(1)
    logger.info(f"Schema OK — {len(schema['ok'])} tables verified.")

    _ensure_events_table()

    coins = _load_coins()
    if not coins:
        logger.error("No coins loaded — exiting.")
        sys.exit(1)
    coin_set = set(coins)
    logger.info(f"Loaded {len(coins)} symbols.")

    shutdown  = ShutdownHandler("PD1")
    session   = requests.Session()
    last_coin_reload = 0.0

    logger.info(f"Starting market scan — polling every {POLL_EVERY}s...")

    while not shutdown.is_set():
        loop_start = time.time()

        # Reload coins every 10 minutes
        if time.time() - last_coin_reload > 600:
            fresh = _load_coins()
            if fresh:
                coin_set = set(fresh)
                last_coin_reload = time.time()

        try:
            resp = session.get(
                "https://fapi.binance.com/fapi/v1/ticker/24hr",
                timeout=8,
            )
            if resp.status_code != 200:
                logger.warning(f"Binance REST {resp.status_code} — skipping cycle")
                shutdown.sleep(POLL_EVERY)
                continue

            raw = resp.json()

        except requests.RequestException as e:
            logger.warning(f"REST error: {e}")
            shutdown.sleep(POLL_EVERY)
            continue

        for item in raw:
            if shutdown.is_set():
                break

            symbol = item.get("symbol", "")
            if symbol not in coin_set:
                continue

            try:
                price   = float(item["lastPrice"])
                cum_vol = float(item["volume"])

                # Compute 10s volume delta — guard against 24h rollover
                prev_entry = _CANDLES.get(symbol)
                if prev_entry and prev_entry:
                    prev_cum = float(prev_entry[-1]["cum_vol"])
                    raw_delta = cum_vol - prev_cum
                    if raw_delta < 0:
                        vol_delta, vol_valid = 0.0, False  # 24h rollover
                    else:
                        vol_delta, vol_valid = raw_delta, True
                else:
                    vol_delta, vol_valid = 0.0, False  # first data point

                _process_symbol(symbol, price, vol_delta, vol_valid, cum_vol)

            except Exception as e:
                logger.debug(f"Symbol {symbol} error: {e}")

        elapsed = time.time() - loop_start
        sleep   = max(0, POLL_EVERY - elapsed)
        if sleep > 0:
            shutdown.sleep(sleep)

    logger.info("Pump/Dump Detector stopped cleanly.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Pump/Dump Detector stopped (Ctrl+C).")


