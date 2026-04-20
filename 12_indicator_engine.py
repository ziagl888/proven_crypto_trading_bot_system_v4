#!/usr/bin/env python3
# 12_indicator_engine.py
# Indicator Engine V4 — Single Process, Shared Memory Architecture
#
# Design:
#   - Polls candle_close_events table every 10s
#   - On new close event: batch-reads ALL symbols for that timeframe in ONE query
#   - Calculates all indicators vectorially (pandas/numpy, no Python loops)
#   - Writes results to indicators_* table (one batch upsert)
#   - Stores latest row per symbol in INDICATOR_CACHE (shared dict)
#   - Triggers bot runner threads which read from cache (zero DB reads)
#
# Cache scope:
#   - INDICATOR_CACHE["1h"]["BTCUSDT"] = {col: val, ...}
#   - INDICATOR_CACHE["30m"]["BTCUSDT"] = {col: val, ...}
#   - Other TFs (4h, 2h, 1d, 1w) written to DB only — bots read DB directly
#
# Bot runner:
#   - Bots that need indicators are called as threads after each cache update
#   - Each bot gets a snapshot of the cache — no locking needed during execution
#   - Only DB write: telegram_outbox on signal (rare)

from __future__ import annotations

import datetime
import logging
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import numpy as np
import pandas as pd
from psycopg2 import extras
from scipy import stats
import scipy.signal

from core.config import INDICATOR_TIMEFRAMES, NUM_WORKERS
from core.database import db_connection, get_db_connection
from core.schema import INDICATOR_COLUMNS, verify_schema
from core.bootstrap import load_coins
from core.shutdown import ShutdownHandler

# ── Suppress noisy warnings globally ─────────────────────────────────────────
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", message=".*SQLAlchemy.*")
warnings.filterwarnings("ignore", message=".*pandas.*")
warnings.filterwarnings("ignore", message=".*numpy.*")
# Suppress scipy/statsmodels convergence warnings that spam logs
warnings.filterwarnings("ignore", message=".*divide by zero.*")
warnings.filterwarnings("ignore", message=".*invalid value.*")
warnings.filterwarnings("ignore", message=".*overflow.*")

# ── Logging ───────────────────────────────────────────────────────────────────
# Use absolute path so the log file is always created relative to this script,
# regardless of the working directory when launched by the watchdog.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR     = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - INDICATOR - %(levelname)s - %(message)s",
    force=True,   # override any existing root logger config
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join(_LOG_DIR, "indicator_engine.log"),
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

# ── Cache ─────────────────────────────────────────────────────────────────────
# INDICATOR_CACHE[timeframe][symbol] = {column: value, ...}
# Read by bot threads — written only by the indicator engine main thread.
# Python dicts are thread-safe for read/write of individual keys (GIL).
INDICATOR_CACHE: dict[str, dict[str, dict[str, Any]]] = {
    tf: {} for tf in INDICATOR_TIMEFRAMES
}

# Tracks last processed closed_at per timeframe to avoid double-processing
_LAST_PROCESSED: dict[str, datetime.datetime] = {}

# Tracks which timeframes currently have an active cycle running.
# Prevents spawning multiple parallel cycles for the same TF.
_RUNNING_TFS: set[str] = set()
_RUNNING_TFS_LOCK = threading.Lock()

# Cache-priority timeframes (loaded into INDICATOR_CACHE)
CACHE_TIMEFRAMES = {"1h", "30m"}

# ── Schema check ──────────────────────────────────────────────────────────────

def check_schema() -> None:
    result = verify_schema()
    if result["missing"]:
        logger.critical(f"Missing DB tables: {result['missing']}. Run bootstrap first.")
        sys.exit(1)
    logger.info(f"Schema OK — {len(result['ok'])} tables verified.")


# ── Indicator math ────────────────────────────────────────────────────────────

def _rsi(series: pd.Series, period: int) -> pd.Series:
    """Wilder's RSI — uses com=period-1 (alpha=1/period), not span."""
    delta = series.diff()
    up    = delta.clip(lower=0)
    down  = -delta.clip(upper=0)
    # Wilder's smoothing: alpha = 1/period → com = period - 1
    rs    = up.ewm(com=period - 1, adjust=False).mean() /             down.ewm(com=period - 1, adjust=False).mean()
    return (100.0 - 100.0 / (1.0 + rs)).fillna(50)


def _wma(series: pd.Series, period: int) -> pd.Series:
    weights   = np.arange(1, period + 1, dtype=float)
    sum_w     = weights.sum()
    return series.rolling(period).apply(
        lambda x: np.dot(x, weights) / sum_w, raw=True
    ).fillna(0)


def _smma(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(alpha=1.0 / period, adjust=False).mean().fillna(0)


def _kama(series: pd.Series, period: int = 10, fast: int = 2, slow: int = 30) -> pd.Series:
    """
    Kaufman Adaptive Moving Average.
    Volatility is pre-computed as rolling sum of abs(diff) — avoids
    repeated np.diff slicing inside the loop (O(n) → O(1) per candle).
    """
    closes   = np.asarray(series, dtype=np.float64)   # guaranteed C-contiguous float64
    n        = len(closes)
    kama_arr = np.full(n, np.nan, dtype=np.float64)
    if n <= period:
        return pd.Series(kama_arr, index=series.index)

    # Pre-compute absolute changes and rolling volatility sum
    abs_diff   = np.abs(np.diff(closes, prepend=closes[0]))  # len = n, float64
    vol_cumsum = np.cumsum(abs_diff)
    # vol[i] = sum of abs_diff[i-period+1 .. i]
    vol = vol_cumsum.copy()
    vol[period:] = vol_cumsum[period:] - vol_cumsum[:-period]

    fast_sc = 2.0 / (fast + 1)
    slow_sc = 2.0 / (slow + 1)
    kama_arr[period - 1] = float(np.mean(closes[:period]))

    for i in range(period, n):
        change = abs(closes[i] - closes[i - period])
        v      = vol[i]
        er     = change / v if v != 0 else 0.0
        sc     = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        kama_arr[i] = kama_arr[i - 1] + sc * (closes[i] - kama_arr[i - 1])

    return pd.Series(kama_arr, index=series.index)


def _trendline(df: pd.DataFrame) -> dict[str, pd.Series]:
    lookback = min(len(df), 100)
    subset   = df.iloc[-lookback:]
    y        = subset["close"].values
    x        = np.arange(len(y))
    idx      = df.index

    if len(y) < 2 or np.all(y == y[0]):
        slope, intercept, r_val = 0.0, float(y[0]) if len(y) else 0.0, 0.0
        tv = np.full(len(df), intercept)
        std = 0.0
    else:
        slope, intercept, r_val, _, _ = stats.linregress(x, y)
        slope     = slope if np.isfinite(slope) else 0.0
        intercept = intercept if np.isfinite(intercept) else float(y[-1])
        r_val     = r_val if np.isfinite(r_val) else 0.0
        full_x    = np.arange(len(df)) - (len(df) - lookback)
        tv        = slope * full_x + intercept
        residuals = y - (slope * x + intercept)
        std       = float(np.std(residuals)) if len(residuals) > 0 else 0.0
        std       = std if np.isfinite(std) else 0.0

    base      = float(y[0]) if len(y) and y[0] != 0 else float(y[-1]) if len(y) else 1.0
    threshold = 0.0001 * abs(base) if base != 0 else 1e-8
    if slope > threshold:      direction = "UP"
    elif slope < -threshold:   direction = "DOWN"
    else:                      direction = "SIDEWAYS"

    return {
        "trendline_slope":     pd.Series(slope,             index=idx),
        "trendline_intercept": pd.Series(intercept,         index=idx),
        "trendline_price":     pd.Series(tv,                index=idx),
        "channel_upper_price": pd.Series(tv + 2 * std,      index=idx),
        "channel_lower_price": pd.Series(tv - 2 * std,      index=idx),
        "mid_line":            pd.Series(tv,                index=idx),
        "r_squared":           pd.Series(r_val ** 2,        index=idx),
        "trend_direction":     pd.Series(direction,         index=idx),
    }


def _hvn_poc(df: pd.DataFrame) -> dict[str, float]:
    try:
        prices  = np.asarray(df["close"].values, dtype=np.float64)
        volumes = np.asarray(df["volume"].values, dtype=np.float64) if "volume" in df.columns else np.ones(len(prices), dtype=np.float64)
        bins    = max(int(np.sqrt(len(prices))), 10)
        hist, edges = np.histogram(prices, bins=bins, weights=volumes)
        poc_idx     = int(np.argmax(hist))
        poc_price   = float((edges[poc_idx] + edges[poc_idx + 1]) / 2)
        peaks, _    = scipy.signal.find_peaks(hist, distance=5)
        sorted_peaks = sorted(peaks, key=lambda i: hist[i], reverse=True)
        hvns = []
        for idx in sorted_peaks[:4]:
            p = float((edges[idx] + edges[idx + 1]) / 2)
            if abs(p - poc_price) > poc_price * 0.005:
                hvns.append(p)
        while len(hvns) < 3:
            hvns.append(0.0)
        return {"poc": poc_price, "hvn_1": hvns[0], "hvn_2": hvns[1], "hvn_3": hvns[2]}
    except Exception:
        return {"poc": 0.0, "hvn_1": 0.0, "hvn_2": 0.0, "hvn_3": 0.0}


def _support_resistance(df: pd.DataFrame, window: int = 20) -> dict[str, float]:
    try:
        highs    = df["high"].values
        lows     = df["low"].values
        last_close = float(df["close"].iloc[-1])
        max_idx  = scipy.signal.argrelextrema(highs, np.greater, order=window)[0]
        min_idx  = scipy.signal.argrelextrema(lows,  np.less,    order=window)[0]
        res_list = sorted([highs[i] for i in max_idx if highs[i] > last_close])
        sup_list = sorted([lows[i]  for i in min_idx if lows[i]  < last_close], reverse=True)
        return {
            "support_price":    float(sup_list[0]) if sup_list else 0.0,
            "resistance_price": float(res_list[0]) if res_list else 0.0,
        }
    except Exception:
        return {"support_price": 0.0, "resistance_price": 0.0}


# Timeframe-dependent order parameter for swing detection.
# order=N means N candles left AND right must be lower/higher.
_SWING_ORDER: dict[str, int] = {
    "30m": 5,
    "1h":  5,
    "2h":  8,
    "4h":  10,
    "1d":  10,
    "1w":  5,
}
_SWING_COUNT = 5   # store last N swing highs and lows


def _swings(df: pd.DataFrame, tf: str) -> dict[str, float | int]:
    """
    Detects the last _SWING_COUNT confirmed swing highs and lows.
    Returns price + age (candles since the swing) for each.

    A swing high is a local maximum: N candles left and right are all lower.
    A swing low is a local minimum: N candles left and right are all higher.
    order is timeframe-dependent (see _SWING_ORDER).
    """
    result: dict[str, float | int] = {}
    try:
        order  = _SWING_ORDER.get(tf, 5)
        highs  = df["high"].values
        lows   = df["low"].values
        n      = len(df)

        high_idx = scipy.signal.argrelextrema(highs, np.greater, order=order)[0]
        low_idx  = scipy.signal.argrelextrema(lows,  np.less,    order=order)[0]

        # Most recent first
        sh = list(reversed(high_idx.tolist()))[:_SWING_COUNT]
        sl = list(reversed(low_idx.tolist()))[:_SWING_COUNT]

        for i in range(1, _SWING_COUNT + 1):
            if i - 1 < len(sh):
                idx = sh[i - 1]
                result[f"swing_high_{i}"]     = float(highs[idx])
                result[f"swing_high_{i}_age"] = int(n - 1 - idx)
            else:
                result[f"swing_high_{i}"]     = 0.0
                result[f"swing_high_{i}_age"] = 0

            if i - 1 < len(sl):
                idx = sl[i - 1]
                result[f"swing_low_{i}"]      = float(lows[idx])
                result[f"swing_low_{i}_age"]  = int(n - 1 - idx)
            else:
                result[f"swing_low_{i}"]      = 0.0
                result[f"swing_low_{i}_age"]  = 0

    except Exception:
        for i in range(1, _SWING_COUNT + 1):
            result[f"swing_high_{i}"]     = 0.0
            result[f"swing_high_{i}_age"] = 0
            result[f"swing_low_{i}"]      = 0.0
            result[f"swing_low_{i}_age"]  = 0

    return result


def _fibonacci(df: pd.DataFrame, tf: str) -> dict[str, float]:
    """
    Fibonacci retracements and extensions based on the most recent
    confirmed swing high and swing low.

    fib_support_*   = retracement levels from last swing high DOWN
                      (where price finds support on a pullback in uptrend)
    fib_resistance_* = retracement levels from last swing low UP
                      (where price finds resistance on a rally in downtrend)
    fib_extension_* = extension levels above last swing high
                      (upside targets beyond the swing high)

    If no swing is detected, falls back to absolute period high/low.
    """
    try:
        order     = _SWING_ORDER.get(tf, 5)
        highs_arr = df["high"].values
        lows_arr  = df["low"].values

        high_idx = scipy.signal.argrelextrema(highs_arr, np.greater, order=order)[0]
        low_idx  = scipy.signal.argrelextrema(lows_arr,  np.less,    order=order)[0]

        # Use most recent swing high and swing low
        sh = float(highs_arr[high_idx[-1]]) if len(high_idx) > 0 else float(df["high"].max())
        sl = float(lows_arr[low_idx[-1]])   if len(low_idx)  > 0 else float(df["low"].min())

        result: dict[str, float] = {}
        diff = sh - sl
        if diff <= 0:
            return result

        # Retracement from swing high downward → Support levels
        # Price pulls back from sh toward sl — these are buy zones
        for lvl in [0.236, 0.382, 0.5, 0.618, 0.786]:
            key   = str(lvl).replace(".", "_")
            price = sh - diff * lvl
            result[f"fib_support_{key}"] = price

        # Retracement from swing low upward → Resistance levels
        # Price rallies from sl toward sh — these are sell zones
        for lvl in [0.236, 0.382, 0.5, 0.618, 0.786]:
            key   = str(lvl).replace(".", "_")
            price = sl + diff * lvl
            result[f"fib_resistance_{key}"] = price

        # Extensions above swing high → Upside targets
        for ext in [1.272, 1.618, 2.618]:
            key   = str(ext).replace(".", "_")
            result[f"fib_extension_{key}"] = sh + diff * (ext - 1.0)

        return result
    except Exception:
        return {}


def calculate_indicators(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    """
    Calculates all indicators for a single symbol's OHLCV DataFrame.
    Returns a DataFrame with one row per candle (same index as input).
    All calculations are vectorial — no Python loops over candles.
    """
    df    = df.sort_values("open_time").copy()
    close = df["close"]
    high  = df["high"]
    low   = df["low"]
    r: dict[str, Any] = {}

    # RSI
    for p in [6, 9, 12, 14, 24]:
        r[f"rsi_{p}"] = _rsi(close, p)

    # EMA
    for p in [7, 9, 12, 21, 26, 34, 50, 55, 89, 99, 200]:
        r[f"ema_{p}"] = close.ewm(span=p, adjust=False).mean().fillna(0)

    # SMA
    for p in [7, 10, 20, 25, 50, 99, 100, 200]:
        r[f"ma_{p}"] = close.rolling(p).mean().fillna(0)

    # WMA
    for p in [7, 9, 12, 21, 26, 34, 50, 55, 89, 99, 200]:
        r[f"wma_{p}"] = _wma(close, p)

    # SMMA
    for p in [10, 20, 25, 50, 99, 100, 200]:
        r[f"smma_{p}"] = _smma(close, p)

    # KAMA
    for p in [7, 9, 12, 21, 26, 34, 50, 55, 89, 99]:
        r[f"kama_{p}"] = _kama(close, p)

    # ATR
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low  - close.shift()).abs()
    tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    for p in [9, 14, 21]:
        r[f"atr_{p}"] = tr.ewm(alpha=1.0 / p, adjust=False).mean().fillna(0)

    # TSI
    def _tsi(r_period: int, s_period: int) -> tuple[pd.Series, pd.Series]:
        diff   = close.diff()
        sm1    = diff.ewm(span=r_period, adjust=False).mean()
        sm2    = sm1.ewm(span=s_period, adjust=False).mean()
        abs1   = diff.abs().ewm(span=r_period, adjust=False).mean()
        abs2   = abs1.ewm(span=s_period, adjust=False).mean()
        tsi_v  = (100 * sm2 / abs2).fillna(0)
        return tsi_v, tsi_v.ewm(span=s_period, adjust=False).mean()

    r["tsi_25_13_13"], r["tsi_25_13_13_signal"]       = _tsi(25, 13)
    r["tsi_fast_12_7_7"], r["tsi_fast_12_7_7_signal"] = _tsi(12, 7)

    # MACD
    def _macd(fast: int, slow: int, sig: int) -> tuple[pd.Series, pd.Series]:
        dif = close.ewm(span=fast, adjust=False).mean() - \
              close.ewm(span=slow, adjust=False).mean()
        dea = dif.ewm(span=sig, adjust=False).mean()
        return dif, dea

    r["macd_dif_fast_9_21_9"],    r["macd_dea_fast_9_21_9"]    = _macd(9,  21, 9)
    r["macd_dif_normal_12_26_9"], r["macd_dea_normal_12_26_9"] = _macd(12, 26, 9)

    # Bollinger
    boll_mid = close.rolling(20).mean()
    boll_std = close.rolling(20).std()
    r["boll_mid_20"]   = boll_mid.fillna(0)
    r["boll_upper_20"] = (boll_mid + 2 * boll_std).fillna(0)
    r["boll_lower_20"] = (boll_mid - 2 * boll_std).fillna(0)

    # Donchian
    for w in [4, 10, 12, 15, 20]:
        up  = high.rolling(w).max().fillna(0)
        lo_ = low.rolling(w).min().fillna(0)
        r[f"donchian_upper_{w}"] = up
        r[f"donchian_lower_{w}"] = lo_
        r[f"donchian_mid_{w}"]   = ((up + lo_) / 2).fillna(0)

    # Trendline & channel (scalar → broadcast to series)
    r.update(_trendline(df))

    # HVN / POC (scalar → broadcast)
    hvn = _hvn_poc(df)
    for k, v in hvn.items():
        r[k] = v

    # Support / Resistance (scalar → broadcast)
    sr = _support_resistance(df)
    for k, v in sr.items():
        r[k] = v

    # Swing Highs / Lows
    swings = _swings(df, tf)
    r.update(swings)

    # Fibonacci — based on actual swing high/low (not period max/min)
    fibs = _fibonacci(df, tf)
    for k, v in fibs.items():
        r[k] = v

    # ── Derived features ──────────────────────────────────────────────
    eps = 1e-10
    atr14       = r["atr_14"]
    boll_up     = r["boll_upper_20"]
    boll_lo     = r["boll_lower_20"]
    dc_up       = r["donchian_upper_20"]
    dc_lo       = r["donchian_lower_20"]
    ema9        = r["ema_9"]
    ema21       = r["ema_21"]
    ema200      = r["ema_200"]
    kama9       = r["kama_9"]
    macd_dif_f  = r["macd_dif_fast_9_21_9"]
    macd_dea_f  = r["macd_dea_fast_9_21_9"]
    macd_dif_n  = r["macd_dif_normal_12_26_9"]
    macd_dea_n  = r["macd_dea_normal_12_26_9"]

    r["atr_pct"]               = (atr14 / (close + eps) * 100).fillna(0)
    boll_range                 = (boll_up - boll_lo).replace(0, np.nan)
    r["bb_position_relative"]  = ((close - boll_lo) / boll_range).fillna(0)
    dc_range                   = (dc_up - dc_lo).replace(0, np.nan)
    r["dc_position_relative"]  = ((close - dc_lo) / dc_range).fillna(0)
    r["dist_close_ema9_pct"]   = ((close - ema9)   / (ema9   + eps)).fillna(0)
    r["dist_ema9_ema21_pct"]   = ((ema9  - ema21)  / (ema21  + eps)).fillna(0)
    r["dist_close_kama9_pct"]  = ((close - kama9)  / (kama9  + eps)).fillna(0)
    r["dist_close_ema200_pct"] = ((close - ema200) / (ema200 + eps)).fillna(0)
    r["macd_hist_fast"]        = (macd_dif_f - macd_dea_f).fillna(0)
    r["macd_hist_normal"]      = (macd_dif_n - macd_dea_n).fillna(0)

    # Assemble result DataFrame
    ind_df = pd.DataFrame(r, index=df.index)
    # Keep as pandas Series (not .values) to preserve UTC timezone info.
    # .values would return numpy datetime64 without tzinfo, causing psycopg2
    # to interpret timestamps as local time instead of UTC.
    ind_df["open_time"] = df["open_time"]
    ind_df["close"]     = df["close"]
    ind_df["symbol"]    = df["symbol"].iloc[0] if not df.empty else ""
    return ind_df


# ── DB batch operations ───────────────────────────────────────────────────────

# Lookback window per timeframe — covers LOOKBACK_CANDLES (500) with margin.
# EMA_200 needs ~1000 candles to stabilize; 500 is sufficient for all others.
# These day values give 500+ candles for each TF.
_TF_LOOKBACK_DAYS: dict[str, int] = {
    "30m": 11,    # 500 candles × 30min = 10.4 days
    "1h":  22,    # 500 candles × 1h   = 20.8 days
    "2h":  44,    # 500 candles × 2h   = 41.7 days
    "4h":  85,    # 500 candles × 4h   = 83.3 days
    "1d":  520,   # 500 candles × 1d   = 500 days
    "1w":  3640,  # 500 candles × 1w   = 3500 days
}


def _load_ohlcv_batch(tf: str, symbols: list[str]) -> pd.DataFrame:
    """
    Loads the last N days of candles for ALL symbols of a timeframe
    in a single query. Returns a DataFrame with a 'symbol' column.
    N is chosen to cover LOOKBACK_CANDLES for each timeframe.
    """
    days         = _TF_LOOKBACK_DAYS.get(tf, 30)
    placeholders = ",".join(["%s"] * len(symbols))

    sql = f"""
        SELECT symbol, open_time, open, high, low, close, volume
        FROM ohlcv_{tf}
        WHERE symbol IN ({placeholders})
          AND open_time >= NOW() - INTERVAL '{days} days'
        ORDER BY symbol, open_time ASC
    """
    with db_connection() as conn:
        df = pd.read_sql(sql, conn, params=symbols)

    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    return df


def _write_indicators_batch(tf: str, results: list[pd.DataFrame]) -> int:
    """
    Writes indicator results for all symbols to indicators_{tf} in one upsert.
    Returns number of rows written.
    """
    if not results:
        return 0

    combined = pd.concat(results, ignore_index=True)

    # Ensure open_time is UTC-aware after concat (concat can drop tz info)
    if "open_time" in combined.columns:
        combined["open_time"] = pd.to_datetime(combined["open_time"], utc=True)

    # Build column list from schema
    valid_cols = ["symbol", "open_time", "close"] + \
                 [col for col, _ in INDICATOR_COLUMNS]

    for col in valid_cols:
        if col not in combined.columns:
            combined[col] = 0

    combined = combined[valid_cols].copy()

    # Replace inf / nan with 0
    combined = combined.replace([np.inf, -np.inf], 0).fillna(0)

    cols_str   = ", ".join(valid_cols)
    update_sql = ", ".join(
        f"{c} = EXCLUDED.{c}"
        for c in valid_cols if c not in ("symbol", "open_time")
    )
    sql = f"""
        INSERT INTO indicators_{tf} ({cols_str})
        VALUES %s
        ON CONFLICT (symbol, open_time) DO UPDATE SET {update_sql}
    """
    rows = [tuple(x) for x in combined.to_numpy()]

    with db_connection() as conn:
        with conn.cursor() as cur:
            extras.execute_values(cur, sql, rows, page_size=500)
        conn.commit()

    return len(rows)


# ── Per-symbol calculation workers ───────────────────────────────────────────

def _calc_symbol(args: tuple[str, pd.DataFrame, str]) -> pd.DataFrame | None:
    """
    Worker function for ThreadPoolExecutor.
    Calculates indicators for one symbol, returns latest row only.
    """
    symbol, df_sym, tf = args
    try:
        if len(df_sym) < 50:
            return None
        return calculate_indicators(df_sym, tf).tail(1)
    except Exception as e:
        logger.warning(f"Indicator calc failed {symbol}/{tf}: {e}")
        return None




# ── Indicator gap fill ───────────────────────────────────────────────────────

def _get_indicator_gaps(tf: str, symbols: list[str]) -> dict[str, datetime.datetime]:
    """
    Returns dict of {symbol: last_indicator_open_time} for symbols
    where indicators lag behind OHLCV data.
    Only returns symbols that actually have a gap.
    """
    gaps: dict[str, datetime.datetime] = {}
    try:
        placeholders = ",".join(["%s"] * len(symbols))
        with db_connection() as conn:
            with conn.cursor() as cur:
                # Get last indicator timestamp per symbol
                cur.execute(
                    f"""
                    SELECT symbol, MAX(open_time) as last_ind
                    FROM indicators_{tf}
                    WHERE symbol IN ({placeholders})
                    GROUP BY symbol
                    """,
                    symbols,
                )
                ind_latest = {row[0]: row[1] for row in cur.fetchall()}

                # Get last OHLCV timestamp per symbol
                cur.execute(
                    f"""
                    SELECT symbol, MAX(open_time) as last_ohlcv
                    FROM ohlcv_{tf}
                    WHERE symbol IN ({placeholders})
                    GROUP BY symbol
                    """,
                    symbols,
                )
                ohlcv_latest = {row[0]: row[1] for row in cur.fetchall()}

        # Find symbols where ohlcv is ahead of indicators
        for sym in symbols:
            last_ohlcv = ohlcv_latest.get(sym)
            last_ind   = ind_latest.get(sym)

            if last_ohlcv is None:
                continue  # No OHLCV data yet

            if last_ohlcv.tzinfo is None:
                last_ohlcv = last_ohlcv.replace(tzinfo=datetime.timezone.utc)

            if last_ind is None:
                # No indicators at all — need full calculation
                gaps[sym] = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
                continue

            if last_ind.tzinfo is None:
                last_ind = last_ind.replace(tzinfo=datetime.timezone.utc)

            if last_ohlcv > last_ind:
                gaps[sym] = last_ind

    except Exception as e:
        logger.error(f"[{tf}] Indicator gap detection error: {e}")

    return gaps


def fill_indicator_gaps(tf: str, symbols: list[str]) -> None:
    """
    Detects symbols where indicators lag behind OHLCV data and
    recalculates ALL missing indicator rows (Option B — full backfill).

    This runs:
      - On startup (before the normal poll loop)
      - After the gap checker deletes stale indicator rows
      - Any time the engine detects missing indicator data
    """
    logger.info(f"[{tf}] Checking for indicator gaps...")
    gaps = _get_indicator_gaps(tf, symbols)

    if not gaps:
        logger.info(f"[{tf}] No indicator gaps found.")
        return

    logger.info(f"[{tf}] Found {len(gaps)} symbols with indicator gaps — recalculating...")

    for sym, last_ind_time in gaps.items():
        try:
            # Load all OHLCV from last indicator time onward (+ lookback for warmup)
            days     = _TF_LOOKBACK_DAYS.get(tf, 30)
            # Use max of: lookback window OR time since last indicator
            cutoff   = max(
                datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days),
                last_ind_time - datetime.timedelta(days=days),
            )

            with db_connection() as conn:
                df_sym = pd.read_sql(
                    f"SELECT symbol, open_time, open, high, low, close, volume "
                    f"FROM ohlcv_{tf} "
                    f"WHERE symbol = %s AND open_time >= %s "
                    f"ORDER BY open_time ASC",
                    conn,
                    params=(sym, cutoff),
                )

            if df_sym.empty or len(df_sym) < 50:
                continue

            df_sym["open_time"] = pd.to_datetime(df_sym["open_time"], utc=True)

            # Calculate ALL indicators for full history
            ind_df = calculate_indicators(df_sym, tf)

            # Only write rows AFTER last_ind_time (avoid overwriting correct data)
            ind_to_write = ind_df[ind_df["open_time"] > last_ind_time]

            if not ind_to_write.empty:
                _write_indicators_batch(tf, [ind_to_write])
                logger.info(
                    f"[{tf}] Gap fill {sym}: wrote {len(ind_to_write)} rows "
                    f"(from {ind_to_write['open_time'].iloc[0].strftime('%Y-%m-%d %H:%M')})"
                )

                # Update cache if this is a priority TF
                if tf in CACHE_TIMEFRAMES:
                    last_row = ind_to_write.iloc[-1]
                    INDICATOR_CACHE[tf][sym] = {
                        str(k): v for k, v in last_row.to_dict().items()
                    }

        except Exception as e:
            logger.warning(f"[{tf}] Gap fill failed for {sym}: {e}")

    logger.info(f"[{tf}] Indicator gap fill complete.")


# ── Main calculation cycle ────────────────────────────────────────────────────

def run_indicator_cycle(tf: str, symbols: list[str]) -> None:
    """
    Full indicator calculation cycle for one timeframe:
      1. Batch-load OHLCV for all symbols (1 DB query)
      2. Calculate indicators in parallel (ThreadPoolExecutor)
      3. Batch-write to indicators_{tf} (1 DB upsert)
      4. Update INDICATOR_CACHE for priority timeframes
    """
    t_start = time.time()
    logger.info(f"[{tf}] Starting indicator cycle for {len(symbols)} symbols...")

    # Step 1: Load all OHLCV in one query
    try:
        df_all = _load_ohlcv_batch(tf, symbols)
    except Exception as e:
        logger.error(f"[{tf}] OHLCV batch load failed: {e}")
        return

    if df_all.empty:
        logger.warning(f"[{tf}] No OHLCV data found.")
        return

    # Step 2: Split by symbol using groupby (one pass, not 500 boolean masks)
    sym_groups = {str(sym): grp.copy() for sym, grp in df_all.groupby("symbol", sort=False)}
    args_list = [
        (sym, sym_groups[sym], tf)
        for sym in symbols
        if sym in sym_groups
    ]

    # Use ThreadPoolExecutor — avoids ProcessPool spawn issues on Windows
    # (Python 3.14 + WMI bug) and eliminates pickle/IPC overhead entirely.
    # pandas/numpy release the GIL during computation so threads are effective.
    results: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
        futures = {pool.submit(_calc_symbol, args): args[0] for args in args_list}
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                results.append(result)

    if not results:
        logger.warning(f"[{tf}] No indicator results produced.")
        return

    # Step 3: Batch-write to DB (1 upsert)
    written = _write_indicators_batch(tf, results)

    # Step 4: Update cache for priority timeframes
    if tf in CACHE_TIMEFRAMES:
        cache_updated = 0
        for df_row in results:
            if df_row.empty:
                continue
            row  = df_row.iloc[0]
            sym  = str(row.get("symbol", ""))
            if not sym:
                continue
            INDICATOR_CACHE[tf][sym] = {str(k): v for k, v in row.to_dict().items()}
            cache_updated += 1
        logger.info(
            f"[{tf}] Cache updated: {cache_updated} symbols."
        )

    elapsed = time.time() - t_start
    logger.info(
        f"[{tf}] Cycle complete — {written} rows written, "
        f"{len(results)} symbols, {elapsed:.1f}s."
    )


# ── Bot runner ────────────────────────────────────────────────────────────────
# Bots are imported and called as functions here.
# Each bot receives a snapshot of the indicator cache.
# Only DB write happens when a signal is generated (telegram_outbox).
#
# Bots are added here incrementally as they are migrated to V4.

def _get_cache_snapshot(tf: str) -> dict[str, dict[str, Any]]:
    """Returns a shallow copy of the cache for a timeframe."""
    return dict(INDICATOR_CACHE.get(tf, {}))


def run_bots_for_timeframe(tf: str) -> None:
    """
    Runs all bots that depend on this timeframe's indicators.
    Each bot runs in its own thread for parallelism.
    Bots read from cache snapshot — no DB reads for indicators.
    """
    # Cache snapshot — thread-safe copy before dispatching threads
    cache_1h  = _get_cache_snapshot("1h")
    cache_30m = _get_cache_snapshot("30m")

    if not cache_1h and tf == "1h":
        logger.warning("Bot runner: 1h cache is empty — skipping.")
        return
    if not cache_30m and tf == "30m":
        logger.warning("Bot runner: 30m cache is empty — skipping.")
        return

    # Bot functions will be added here as V4 migration progresses.
    # Example pattern:
    #
    # from bots.ai_sr_bot import run_scan as ai_sr_scan
    # bot_tasks = [
    #     lambda: ai_sr_scan(cache_1h),
    #     lambda: ai_mis_scan(cache_1h),
    # ]
    # with ThreadPoolExecutor(max_workers=8) as pool:
    #     futures = [pool.submit(task) for task in bot_tasks]
    #     for f in as_completed(futures):
    #         try:
    #             f.result()
    #         except Exception as e:
    #             logger.error(f"Bot error: {e}")

    logger.debug(f"[{tf}] Bot runner: no bots registered yet.")


# ── Event poller ──────────────────────────────────────────────────────────────

def poll_and_process() -> None:
    """
    Main loop: polls candle_close_events every 10s.
    Triggers indicator calculation + bot runner when a new close is detected.
    """
    # Wait for ingestion to finish initial backfill before calculating indicators.
    # Without this, the engine would calculate on empty/partial OHLCV tables.
    from core.system_state import wait_for, KEY_BACKFILL_DONE
    if not wait_for(KEY_BACKFILL_DONE, poll_interval=30, log_interval=120):
        logger.critical("Timed out waiting for initial backfill. Aborting.")
        sys.exit(1)
    logger.info("Initial backfill confirmed — starting indicator calculations.")

    symbols = load_coins()
    if not symbols:
        logger.critical("No coins in coins.json — run bootstrap first.")
        sys.exit(1)
    logger.info(f"Loaded {len(symbols)} symbols.")

    # Step 1: Fill any indicator gaps from previous downtime (all TFs)
    logger.info("Checking for indicator gaps from previous downtime...")
    for tf in INDICATOR_TIMEFRAMES:
        fill_indicator_gaps(tf, symbols)

    # Step 2: Initial full cycle for cache-priority timeframes
    logger.info("Running initial indicator calculation on startup...")
    for tf in CACHE_TIMEFRAMES:
        run_indicator_cycle(tf, symbols)
        if INDICATOR_CACHE[tf]:
            run_bots_for_timeframe(tf)

    logger.info("Polling candle_close_events every 10s...")
    shutdown = ShutdownHandler("INDICATOR")

    while not shutdown.is_set():
        try:
            with db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT timeframe, closed_at FROM candle_close_events"
                    )
                    rows = cur.fetchall()

            for tf, closed_at in rows:
                if tf not in INDICATOR_TIMEFRAMES:
                    continue
                if shutdown.is_set():
                    break

                # Ensure timezone aware
                if closed_at.tzinfo is None:
                    closed_at = closed_at.replace(tzinfo=datetime.timezone.utc)

                last = _LAST_PROCESSED.get(tf)
                if last and closed_at <= last:
                    continue  # Already processed this close

                logger.info(f"New candle close detected: {tf} at {closed_at}")

                # Skip if a cycle is already running for this TF
                with _RUNNING_TFS_LOCK:
                    if tf in _RUNNING_TFS:
                        logger.debug(f"[{tf}] Cycle already running — skipping poll.")
                        continue
                    _RUNNING_TFS.add(tf)

                fresh_symbols = load_coins() or symbols

                t = threading.Thread(
                    target=_run_cycle_and_bots,
                    args=(tf, fresh_symbols, closed_at),
                    daemon=True,
                    name=f"ind-{tf}",
                )
                t.start()

        except Exception as e:
            logger.error(f"Poll error: {e}")

        # Interruptible sleep
        shutdown.sleep(10)

    # Wait for any running cycles to finish
    logger.info("Indicator Engine stopping — waiting for active cycles...")
    for _ in range(shutdown.timeout):
        with _RUNNING_TFS_LOCK:
            if not _RUNNING_TFS:
                break
        time.sleep(1)
    logger.info("Indicator Engine stopped cleanly.")


def _run_cycle_and_bots(tf: str, symbols: list[str], closed_at: datetime.datetime) -> None:
    """
    Runs indicator cycle then bot runner for a timeframe.
    Always removes tf from _RUNNING_TFS when done so the next
    candle close can trigger a new cycle.
    Only marks _LAST_PROCESSED after success — failed cycles retry on next poll.
    """
    try:
        run_indicator_cycle(tf, symbols)
        _LAST_PROCESSED[tf] = closed_at   # mark as done only on success
        run_bots_for_timeframe(tf)
    except Exception as e:
        logger.error(f"[{tf}] Cycle+bots error: {e}")
        # Do NOT update _LAST_PROCESSED — will retry on next poll
    finally:
        with _RUNNING_TFS_LOCK:
            _RUNNING_TFS.discard(tf)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 60)
    logger.info("Indicator Engine V4 — starting")
    logger.info("=" * 60)

    check_schema()
    poll_and_process()


if __name__ == "__main__":
    main()











