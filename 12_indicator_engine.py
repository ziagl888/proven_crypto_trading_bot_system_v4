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
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
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

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - INDICATOR - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/indicator_engine.log", encoding="utf-8"),
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

# Candles to load per symbol for indicator warmup
# 500 candles covers EMA_200 warmup on all timeframes
LOOKBACK_CANDLES = 500

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
    delta = series.diff()
    up    = delta.clip(lower=0)
    down  = -delta.clip(upper=0)
    rs    = up.ewm(span=period, adjust=False).mean() / \
            down.ewm(span=period, adjust=False).mean()
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
    closes   = series.values.astype(float)
    kama_arr = np.full_like(closes, np.nan)
    if len(closes) <= period:
        return pd.Series(kama_arr, index=series.index)
    kama_arr[period - 1] = float(np.mean(closes[:period]))
    fast_sc = 2.0 / (fast + 1)
    slow_sc = 2.0 / (slow + 1)
    for i in range(period, len(closes)):
        change    = abs(closes[i] - closes[i - period])
        volatility = np.sum(np.abs(np.diff(closes[i - period: i + 1])))
        er  = change / volatility if volatility != 0 else 0
        sc  = (er * (fast_sc - slow_sc) + slow_sc) ** 2
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
        prices  = df["close"].values
        volumes = df["volume"].values if "volume" in df.columns else np.ones(len(prices))
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


def _fibonacci(df: pd.DataFrame) -> dict[str, float]:
    try:
        hi   = float(df["high"].max())
        lo   = float(df["low"].min())
        diff = hi - lo
        result = {}
        for lvl in [0.236, 0.382, 0.5, 0.618, 0.786]:
            key = str(lvl).replace(".", "_")
            price = hi - diff * lvl
            result[f"fib_support_{key}"]    = price
            result[f"fib_resistance_{key}"] = price
        for ext in [1.272, 1.618, 2.618]:
            key = str(ext).replace(".", "_")
            result[f"fib_extension_{key}"] = hi + diff * (ext - 1)
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

    # Fibonacci (scalar → broadcast)
    fibs = _fibonacci(df)
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
    ind_df["open_time"] = df["open_time"].values
    ind_df["close"]     = df["close"].values
    ind_df["symbol"]    = df["symbol"].iloc[0] if not df.empty else ""
    return ind_df


# ── DB batch operations ───────────────────────────────────────────────────────

def _load_ohlcv_batch(tf: str, symbols: list[str]) -> pd.DataFrame:
    """
    Loads the last LOOKBACK_CANDLES candles for ALL symbols of a timeframe
    in a single query. Returns a DataFrame with a 'symbol' column.
    """
    with db_connection() as conn:
        placeholders = ",".join(["%s"] * len(symbols))
        sql = f"""
            SELECT symbol, open_time, open, high, low, close, volume
            FROM ohlcv_{tf}
            WHERE symbol IN ({placeholders})
              AND open_time >= NOW() - (
                  SELECT {LOOKBACK_CANDLES} * interval_seconds * interval '1 second'
                  FROM (
                      SELECT EXTRACT(EPOCH FROM MAX(open_time) - MIN(open_time))
                           / NULLIF(COUNT(*) - 1, 0) AS interval_seconds
                      FROM ohlcv_{tf}
                      WHERE symbol = %s
                        AND open_time >= NOW() - INTERVAL '7 days'
                  ) sub
              )
            ORDER BY symbol, open_time ASC
        """
        # Fallback: simpler query using a fixed interval estimate
        try:
            df = pd.read_sql(sql, conn, params=symbols + [symbols[0]])
        except Exception:
            # Fallback with hardcoded lookback window per timeframe
            tf_days = {
                "30m": 11, "1h": 22, "2h": 44, "4h": 88,
                "1d": 520, "1w": 3640
            }
            days = tf_days.get(tf, 30)
            sql2 = f"""
                SELECT symbol, open_time, open, high, low, close, volume
                FROM ohlcv_{tf}
                WHERE symbol IN ({placeholders})
                  AND open_time >= NOW() - INTERVAL '{days} days'
                ORDER BY symbol, open_time ASC
            """
            df = pd.read_sql(sql2, conn, params=symbols)

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


# ── Per-symbol calculation worker ─────────────────────────────────────────────

def _calc_symbol(args: tuple[str, pd.DataFrame, str]) -> pd.DataFrame | None:
    """
    Worker function for ProcessPoolExecutor.
    Receives pre-loaded OHLCV data for one symbol, returns indicator DataFrame.
    """
    import warnings
    warnings.filterwarnings("ignore")

    symbol, df_sym, tf = args
    try:
        if len(df_sym) < 50:
            return None
        ind_df = calculate_indicators(df_sym, tf)
        # Only return the latest row — that's all we need to write/cache
        return ind_df.tail(1)
    except Exception as e:
        logger.warning(f"Indicator calc failed {symbol}/{tf}: {e}")
        return None


# ── Main calculation cycle ────────────────────────────────────────────────────

def run_indicator_cycle(tf: str, symbols: list[str]) -> None:
    """
    Full indicator calculation cycle for one timeframe:
      1. Batch-load OHLCV for all symbols (1 DB query)
      2. Calculate indicators in parallel (ProcessPoolExecutor)
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

    # Step 2: Split by symbol and calculate in parallel
    args_list = [
        (sym, df_all[df_all["symbol"] == sym].copy(), tf)
        for sym in symbols
        if sym in df_all["symbol"].values
    ]

    results: list[pd.DataFrame] = []
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as pool:
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
            INDICATOR_CACHE[tf][sym] = row.to_dict()
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
    symbols = load_coins()
    if not symbols:
        logger.critical("No coins in coins.json — run bootstrap first.")
        sys.exit(1)
    logger.info(f"Loaded {len(symbols)} symbols.")

    # Initial run on startup for all cached timeframes
    logger.info("Running initial indicator calculation on startup...")
    for tf in CACHE_TIMEFRAMES:
        run_indicator_cycle(tf, symbols)
        if INDICATOR_CACHE[tf]:
            run_bots_for_timeframe(tf)

    logger.info("Polling candle_close_events every 10s...")

    while True:
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

                # Ensure timezone aware
                if closed_at.tzinfo is None:
                    closed_at = closed_at.replace(tzinfo=datetime.timezone.utc)

                last = _LAST_PROCESSED.get(tf)
                if last and closed_at <= last:
                    continue  # Already processed this close

                logger.info(f"New candle close detected: {tf} at {closed_at}")
                _LAST_PROCESSED[tf] = closed_at

                # Refresh coin list periodically (housekeeping updates it)
                fresh_symbols = load_coins() or symbols

                # Run calculation in a thread so we can process multiple TFs
                t = threading.Thread(
                    target=_run_cycle_and_bots,
                    args=(tf, fresh_symbols),
                    daemon=True,
                    name=f"ind-{tf}",
                )
                t.start()

        except Exception as e:
            logger.error(f"Poll error: {e}")

        time.sleep(10)


def _run_cycle_and_bots(tf: str, symbols: list[str]) -> None:
    """Runs indicator cycle then bot runner for a timeframe."""
    try:
        run_indicator_cycle(tf, symbols)
        run_bots_for_timeframe(tf)
    except Exception as e:
        logger.error(f"[{tf}] Cycle+bots error: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 60)
    logger.info("Indicator Engine V4 — starting")
    logger.info("=" * 60)

    check_schema()
    poll_and_process()


if __name__ == "__main__":
    main()
