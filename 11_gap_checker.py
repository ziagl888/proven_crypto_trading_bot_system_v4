#!/usr/bin/env python3
# 11_gap_checker.py
# OHLCV Gap Checker — runs at :20 and :50 of every hour.
#
# Responsibilities:
#   1. Check ALL OHLCV timeframes for gaps (missing candles)
#   2. Fill gaps via REST backfill
#   3. Delete indicator rows AFTER the oldest gap per symbol/TF
#      so the indicator engine re-calculates from a clean state
#
# Why :20 and :50?
#   - 30m candle closes at :00 and :30
#   - Indicator engine needs ~5-10 min to calculate
#   - Gap check at :20/:50 gives enough buffer after the close
#   - Also catches gaps from any downtime in the previous period

from __future__ import annotations

import datetime
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from core.bootstrap import load_coins
from core.shutdown import ShutdownHandler
from core.config import INGEST_TIMEFRAMES
from core.database import db_connection, get_db_connection
from core.schema import verify_schema, INDICATOR_TIMEFRAMES

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - GAP_CHECKER - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/gap_checker.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

import warnings
warnings.filterwarnings("ignore")

# ── Constants ─────────────────────────────────────────────────────────────────
BASE_URL     = "https://fapi.binance.com"
REST_WORKERS = 3
REST_LIMIT   = 1500   # max candles per Binance klines request

# Binance Futures REST weight limit: 2400/min = 40/sec
# Each klines request costs 2 weight → max 20 req/sec
# With 3 workers we target ~5 req/sec per worker = safe margin
REST_INTER_REQUEST_SLEEP = 0.25   # 250ms between requests per worker
REST_WORKER_STAGGER      = 1.0    # seconds between worker starts

# Run at these minutes past the hour
RUN_MINUTES = {20, 50}

# Timeframe duration in seconds
_TF_SECONDS: dict[str, int] = {
    "5m":  300,  "15m": 900,   "30m": 1800,
    "1h":  3600, "2h":  7200,  "4h":  14400,
    "6h":  21600,"8h":  28800, "12h": 43200,
    "1d":  86400,"3d":  259200,"1w":  604800, "1M": 2592000,
}

# Gap tolerance per timeframe — small gaps on 5m/15m for illiquid coins
# are often real (no trades in that period) rather than ingestion failures.
# We only treat it as a gap if more than this many candles are missing.
_GAP_TOLERANCE: dict[str, int] = {
    "5m":  2,    # illiquid coins often have genuine 5m gaps
    "15m": 2,
    "30m": 1,
    "1h":  1,
    "2h":  1,
    "4h":  1,
    "6h":  1,
    "8h":  1,
    "12h": 1,
    "1d":  1,
    "3d":  1,
    "1w":  1,
    "1M":  1,
}


# ── REST helpers ──────────────────────────────────────────────────────────────

def _fetch_klines(
    session: requests.Session,
    symbol: str,
    tf: str,
    start_ms: int,
    end_ms: int,
) -> list[tuple]:
    """Fetches klines via REST, returns list of DB-ready tuples."""
    url  = BASE_URL + "/fapi/v1/klines"
    rows = []
    curr = start_ms

    while curr < end_ms:
        params = {
            "symbol":    symbol,
            "interval":  tf,
            "startTime": curr,
            "endTime":   end_ms,
            "limit":     REST_LIMIT,
        }
        try:
            resp = session.get(url, params=params, timeout=15)
            if resp.status_code in (429, 418):
                wait = int(resp.headers.get("Retry-After", 30)) + 5
                logger.warning(f"Rate limited — sleeping {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                break
            data = resp.json()
            if not data:
                break
            for k in data:
                open_time = datetime.datetime.fromtimestamp(
                    k[0] / 1000, tz=datetime.timezone.utc
                )
                rows.append((
                    symbol, open_time,
                    float(k[1]), float(k[2]), float(k[3]),
                    float(k[4]), float(k[5]),
                ))
            curr = data[-1][6] + 1
            if len(data) < REST_LIMIT:
                break
            time.sleep(REST_INTER_REQUEST_SLEEP)
        except Exception as e:
            logger.warning(f"REST error {symbol}/{tf}: {e}")
            time.sleep(5)

    return rows


def _upsert_ohlcv(conn, tf: str, rows: list[tuple]) -> int:
    """Upserts OHLCV rows into ohlcv_{tf}."""
    if not rows:
        return 0
    from psycopg2 import extras
    sql = f"""
        INSERT INTO ohlcv_{tf} (symbol, open_time, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (symbol, open_time) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
            close=EXCLUDED.close, volume=EXCLUDED.volume
    """
    with conn.cursor() as cur:
        extras.execute_values(cur, sql, rows, page_size=500)
    conn.commit()
    return len(rows)


# ── Gap detection ─────────────────────────────────────────────────────────────

def _find_ohlcv_gaps(
    conn,
    symbol: str,
    tf: str,
) -> list[tuple[datetime.datetime, datetime.datetime]]:
    """
    Returns list of (gap_start, gap_end) tuples for missing candles.
    Uses generate_series to find all expected timestamps and
    compares with what's actually in the DB.
    """
    tf_sec = _TF_SECONDS.get(tf)
    if not tf_sec:
        return []

    try:
        with conn.cursor() as cur:
            # Get the time range we care about
            cur.execute(
                f"SELECT MIN(open_time), MAX(open_time), COUNT(*) "
                f"FROM ohlcv_{tf} WHERE symbol = %s",
                (symbol,),
            )
            row = cur.fetchone()
            if not row or row[2] == 0:
                return []

            min_ot, max_ot, count = row
            if min_ot.tzinfo is None:
                min_ot = min_ot.replace(tzinfo=datetime.timezone.utc)
            if max_ot.tzinfo is None:
                max_ot = max_ot.replace(tzinfo=datetime.timezone.utc)

            expected = int(
                (max_ot - min_ot).total_seconds() / tf_sec
            ) + 1

            tolerance = _GAP_TOLERANCE.get(tf, 1)
            if expected - count <= tolerance:
                return []  # No significant gaps

            # Find the actual missing timestamps
            cur.execute(
                f"""
                SELECT gs::timestamptz AS expected_time
                FROM generate_series(
                    %s::timestamptz,
                    %s::timestamptz,
                    '{tf_sec} seconds'::interval
                ) gs
                WHERE NOT EXISTS (
                    SELECT 1 FROM ohlcv_{tf}
                    WHERE symbol = %s
                      AND open_time = gs::timestamptz
                )
                ORDER BY gs
                """,
                (min_ot, max_ot, symbol),
            )
            missing = [r[0] for r in cur.fetchall()]

        if not missing:
            return []

        # Consolidate into contiguous ranges
        gaps = []
        gap_start = missing[0]
        gap_end   = missing[0]
        for ts in missing[1:]:
            expected_next = gap_end + datetime.timedelta(seconds=tf_sec)
            if ts <= expected_next + datetime.timedelta(seconds=1):
                gap_end = ts
            else:
                gaps.append((gap_start, gap_end))
                gap_start = ts
                gap_end   = ts
        gaps.append((gap_start, gap_end))
        return gaps

    except Exception as e:
        logger.warning(f"Gap detection error {symbol}/{tf}: {e}")
        conn.rollback()
        return []


# ── Indicator cleanup ─────────────────────────────────────────────────────────

def _delete_indicators_from(
    conn,
    symbol: str,
    tf: str,
    from_time: datetime.datetime,
) -> int:
    """
    Deletes all indicator rows for symbol/tf at or after from_time.
    These rows were calculated with incomplete OHLCV data and must
    be recalculated by the indicator engine.
    """
    if tf not in INDICATOR_TIMEFRAMES:
        return 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM indicators_{tf} "
                f"WHERE symbol = %s AND open_time >= %s",
                (symbol, from_time),
            )
            deleted = cur.rowcount
        conn.commit()
        return deleted
    except Exception as e:
        conn.rollback()
        logger.warning(f"Indicator delete error {symbol}/{tf}: {e}")
        return 0


# ── Trigger indicator engine ─────────────────────────────────────────────────

def _trigger_indicator_recalc(affected_tfs: set[str]) -> None:
    """
    After filling gaps and deleting stale indicator rows, update
    candle_close_events for the affected timeframes so the indicator
    engine picks up the change within its 10s poll interval.

    We set closed_at to NOW() which is newer than _LAST_PROCESSED in
    the engine — this guarantees the engine runs a fresh cycle.
    """
    if not affected_tfs:
        return

    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                for tf in affected_tfs:
                    cur.execute(
                        """
                        INSERT INTO candle_close_events
                            (timeframe, closed_at, symbol_count)
                        VALUES (%s, NOW(), 0)
                        ON CONFLICT (timeframe) DO UPDATE SET
                            closed_at    = EXCLUDED.closed_at,
                            symbol_count = EXCLUDED.symbol_count
                        """,
                        (tf,),
                    )
            conn.commit()
        logger.info(
            f"Triggered indicator recalculation for: {sorted(affected_tfs)}"
        )
    except Exception as e:
        logger.warning(f"Could not trigger indicator recalc: {e}")


# ── Per-symbol gap check + fill ───────────────────────────────────────────────

def _check_and_fill_symbol(symbol: str) -> dict:
    """
    Checks all INGEST_TIMEFRAMES for a single symbol.
    Returns summary dict with gaps found/filled per TF.
    """
    conn    = get_db_connection()
    session = requests.Session()
    session.headers.update({"User-Agent": "CryptoBotV4-GapChecker/1.0"})
    summary = {"symbol": symbol, "gaps_found": 0, "rows_filled": 0, "ind_deleted": 0}

    try:
        for tf in INGEST_TIMEFRAMES:
            gaps = _find_ohlcv_gaps(conn, symbol, tf)
            if not gaps:
                continue

            summary["gaps_found"] += len(gaps)
            oldest_gap_start = gaps[0][0]  # gaps are ordered ascending

            logger.info(
                f"Gap: {symbol}/{tf} — {len(gaps)} gap(s), "
                f"oldest at {oldest_gap_start.strftime('%Y-%m-%d %H:%M')}"
            )

            # Fill each gap via REST
            for gap_start, gap_end in gaps:
                # Add one TF interval buffer on each side
                tf_sec    = _TF_SECONDS.get(tf, 3600)
                start_ms  = int(
                    (gap_start - datetime.timedelta(seconds=tf_sec)).timestamp() * 1000
                )
                end_ms    = int(
                    (gap_end + datetime.timedelta(seconds=tf_sec)).timestamp() * 1000
                )
                rows = _fetch_klines(session, symbol, tf, start_ms, end_ms)
                if rows:
                    filled = _upsert_ohlcv(conn, tf, rows)
                    summary["rows_filled"] += filled

            # Delete indicator rows from oldest gap onward
            deleted = _delete_indicators_from(conn, symbol, tf, oldest_gap_start)
            if deleted > 0:
                summary["ind_deleted"] += deleted
                logger.info(
                    f"Deleted {deleted} stale indicator rows for "
                    f"{symbol}/{tf} from {oldest_gap_start.strftime('%H:%M')}"
                )

    except Exception as e:
        logger.error(f"Gap check error {symbol}: {e}")
    finally:
        conn.close()
        session.close()

    return summary


# ── Main gap check run ────────────────────────────────────────────────────────

def run_gap_check() -> None:
    """Runs full gap check for all symbols across all timeframes."""
    symbols = load_coins()
    if not symbols:
        logger.error("No coins in coins.json — skipping gap check.")
        return

    logger.info(
        f"Gap check starting — {len(symbols)} symbols × "
        f"{len(INGEST_TIMEFRAMES)} timeframes..."
    )
    t_start = time.time()

    total_gaps   = 0
    total_filled = 0
    total_ind    = 0
    symbols_with_gaps = []

    # Stagger symbol submission to avoid burst rate limiting
    import itertools
    with ThreadPoolExecutor(max_workers=REST_WORKERS) as pool:
        futures = {}
        for i, sym in enumerate(symbols):
            if i > 0 and i % REST_WORKERS == 0:
                time.sleep(REST_WORKER_STAGGER)
            futures[pool.submit(_check_and_fill_symbol, sym)] = sym
        for future in as_completed(futures):
            result = future.result()
            if result["gaps_found"] > 0:
                total_gaps   += result["gaps_found"]
                total_filled += result["rows_filled"]
                total_ind    += result["ind_deleted"]
                symbols_with_gaps.append(result["symbol"])

    elapsed = time.time() - t_start

    if total_gaps > 0:
        logger.info(
            f"Gap check complete in {elapsed:.0f}s — "
            f"{len(symbols_with_gaps)} symbols had gaps, "
            f"{total_gaps} gaps filled ({total_filled} rows), "
            f"{total_ind} stale indicator rows deleted."
        )
        if symbols_with_gaps:
            logger.info(f"Affected symbols: {symbols_with_gaps[:20]}"
                       f"{'...' if len(symbols_with_gaps) > 20 else ''}")

        # Trigger indicator engine to recalculate affected timeframes
        # Only trigger for indicator timeframes (not 5m/15m etc.)
        from core.schema import INDICATOR_TIMEFRAMES
        affected_indicator_tfs = {
            tf for tf in INGEST_TIMEFRAMES
            if tf in INDICATOR_TIMEFRAMES
        }
        if affected_indicator_tfs and total_ind > 0:
            _trigger_indicator_recalc(affected_indicator_tfs)
    else:
        logger.info(
            f"Gap check complete in {elapsed:.0f}s — "
            f"no gaps found across {len(symbols)} symbols."
        )


# ── Scheduler ─────────────────────────────────────────────────────────────────

def _seconds_until_next_run() -> float:
    """Returns seconds until the next :20 or :50 run."""
    now = datetime.datetime.now(datetime.timezone.utc)
    candidates = []
    for minute in RUN_MINUTES:
        target = now.replace(minute=minute, second=0, microsecond=0)
        if target <= now:
            target += datetime.timedelta(hours=1)
        candidates.append(target)
    next_run = min(candidates)
    return (next_run - now).total_seconds()


def main() -> None:
    logger.info("=" * 60)
    logger.info("Gap Checker V4 — starting")
    logger.info("=" * 60)

    # Schema check
    result = verify_schema()
    if result["missing"]:
        logger.critical(f"Missing DB tables: {result['missing']}. Aborting.")
        sys.exit(1)

    # Wait for initial backfill to complete before running any gap checks.
    # The ingestion handles restart gaps itself — gap checker only supplements
    # from the first scheduled run onward.
    from core.system_state import wait_for, KEY_BACKFILL_DONE
    logger.info("Waiting for initial backfill to complete...")
    wait_for(KEY_BACKFILL_DONE, poll_interval=30, log_interval=120)
    logger.info("Initial backfill done — gap checker active.")

    shutdown = ShutdownHandler("GAP_CHECKER")

    # Run at :20 and :50 every hour
    while not shutdown.is_set():
        wait = _seconds_until_next_run()
        next_dt = datetime.datetime.now(datetime.timezone.utc) + \
                  datetime.timedelta(seconds=wait)
        logger.info(
            f"Next gap check at "
            f"{next_dt.strftime('%H:%M UTC')} "
            f"(in {wait/60:.0f} min)."
        )
        # Interruptible sleep — wakes up on Ctrl+C
        if shutdown.sleep(wait):
            break
        if not shutdown.is_set():
            run_gap_check()

    logger.info("Gap Checker stopped cleanly.")


if __name__ == "__main__":
    main()





