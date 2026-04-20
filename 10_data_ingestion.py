#!/usr/bin/env python3
# 10_data_ingestion.py
# OHLCV Data Ingestion for V4.
#
# Responsibilities:
#   1. Startup check  — verifies all required DB tables exist
#   2. Initial fill   — REST backfill per timeframe depth on first run
#   3. WebSocket fleet — streams live klines for all coins / timeframes
#                        writes to DB on every 5m candle close
#                        (all other timeframes written alongside)
#   4. Gap checker    — runs every 4h (first run at next 00/04/08/12/16/20 UTC)
#                        detects and fills holes caused by downtime
#
# Write strategy:
#   - WS buffer holds latest kline per (symbol, timeframe) in RAM
#   - On 5m candle close: all buffered timeframes for that symbol are
#     flushed to DB in a single multi-row upsert
#   - No timer-based flush — DB writes happen exactly on candle close
#
# Retention limits (enforced by 23_housekeeping.py):
#   5m, 15m          → 30 days
#   30m, 1h, 2h, 4h  → 1 year
#   6h..1d           → 3 years
#   1w, 1M           → 3 years (Binance max anyway)

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import random
import socket
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import pytz
import requests
import websockets
from psycopg2 import extras

from core.bootstrap import load_coins
from core.config import BASE_URL, INGEST_TIMEFRAMES, NUM_WORKERS
from core.database import get_db_connection
from core.schema import verify_schema

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - INGESTION - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/ingestion.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# REST backfill depth per timeframe (days). 0 = fetch maximum available.
BACKFILL_DAYS: dict[str, int] = {
    "5m":  30,  "15m": 30,
    "30m": 365, "1h":  365, "2h":  365, "4h":  365,
    "6h":  1095,"8h":  1095,"12h": 1095,
    "1d":  1095,"3d":  1095,
    "1w":  0,   "1M":  0,   # 0 = max available
}

# Gap-checker schedule: runs at these UTC hours
GAP_CHECK_HOURS = {0, 4, 8, 12, 16, 20}

# WebSocket settings
WS_STREAMS_PER_WORKER  = 860      # stay well under Binance 1024 limit
WS_STARTUP_STAGGER_SEC = 10.0     # seconds between worker starts
WS_RECONNECT_MIN_SEC   = 5.0
WS_RECONNECT_MAX_SEC   = 300.0
WS_WATCHDOG_SEC        = 120.0    # reconnect if silent for this long
WS_UNSOLICITED_PONG_SEC = 120.0

# REST catch-up
REST_WORKERS    = 3               # parallel coins during backfill/gap-check
REST_BATCH_SIZE = 1500            # max candles per Binance klines request

# ── In-memory buffer ──────────────────────────────────────────────────────────
# Structure: {symbol: {timeframe: kline_tuple}}
# kline_tuple: (symbol, open_time, open, high, low, close, volume)
_BUFFER: dict[str, dict[str, tuple]] = defaultdict(dict)

# Tracks which timeframes closed for each symbol between 5m flushes
# {symbol: {timeframe, ...}}
_CLOSED_TFS: dict[str, set[str]] = defaultdict(set)


# ── Startup check ─────────────────────────────────────────────────────────────

def check_schema() -> None:
    """Aborts if required DB tables are missing."""
    result = verify_schema()
    if result["missing"]:
        logger.critical(
            f"Required DB tables missing: {result['missing']}. "
            f"Run bootstrap first (python 00_main_watchdog.py)."
        )
        sys.exit(1)
    logger.info(f"Schema OK — {len(result['ok'])} tables verified.")


# ── DB helpers ────────────────────────────────────────────────────────────────

def _upsert_candles(conn, rows_by_tf: dict[str, list[tuple]]) -> int:
    """
    Upserts candle rows for multiple timeframes in one DB round-trip.
    rows_by_tf: {timeframe: [(symbol, open_time, o, h, l, c, v), ...]}
    Returns total number of rows written.
    """
    total = 0
    with conn.cursor() as cur:
        for tf, rows in rows_by_tf.items():
            if not rows:
                continue
            sql = f"""
                INSERT INTO ohlcv_{tf}
                    (symbol, open_time, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (symbol, open_time) DO UPDATE SET
                    open   = EXCLUDED.open,
                    high   = EXCLUDED.high,
                    low    = EXCLUDED.low,
                    close  = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """
            extras.execute_values(cur, sql, rows, page_size=500)
            total += len(rows)
    conn.commit()
    return total


def _get_latest_open_time(conn, symbol: str, tf: str) -> datetime.datetime | None:
    """Returns the latest open_time for a symbol/timeframe, or None."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT MAX(open_time) FROM ohlcv_{tf} WHERE symbol = %s",
            (symbol,),
        )
        row = cur.fetchone()
    if row and row[0]:
        ts = row[0]
        return ts if ts.tzinfo else ts.replace(tzinfo=datetime.timezone.utc)
    return None


# ── REST fetch helpers ────────────────────────────────────────────────────────

def _fetch_klines_rest(
    session: requests.Session,
    symbol: str,
    tf: str,
    start_ms: int,
    end_ms: int,
) -> list[tuple]:
    """
    Fetches OHLCV data via REST for a given symbol/timeframe/range.
    Returns list of (symbol, open_time, open, high, low, close, volume).
    Handles pagination and rate-limit back-off automatically.
    """
    url    = BASE_URL + "/fapi/v1/klines"
    rows   = []
    curr   = start_ms

    while curr < end_ms:
        params = {
            "symbol":    symbol,
            "interval":  tf,
            "startTime": curr,
            "endTime":   end_ms,
            "limit":     REST_BATCH_SIZE,
        }
        try:
            resp = session.get(url, params=params, timeout=15)
            if resp.status_code in (429, 418):
                wait = int(resp.headers.get("Retry-After", 30)) + 5
                logger.warning(f"Rate limited — sleeping {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                logger.warning(f"REST {symbol}/{tf}: HTTP {resp.status_code}")
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

            curr = data[-1][6] + 1   # close_time of last candle + 1ms
            if len(data) < REST_BATCH_SIZE:
                break
            time.sleep(0.08)         # ~12 req/s — safe under the 20/s limit

        except Exception as e:
            logger.warning(f"REST error {symbol}/{tf}: {e} — retrying in 5s")
            time.sleep(5)

    return rows


# ── Initial backfill ──────────────────────────────────────────────────────────

def _backfill_symbol(symbol: str, force: bool = False) -> None:
    """
    Backfills all INGEST_TIMEFRAMES for one symbol.
    Skips a timeframe if data already exists (unless force=True).
    """
    conn    = get_db_connection()
    session = requests.Session()
    session.headers.update({"User-Agent": "CryptoBotV4/1.0"})
    now_ms  = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)

    try:
        for tf in INGEST_TIMEFRAMES:
            latest = _get_latest_open_time(conn, symbol, tf)

            if latest and not force:
                # Already has data — skip initial backfill for this TF
                continue

            days = BACKFILL_DAYS.get(tf, 0)
            if days > 0:
                start_dt = datetime.datetime.now(datetime.timezone.utc) - \
                           datetime.timedelta(days=days)
            else:
                # Maximum available — go back 10 years (Binance will cap it)
                start_dt = datetime.datetime.now(datetime.timezone.utc) - \
                           datetime.timedelta(days=3650)

            start_ms = int(start_dt.timestamp() * 1000)
            rows = _fetch_klines_rest(session, symbol, tf, start_ms, now_ms)

            if rows:
                _upsert_candles(conn, {tf: rows})
                logger.debug(f"  Backfill {symbol}/{tf}: {len(rows)} candles")

    except Exception as e:
        logger.error(f"Backfill error {symbol}: {e}")
    finally:
        conn.close()
        session.close()


def run_initial_backfill(symbols: list[str]) -> None:
    """
    Runs initial backfill for all symbols in parallel.
    Only fills timeframes that have NO data yet.
    """
    logger.info(f"Initial backfill: checking {len(symbols)} symbols...")

    # Find symbols that actually need backfilling
    conn = get_db_connection()
    needs_fill = []
    try:
        for sym in symbols:
            # Check just 5m — if it has data we consider the symbol done
            latest = _get_latest_open_time(conn, sym, "5m")
            if latest is None:
                needs_fill.append(sym)
    finally:
        conn.close()

    if not needs_fill:
        logger.info("Initial backfill: all symbols already have data — skipped.")
        return

    logger.info(
        f"Initial backfill: {len(needs_fill)} symbols need data "
        f"({len(symbols) - len(needs_fill)} already filled)."
    )

    with ThreadPoolExecutor(max_workers=REST_WORKERS) as pool:
        list(pool.map(_backfill_symbol, needs_fill))

    logger.info("Initial backfill complete.")


# ── Gap checker ───────────────────────────────────────────────────────────────

# Timeframe duration in seconds — used to detect gaps
_TF_SECONDS: dict[str, int] = {
    "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400,
    "6h": 21600, "8h": 28800, "12h": 43200,
    "1d": 86400, "3d": 259200,
    "1w": 604800, "1M": 2592000,
}


def _fill_gaps_symbol(symbol: str) -> None:
    """Checks and fills gaps for all timeframes of one symbol."""
    conn    = get_db_connection()
    session = requests.Session()
    session.headers.update({"User-Agent": "CryptoBotV4/1.0"})
    now     = datetime.datetime.now(datetime.timezone.utc)

    try:
        for tf in INGEST_TIMEFRAMES:
            tf_sec   = _TF_SECONDS.get(tf, 3600)
            days     = BACKFILL_DAYS.get(tf, 1095)
            if days == 0:
                days = 1095

            cutoff = now - datetime.timedelta(days=days)

            with conn.cursor() as cur:
                # Count expected vs actual candles in the retention window
                cur.execute(
                    f"""
                    SELECT MIN(open_time), MAX(open_time), COUNT(*)
                    FROM ohlcv_{tf}
                    WHERE symbol = %s AND open_time >= %s
                    """,
                    (symbol, cutoff),
                )
                row = cur.fetchone()

            if not row or row[2] == 0:
                continue

            min_ot, max_ot, count = row
            if min_ot.tzinfo is None:
                min_ot = min_ot.replace(tzinfo=datetime.timezone.utc)
            if max_ot.tzinfo is None:
                max_ot = max_ot.replace(tzinfo=datetime.timezone.utc)

            expected = int(
                (max_ot - max(min_ot, cutoff)).total_seconds() / tf_sec
            ) + 1

            if count >= expected:
                continue  # No gaps

            # Gap detected — refetch from min_ot to now
            gap_pct = (expected - count) / expected * 100
            logger.info(
                f"Gap detected {symbol}/{tf}: "
                f"{expected - count} missing candles ({gap_pct:.1f}%) — refilling."
            )
            start_ms = int(min_ot.timestamp() * 1000)
            end_ms   = int(now.timestamp() * 1000)
            rows     = _fetch_klines_rest(session, symbol, tf, start_ms, end_ms)
            if rows:
                _upsert_candles(conn, {tf: rows})

    except Exception as e:
        logger.error(f"Gap check error {symbol}: {e}")
    finally:
        conn.close()
        session.close()


def run_gap_check(symbols: list[str]) -> None:
    """Runs gap detection and fill for all symbols."""
    logger.info(f"Gap check: scanning {len(symbols)} symbols...")
    start = time.time()
    with ThreadPoolExecutor(max_workers=REST_WORKERS) as pool:
        list(pool.map(_fill_gaps_symbol, symbols))
    logger.info(f"Gap check complete in {time.time() - start:.0f}s.")


async def gap_check_scheduler(symbols: list[str]) -> None:
    """
    Async loop that runs gap check at the next scheduled UTC hour
    (00, 04, 08, 12, 16, 20) and then every 4 hours.
    """
    loop = asyncio.get_running_loop()

    while True:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        # Find next scheduled hour
        next_hour = min(
            (h for h in GAP_CHECK_HOURS if h > now_utc.hour),
            default=min(GAP_CHECK_HOURS) + 24,
        )
        next_run = now_utc.replace(
            hour=next_hour % 24, minute=0, second=0, microsecond=0
        )
        if next_hour >= 24:
            next_run += datetime.timedelta(days=1)

        wait_sec = (next_run - now_utc).total_seconds()
        logger.info(
            f"Gap checker: next run at {next_run.strftime('%Y-%m-%d %H:%M')} UTC "
            f"(in {wait_sec/3600:.1f}h)."
        )
        await asyncio.sleep(wait_sec)

        # Run in thread pool so we don't block the event loop
        fresh_symbols = load_coins()
        await loop.run_in_executor(None, run_gap_check, fresh_symbols)


# ── WebSocket fleet ───────────────────────────────────────────────────────────

def _apply_tcp_keepalive(ws) -> None:
    """Sets TCP keepalive on the websocket socket to survive NAT timeouts."""
    try:
        sock = ws.transport.get_extra_info("socket")
        if sock is None:
            return
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if sys.platform == "win32":
            sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 60_000, 10_000))
        else:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6)
    except (AttributeError, OSError):
        pass


# Semaphore: limit concurrent DB flushes to avoid connection pool exhaustion.
# With 500 coins all closing at minute :00, we'd otherwise have 500 simultaneous
# DB connections. 20 concurrent flushes is plenty given pool max=20.
_FLUSH_SEMAPHORE: asyncio.Semaphore | None = None


async def _flush_symbol_async(
    symbol: str, symbol_data: dict[str, tuple], closed_tfs: set[str]
) -> None:
    """Async wrapper for _flush_symbol_to_db with concurrency control."""
    global _FLUSH_SEMAPHORE
    if _FLUSH_SEMAPHORE is None:
        _FLUSH_SEMAPHORE = asyncio.Semaphore(20)
    async with _FLUSH_SEMAPHORE:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, _flush_symbol_to_db, symbol, symbol_data, closed_tfs
        )


def _write_candle_close_event(conn, timeframe: str, symbol_count: int) -> None:
    """
    Upserts a candle close event so the indicator engine knows
    a new candle has closed for this timeframe.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO candle_close_events (timeframe, closed_at, symbol_count)
                VALUES (%s, NOW(), %s)
                ON CONFLICT (timeframe) DO UPDATE SET
                    closed_at    = EXCLUDED.closed_at,
                    symbol_count = EXCLUDED.symbol_count
                """,
                (timeframe, symbol_count),
            )
    except Exception as e:
        logger.warning(f"Could not write candle_close_event for {timeframe}: {e}")


# Tracks how many symbols closed per timeframe in the current batch
# {timeframe: count}  — reset after each flush
_CLOSE_COUNTS: dict[str, int] = {}
_CLOSE_COUNTS_LOCK = None   # set in main_async()


def _flush_symbol_to_db(symbol: str, symbol_data: dict[str, tuple], closed_tfs: set[str]) -> None:
    """
    Writes all buffered timeframes for one symbol to DB.
    Called on every 5m candle close for that symbol.
    closed_tfs: set of timeframes whose candle just closed for this symbol.
    """
    rows_by_tf: dict[str, list[tuple]] = {}
    for tf, kline in symbol_data.items():
        rows_by_tf[tf] = [kline]

    conn = get_db_connection()
    try:
        written = _upsert_candles(conn, rows_by_tf)

        # Record close events for all closed timeframes
        for tf in closed_tfs:
            _CLOSE_COUNTS[tf] = _CLOSE_COUNTS.get(tf, 0) + 1
            _write_candle_close_event(conn, tf, _CLOSE_COUNTS[tf])

        conn.commit()
        if written:
            logger.debug(f"Flushed {symbol}: {written} rows ({list(rows_by_tf.keys())})")
    except Exception as e:
        logger.error(f"DB flush error for {symbol}: {e}")
        conn.rollback()
    finally:
        conn.close()


async def ws_worker(
    worker_id: int,
    streams: list[str],
    startup_delay: float,
) -> None:
    """
    Single WebSocket worker. Connects to Binance combined stream URL,
    buffers incoming klines in RAM, and flushes to DB on every 5m close.
    """
    if startup_delay > 0:
        logger.info(f"WS worker {worker_id}: waiting {startup_delay:.0f}s (staggered start)...")
        await asyncio.sleep(startup_delay)

    url      = "wss://fstream.binance.com/stream?streams=" + "/".join(streams)
    backoff  = WS_RECONNECT_MIN_SEC
    failures = 0

    while True:
        connected_at = None
        try:
            async with websockets.connect(
                url,
                ping_interval=None,
                ping_timeout=None,
                open_timeout=30,
                close_timeout=10,
                max_size=2 ** 22,
            ) as ws:
                _apply_tcp_keepalive(ws)
                connected_at = datetime.datetime.now(datetime.timezone.utc)
                logger.info(
                    f"WS worker {worker_id} connected "
                    f"({len(streams)} streams)."
                )
                failures = 0
                backoff  = WS_RECONNECT_MIN_SEC

                # Unsolicited pong keepalive task
                async def _pong_loop():
                    while True:
                        await asyncio.sleep(WS_UNSOLICITED_PONG_SEC)
                        try:
                            await ws.pong()
                        except Exception:
                            break

                pong_task = asyncio.create_task(_pong_loop())
                last_msg  = datetime.datetime.now(datetime.timezone.utc)

                try:
                    while True:
                        try:
                            raw = await asyncio.wait_for(
                                ws.recv(), timeout=WS_WATCHDOG_SEC
                            )
                        except asyncio.TimeoutError:
                            silence = (
                                datetime.datetime.now(datetime.timezone.utc)
                                - last_msg
                            ).total_seconds()
                            logger.warning(
                                f"WS worker {worker_id}: "
                                f"{silence:.0f}s silence — reconnecting."
                            )
                            break

                        last_msg = datetime.datetime.now(datetime.timezone.utc)

                        try:
                            payload = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        # Skip subscribe responses and errors
                        if "result" in payload or "error" in payload:
                            continue

                        data = payload.get("data", {})
                        if "k" not in data:
                            continue

                        k      = data["k"]
                        sym    = k["s"]
                        tf     = k["i"]
                        is_closed = k["x"]     # True = candle just closed

                        open_time = datetime.datetime.fromtimestamp(
                            k["t"] / 1000, tz=datetime.timezone.utc
                        )
                        kline = (
                            sym, open_time,
                            float(k["o"]), float(k["h"]),
                            float(k["l"]), float(k["c"]),
                            float(k["v"]),
                        )

                        # Always update RAM buffer
                        _BUFFER[sym][tf] = kline

                        # Track which TFs closed for this symbol
                        if is_closed:
                            _CLOSED_TFS[sym].add(tf)

                        # On 5m close: flush ALL buffered TFs for this symbol.
                        # We use create_task so the flush runs concurrently
                        # but is properly tracked by the event loop.
                        if tf == "5m" and is_closed:
                            symbol_snapshot = dict(_BUFFER[sym])
                            closed_tfs = _CLOSED_TFS.pop(sym, set())
                            closed_tfs.add("5m")
                            # Schedule DB flush as a proper async task
                            asyncio.create_task(
                                _flush_symbol_async(sym, symbol_snapshot, closed_tfs)
                            )

                finally:
                    pong_task.cancel()
                    try:
                        await pong_task
                    except (asyncio.CancelledError, Exception):
                        pass

        except asyncio.CancelledError:
            logger.info(f"WS worker {worker_id}: stopped.")
            raise
        except Exception as e:
            failures += 1
            uptime = ""
            if connected_at:
                secs = (
                    datetime.datetime.now(datetime.timezone.utc) - connected_at
                ).total_seconds()
                uptime = f" (was connected {secs:.0f}s)"

            jitter   = random.uniform(0.8, 1.2)
            spread   = (worker_id - 1) * 2.0
            wait     = min(backoff * jitter, WS_RECONNECT_MAX_SEC) + spread
            logger.warning(
                f"WS worker {worker_id} disconnected{uptime}: "
                f"{type(e).__name__}: {e}. "
                f"Reconnect in {wait:.1f}s (attempt #{failures})."
            )
            await asyncio.sleep(wait)
            backoff = min(backoff * 2.0, WS_RECONNECT_MAX_SEC)


async def _fallback_flush_loop() -> None:
    """
    Fallback: every 6 minutes, flush any symbols that have buffered data
    but haven't been flushed via the 5m trigger.
    This catches edge cases where Binance doesn't send a 5m close event
    (e.g. illiquid coins, stream reconnects mid-candle).
    """
    while True:
        await asyncio.sleep(360)   # every 6 minutes
        if not _BUFFER:
            continue

        flushed = 0
        for sym, sym_data in list(_BUFFER.items()):
            if not sym_data:
                continue
            # Only flush if we have a 5m entry that looks stale
            kline_5m = sym_data.get("5m")
            if kline_5m is None:
                continue
            # Check if open_time of buffered 5m candle is more than 6 min old
            open_time = kline_5m[1]  # index 1 = open_time datetime
            age_secs = (
                datetime.datetime.now(datetime.timezone.utc) - open_time
            ).total_seconds()
            if age_secs > 360:
                snapshot   = dict(sym_data)
                closed_tfs = _CLOSED_TFS.pop(sym, set())
                asyncio.create_task(
                    _flush_symbol_async(sym, snapshot, closed_tfs)
                )
                flushed += 1

        if flushed > 0:
            logger.info(f"Fallback flush: flushed {flushed} stale symbols.")


async def start_ws_fleet(symbols: list[str]) -> None:
    """
    Splits all streams across workers and starts them with staggered delays.
    """
    all_streams = [
        f"{sym.lower()}@kline_{tf}"
        for sym in symbols
        for tf in INGEST_TIMEFRAMES
    ]

    chunks = [
        all_streams[i: i + WS_STREAMS_PER_WORKER]
        for i in range(0, len(all_streams), WS_STREAMS_PER_WORKER)
    ]

    logger.info(
        f"WS fleet: {len(chunks)} workers for {len(all_streams)} streams "
        f"(~{len(all_streams) // max(len(chunks), 1)} streams/worker)."
    )

    tasks = [
        ws_worker(i + 1, chunk, startup_delay=i * WS_STARTUP_STAGGER_SEC)
        for i, chunk in enumerate(chunks)
    ]
    # Add fallback flush as background task
    tasks.append(_fallback_flush_loop())
    await asyncio.gather(*tasks)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main_async() -> None:
    logger.info("=" * 60)
    logger.info("Data Ingestion V4 — starting")
    logger.info("=" * 60)

    # 1. Schema check
    check_schema()

    # 2. Load coin list
    symbols = load_coins()
    if not symbols:
        logger.critical("No coins found in coins.json — run bootstrap first.")
        sys.exit(1)
    logger.info(f"Loaded {len(symbols)} symbols from coins.json.")

    # 3. Initial backfill (skips symbols that already have data)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, run_initial_backfill, symbols)

    # Signal other processes that OHLCV data is ready
    from core.system_state import set_state, KEY_BACKFILL_DONE
    set_state(KEY_BACKFILL_DONE)
    logger.info("Bootstrap flag set: initial_backfill_done — indicator engine may now start.")

    # 4. Start gap checker (async scheduler)
    gap_task = asyncio.create_task(gap_check_scheduler(symbols))

    # 5. Start WebSocket fleet
    ws_task = asyncio.create_task(start_ws_fleet(symbols))

    await asyncio.gather(gap_task, ws_task)


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Data Ingestion stopped (Ctrl+C).")


if __name__ == "__main__":
    main()



