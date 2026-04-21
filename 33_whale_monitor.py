# 33_whale_monitor.py
# Whale Trade Monitor V4
#
# Connects to Binance Futures aggTrade WebSocket streams for all coins.
# Records trades >= MIN_USD_VALUE ($25k) to whale_trades DB table.
#
# Reports (every 30 min, synced to :00:05 and :30:05 UTC):
#   - BTC / ETH: Long/Short count + volume + L/S ratio (1h, prev 1h, 4h, 24h)
#   - TOP20: same stats
#   - Top 5 altcoins by whale long volume (ex BTC/ETH)
#   - Top 5 altcoins by whale short volume (ex BTC/ETH)
#   - Top 5 individual largest trades (long + short)
#
# Performance vs V3:
#   - DB instead of JSON files — no global list growing unboundedly
#   - All stats calculated via SQL aggregates (no Python loops over large lists)
#   - Single WS fleet, same architecture as 10_data_ingestion.py
#   - Retention enforced by housekeeping (nightly DELETE WHERE ts < NOW()-3d)
#
# WS architecture:
#   - Binance limit: 1024 streams per connection
#   - 573 coins = 1 connection (573 < 1024)
#   - URL-encoded combined stream (no SUBSCRIBE message needed)
#   - Reconnect with exponential backoff

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import logging.handlers
import os
import sys
import time
from collections import defaultdict

import websockets
from dotenv import load_dotenv

load_dotenv()

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR     = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - WHALE - %(levelname)s - %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            os.path.join(_LOG_DIR, "whale_monitor.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

from core.config import MARKET_MONITOR_CHANNEL_ID
from core.database import get_db_connection
from core.schema import verify_schema
from core.shutdown import ShutdownHandler

# ── Constants ─────────────────────────────────────────────────────────────────

MIN_USD_VALUE     = 25_000.0        # min notional to record as whale trade
RETENTION_DAYS    = 3
STREAMS_PER_WS    = 600             # stay well below Binance 1024 limit
WS_BASE           = "wss://fstream.binance.com/stream?streams="

# TOP20 for summary blocks (same as funding monitor)
TOP20 = [
    "BTCUSDT", "ETHUSDT", "XRPUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "DOGEUSDT", "ADAUSDT", "BCHUSDT", "HYPEUSDT",
    "LINKUSDT", "ZECUSDT", "XLMUSDT", "LTCUSDT", "HBARUSDT",
    "AVAXUSDT", "SUIUSDT", "UNIUSDT", "TONUSDT", "DOTUSDT",
]
TOP20_SET = set(TOP20)

# ── In-memory write buffer ────────────────────────────────────────────────────
# Trades accumulate here and are flushed to DB every FLUSH_INTERVAL_S.
# This decouples the high-frequency WS handler from DB writes.
FLUSH_INTERVAL_S  = 15          # flush buffer to DB every 15 seconds
_write_buffer: list[tuple]  = []
_buffer_lock  = asyncio.Lock()


# ── DB helpers ────────────────────────────────────────────────────────────────

async def _flush_buffer() -> None:
    """Flushes accumulated trades to DB. Called every FLUSH_INTERVAL_S."""
    async with _buffer_lock:
        if not _write_buffer:
            return
        batch = list(_write_buffer)
        _write_buffer.clear()

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Use execute_values for fast bulk insert
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                """
                INSERT INTO whale_trades (symbol, ts, direction, usd_value, price, qty)
                VALUES %s
                """,
                batch,
                template="(%s, %s, %s, %s, %s, %s)",
            )
        conn.commit()
        logger.debug(f"Flushed {len(batch)} whale trades to DB.")
    except Exception as e:
        logger.error(f"DB flush error: {e}")
        conn.rollback()
        # Put trades back in buffer on failure
        async with _buffer_lock:
            _write_buffer.extend(batch)
    finally:
        conn.close()


def _get_stats(window_start: datetime.datetime,
               window_end: datetime.datetime,
               symbols: list[str] | None = None,
               exclude: list[str] | None = None) -> dict:
    """
    Returns aggregated stats for a time window.
    SQL aggregate — no Python loops over raw data.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            filters = ["ts >= %s", "ts < %s"]
            params: list = [window_start, window_end]

            if symbols:
                filters.append("symbol = ANY(%s)")
                params.append(symbols)
            if exclude:
                filters.append("symbol != ALL(%s)")
                params.append(exclude)

            where = " AND ".join(filters)
            cur.execute(
                f"""
                SELECT
                    direction,
                    COUNT(*)       AS cnt,
                    SUM(usd_value) AS vol
                FROM whale_trades
                WHERE {where}
                GROUP BY direction
                """,
                params,
            )
            rows = cur.fetchall()

        result = {"long_cnt": 0, "long_vol": 0.0,
                  "short_cnt": 0, "short_vol": 0.0}
        for direction, cnt, vol in rows:
            if direction == "LONG":
                result["long_cnt"]  = int(cnt)
                result["long_vol"]  = float(vol or 0)
            else:
                result["short_cnt"] = int(cnt)
                result["short_vol"] = float(vol or 0)
        return result
    except Exception as e:
        logger.debug(f"Stats query error: {e}")
        return {"long_cnt": 0, "long_vol": 0.0,
                "short_cnt": 0, "short_vol": 0.0}
    finally:
        conn.close()


def _get_top_coins(window_start: datetime.datetime,
                    window_end: datetime.datetime,
                    direction: str,
                    exclude: list[str],
                    limit: int = 5) -> list[tuple[str, int, float]]:
    """Returns top coins by whale volume for a direction."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, COUNT(*) AS cnt, SUM(usd_value) AS vol
                FROM whale_trades
                WHERE ts >= %s AND ts < %s
                  AND direction = %s
                  AND symbol != ALL(%s)
                GROUP BY symbol
                ORDER BY vol DESC
                LIMIT %s
                """,
                (window_start, window_end, direction, exclude, limit),
            )
            return [(row[0], int(row[1]), float(row[2])) for row in cur.fetchall()]
    except Exception as e:
        logger.debug(f"Top coins query error: {e}")
        return []
    finally:
        conn.close()


def _get_top_trades(window_start: datetime.datetime,
                     window_end: datetime.datetime,
                     direction: str,
                     limit: int = 5) -> list[tuple[str, float, float, datetime.datetime]]:
    """Returns largest individual trades."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, usd_value, price, ts
                FROM whale_trades
                WHERE ts >= %s AND ts < %s
                  AND direction = %s
                ORDER BY usd_value DESC
                LIMIT %s
                """,
                (window_start, window_end, direction, limit),
            )
            return [(row[0], float(row[1]), float(row[2]), row[3])
                    for row in cur.fetchall()]
    except Exception as e:
        logger.debug(f"Top trades query error: {e}")
        return []
    finally:
        conn.close()


def _send_alert(message: str) -> None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO telegram_outbox (channel_id, message) VALUES (%s, %s)",
                (MARKET_MONITOR_CHANNEL_ID, message),
            )
        conn.commit()
    except Exception as e:
        logger.error(f"Alert send error: {e}")
    finally:
        conn.close()


# ── Formatting ────────────────────────────────────────────────────────────────

def _fmt_usd(val: float) -> str:
    sign = "-" if val < 0 else ""
    v    = abs(val)
    if v >= 1_000_000:
        return f"{sign}${v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"{sign}${v/1_000:.0f}K"
    return f"{sign}${v:.0f}"


def _ratio(long_vol: float, short_vol: float) -> str:
    if short_vol == 0:
        return "∞" if long_vol > 0 else "0.0"
    return f"{long_vol/short_vol:.2f}"


def _asset_block(name: str,
                  now: datetime.datetime,
                  symbols: list[str] | None = None) -> str:
    """Builds the summary block for one asset group."""
    h1  = now - datetime.timedelta(hours=1)
    h2  = now - datetime.timedelta(hours=2)
    h4  = now - datetime.timedelta(hours=4)
    h24 = now - datetime.timedelta(hours=24)

    s1   = _get_stats(h1,  now,  symbols)
    sp1  = _get_stats(h2,  h1,   symbols)   # prev 1h
    s4   = _get_stats(h4,  now,  symbols)
    s24  = _get_stats(h24, now,  symbols)

    return (
        f"<b>{name}</b>\n"
        f"🟢 L: {s1['long_cnt']} ({_fmt_usd(s1['long_vol'])})\n"
        f"🔴 S: {s1['short_cnt']} ({_fmt_usd(s1['short_vol'])})\n"
        f"⚖️ Ratio: <b>{_ratio(s1['long_vol'], s1['short_vol'])}</b>\n"
        f"⏳ Prev: 1h:{_ratio(sp1['long_vol'], sp1['short_vol'])} "
        f"4h:{_ratio(s4['long_vol'], s4['short_vol'])} "
        f"24h:{_ratio(s24['long_vol'], s24['short_vol'])}"
    )


def _build_overview() -> str:
    now  = datetime.datetime.now(datetime.timezone.utc)
    h1   = now - datetime.timedelta(hours=1)
    excl = ["BTCUSDT", "ETHUSDT"]

    btc_block   = _asset_block("BTC Trades",  now, ["BTCUSDT"])
    eth_block   = _asset_block("ETH Trades",  now, ["ETHUSDT"])
    top20_block = _asset_block("TOP20 Coins", now, list(TOP20_SET))

    # Top 5 altcoins by volume (ex BTC/ETH)
    top_longs  = _get_top_coins(h1, now, "LONG",  excl)
    top_shorts = _get_top_coins(h1, now, "SHORT", excl)

    long_lines  = "\n".join(
        f"{s:<10} {c:>2}x {_fmt_usd(v):>8}" for s, c, v in top_longs
    ) or "  —"
    short_lines = "\n".join(
        f"{s:<10} {c:>2}x {_fmt_usd(v):>8}" for s, c, v in top_shorts
    ) or "  —"

    # Top 5 individual trades
    big_longs  = _get_top_trades(h1, now, "LONG")
    big_shorts = _get_top_trades(h1, now, "SHORT")

    def _trade_line(sym, usd, prc, ts):
        t = ts.astimezone(datetime.timezone.utc).strftime("%H:%M")
        return f"{sym:<9} {_fmt_usd(usd):>7} @ {prc:.4f} ({t})"

    bl_lines = "\n".join(_trade_line(*t) for t in big_longs)  or "  —"
    bs_lines = "\n".join(_trade_line(*t) for t in big_shorts) or "  —"

    ts_str = now.strftime("%H:%M UTC")
    return (
        f"<pre>🐋 <b>WHALE ACTIVITY UPDATE</b>  {ts_str}\n\n"
        f"{btc_block}\n\n"
        f"{eth_block}\n\n"
        f"{top20_block}\n\n"
        f"{'─'*31}\n"
        f"<b>Top 5 Altcoin Longs (1h, ex BTC/ETH):</b>\n"
        f"{long_lines}\n\n"
        f"<b>Top 5 Altcoin Shorts (1h, ex BTC/ETH):</b>\n"
        f"{short_lines}\n\n"
        f"{'─'*31}\n"
        f"<b>Top 5 Whale Trades Long (1h):</b>\n"
        f"{bl_lines}\n\n"
        f"<b>Top 5 Whale Trades Short (1h):</b>\n"
        f"{bs_lines}</pre>"
    )


# ── Next trigger ──────────────────────────────────────────────────────────────

def _next_overview_trigger() -> datetime.datetime:
    """Returns next :00:05 or :30:05 UTC."""
    now = datetime.datetime.now(datetime.timezone.utc)
    if now.minute < 30 or (now.minute == 30 and now.second < 5):
        target = now.replace(minute=30, second=5, microsecond=0)
    else:
        target = (now + datetime.timedelta(hours=1)).replace(
            minute=0, second=5, microsecond=0)
    return target


# ── WebSocket listener ────────────────────────────────────────────────────────

def _load_coins() -> list[str]:
    try:
        with open(os.path.join(_SCRIPT_DIR, "coins.json")) as f:
            data = json.load(f)
        coins = data if isinstance(data, list) else data.get("coins", [])
        return sorted({c.upper() for c in coins if c.upper().endswith("USDT")})
    except Exception as e:
        logger.error(f"Could not load coins.json: {e}")
        return []


async def _ws_listener(coins: list[str], shutdown: ShutdownHandler) -> None:
    """
    Listens to aggTrade streams for all coins.
    Filters trades >= MIN_USD_VALUE and appends to write buffer.
    Reconnects with exponential backoff on disconnect.
    """
    streams  = [f"{c.lower()}@aggTrade" for c in coins]
    # Split into chunks if needed (> STREAMS_PER_WS)
    chunks   = [streams[i:i+STREAMS_PER_WS]
                 for i in range(0, len(streams), STREAMS_PER_WS)]
    backoff  = 5.0

    logger.info(
        f"Starting whale WS listener: {len(coins)} coins "
        f"across {len(chunks)} connection(s)."
    )

    # For simplicity: single connection (573 < 600 limit)
    # If coin list grows > 600, add second connection here.
    url = WS_BASE + "/".join(streams)

    while not shutdown.is_set():
        try:
            async with websockets.connect(
                url,
                ping_interval=None,
                ping_timeout=None,
                open_timeout=30,
                max_size=2**22,
            ) as ws:
                logger.info(f"🟢 Whale WS connected ({len(streams)} streams)")
                backoff = 5.0

                # Unsolicited pong keepalive every 120s
                async def _pong_task():
                    while True:
                        await asyncio.sleep(120)
                        try:
                            await ws.pong()
                        except Exception:
                            break

                pong = asyncio.create_task(_pong_task())
                try:
                    while not shutdown.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=45)
                        except asyncio.TimeoutError:
                            try:
                                await ws.pong()
                            except Exception:
                                break
                            continue

                        try:
                            payload = json.loads(raw)
                        except Exception:
                            continue

                        if "data" not in payload:
                            continue
                        d = payload["data"]
                        if d.get("e") != "aggTrade":
                            continue

                        qty   = float(d["q"])
                        price = float(d["p"])
                        usd   = qty * price
                        if usd < MIN_USD_VALUE:
                            continue

                        direction = "SHORT" if d["m"] else "LONG"
                        ts = datetime.datetime.fromtimestamp(
                            d["T"] / 1000, tz=datetime.timezone.utc
                        )
                        async with _buffer_lock:
                            _write_buffer.append((
                                d["s"], ts, direction,
                                round(usd, 2), price, qty,
                            ))
                finally:
                    pong.cancel()
                    try:
                        await pong
                    except (asyncio.CancelledError, Exception):
                        pass

        except Exception as e:
            if shutdown.is_set():
                break
            logger.warning(
                f"🔴 Whale WS disconnected ({e}) — "
                f"reconnecting in {backoff:.0f}s..."
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)

    logger.info("Whale WS listener stopped.")


# ── Flush + overview background tasks ────────────────────────────────────────

async def _flush_task(shutdown: ShutdownHandler) -> None:
    while not shutdown.is_set():
        await asyncio.sleep(FLUSH_INTERVAL_S)
        try:
            await _flush_buffer()
        except Exception as e:
            logger.error(f"Flush task error: {e}")


async def _overview_task(shutdown: ShutdownHandler) -> None:
    next_t = _next_overview_trigger()
    logger.info(f"First whale overview at {next_t.strftime('%H:%M:%S')} UTC")

    while not shutdown.is_set():
        now = datetime.datetime.now(datetime.timezone.utc)
        if now >= next_t:
            try:
                msg = _build_overview()
                _send_alert(msg)
                logger.info("Whale overview sent.")
            except Exception as e:
                logger.error(f"Overview error: {e}")
            next_t = _next_overview_trigger()
            logger.info(f"Next whale overview at {next_t.strftime('%H:%M:%S')} UTC")
        await asyncio.sleep(10)


# ── Main ──────────────────────────────────────────────────────────────────────

async def _run(shutdown: ShutdownHandler) -> None:
    coins = _load_coins()
    if not coins:
        logger.error("No coins found — aborting.")
        return

    logger.info(f"Monitoring {len(coins)} coins for whale trades >= ${MIN_USD_VALUE/1000:.0f}k")

    await asyncio.gather(
        _ws_listener(coins, shutdown),
        _flush_task(shutdown),
        _overview_task(shutdown),
    )


def main() -> None:
    logger.info("=" * 60)
    logger.info("Whale Monitor V4 — starting")
    logger.info("=" * 60)

    schema = verify_schema()
    if schema["missing"]:
        logger.error(f"Missing tables: {schema['missing']}")
        sys.exit(1)
    logger.info(f"Schema OK — {len(schema['ok'])} tables verified.")

    shutdown = ShutdownHandler("WHALE")
    asyncio.run(_run(shutdown))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Whale Monitor stopped (Ctrl+C).")
