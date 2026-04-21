# 32_funding_monitor.py
# Funding Rate Monitor V4
#
# Polls Binance fapi/v1/premiumIndex every 5 minutes for all coins.
# Writes rates to funding_rates DB table (replaces V3 JSON files).
#
# Reports (every 30 min, synced to :00:10 and :30:10 UTC):
#   - BTC / ETH rates with 1h + 24h delta (in basis points)
#   - TOP20 sentiment: % coins positive now vs 1h ago
#   - Top 5 most positive / most negative rates
#
# Extreme alerts (whenever polled, 15-min cooldown):
#   - If >=75% / >=85% / >=95% of TOP20 are positive or negative
#
# Performance vs V3:
#   - DB instead of JSON files — no rebuild_symbol_index(), no file I/O
#   - Historical lookup via SQL (indexed) instead of bisect on RAM list
#   - Single aiohttp session reused across all polls
#   - asyncio throughout — no blocking calls

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import logging.handlers
import os
import sys
import time

import aiohttp
from dotenv import load_dotenv

load_dotenv()

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR     = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - FUNDING - %(levelname)s - %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            os.path.join(_LOG_DIR, "funding_monitor.log"),
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

POLL_INTERVAL_S   = 300        # 5 minutes
BINANCE_URL       = "https://fapi.binance.com/fapi/v1/premiumIndex"
RETENTION_DAYS    = 3

# Extreme alert thresholds + cooldown
ALERT_THRESHOLDS  = [95, 85, 75]   # % of TOP20 positive or negative
ALERT_COOLDOWN_S  = 900            # 15 minutes between alerts

# TOP20 coins for sentiment tracking
TOP20 = [
    "BTCUSDT", "ETHUSDT", "XRPUSDT", "BNBUSDT", "SOLUSDT",
    "TRXUSDT", "DOGEUSDT", "ADAUSDT", "BCHUSDT", "HYPEUSDT",
    "LINKUSDT", "ZECUSDT", "XLMUSDT", "LTCUSDT", "HBARUSDT",
    "AVAXUSDT", "SUIUSDT", "UNIUSDT", "TONUSDT", "DOTUSDT",
]
TOP20_SET = set(TOP20)

_last_alert_ts: float = 0.0


# ── DB helpers ────────────────────────────────────────────────────────────────

def _save_rates(rates: list[tuple[str, float, datetime.datetime]]) -> None:
    """Bulk-inserts funding rates. ON CONFLICT DO NOTHING — idempotent."""
    if not rates:
        return
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # executemany is fine for 573 rows — fast enough
            cur.executemany(
                """
                INSERT INTO funding_rates (symbol, ts, rate)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol, ts) DO NOTHING
                """,
                rates,
            )
        conn.commit()
    except Exception as e:
        logger.error(f"DB save error: {e}")
        conn.rollback()
    finally:
        conn.close()


def _purge_old_rates() -> None:
    """Removes rates older than RETENTION_DAYS. Called once per day by housekeeping
    but also run on startup to keep table lean."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM funding_rates WHERE ts < NOW() - INTERVAL '%s days'",
                (RETENTION_DAYS,),
            )
            deleted = cur.rowcount
        conn.commit()
        if deleted:
            logger.info(f"Purged {deleted} old funding rate rows.")
    except Exception as e:
        logger.warning(f"Purge error: {e}")
        conn.rollback()
    finally:
        conn.close()


def _get_historical_rates(symbols: list[str],
                           target_ts: datetime.datetime) -> dict[str, float]:
    """
    Returns {symbol: rate} for each symbol closest to target_ts (±10 min).
    Single SQL query — much faster than V3 bisect per symbol.
    """
    if not symbols:
        return {}
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (symbol)
                    symbol, rate
                FROM funding_rates
                WHERE symbol = ANY(%s)
                  AND ts BETWEEN %s AND %s
                ORDER BY symbol, ABS(EXTRACT(EPOCH FROM (ts - %s)))
                """,
                (
                    symbols,
                    target_ts - datetime.timedelta(minutes=10),
                    target_ts + datetime.timedelta(minutes=10),
                    target_ts,
                ),
            )
            return {row[0]: float(row[1]) for row in cur.fetchall()}
    except Exception as e:
        logger.debug(f"Historical rates error: {e}")
        return {}
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

def _fmt_rate(rate: float) -> str:
    """Formats rate as percentage string: 0.0001 → '+0.0100%'"""
    return f"{rate * 100:+.4f}%"


def _fmt_bps(current: float, historical: float | None) -> str:
    """Difference in basis points: 1 bps = 0.0001 = 0.01%"""
    if historical is None:
        return "N/A"
    bps = (current - historical) * 10_000
    return f"{bps:+.1f}bps"


# ── Extreme alert ─────────────────────────────────────────────────────────────

def _check_extreme_alert(current_rates: dict[str, float],
                          now_ts: float) -> None:
    global _last_alert_ts
    if now_ts - _last_alert_ts < ALERT_COOLDOWN_S:
        return

    top20_rates = {s: r for s, r in current_rates.items() if s in TOP20_SET}
    total = len(top20_rates)
    if total == 0:
        return

    pos_count = sum(1 for r in top20_rates.values() if r > 0)
    pos_pct   = pos_count / total * 100
    neg_pct   = 100 - pos_pct

    triggered   = False
    direction   = ""
    pct_display = 0.0

    for thresh in ALERT_THRESHOLDS:
        if pos_pct >= thresh:
            triggered, direction, pct_display = True, "POSITIVE", pos_pct
            break
        if neg_pct >= thresh:
            triggered, direction, pct_display = True, "NEGATIVE", neg_pct
            break

    if not triggered:
        return

    emoji = "🟢" if direction == "POSITIVE" else "🔴"
    rates_lines = "\n".join(
        f"{s:<12} {_fmt_rate(top20_rates[s])}"
        for s in TOP20 if s in top20_rates
    )
    msg = (
        f"<pre>🚨 <b>FUNDING EXTREME ALERT</b> 🚨\n"
        f"{emoji} <b>{pct_display:.0f}% of TOP20 are {direction}</b>\n\n"
        f"<b>Current Rates TOP20:</b>\n"
        f"{rates_lines}</pre>"
    )
    _send_alert(msg)
    _last_alert_ts = now_ts
    logger.info(f"Extreme alert fired: {pct_display:.0f}% {direction}")


# ── 30-min overview ───────────────────────────────────────────────────────────

def _build_overview(current_rates: dict[str, float]) -> str:
    now_utc  = datetime.datetime.now(datetime.timezone.utc)
    hist_1h  = _get_historical_rates(list(current_rates.keys()),
                                     now_utc - datetime.timedelta(hours=1))
    hist_24h = _get_historical_rates(["BTCUSDT", "ETHUSDT"],
                                     now_utc - datetime.timedelta(hours=24))

    btc = current_rates.get("BTCUSDT", 0.0)
    eth = current_rates.get("ETHUSDT", 0.0)

    btc_1h  = _fmt_bps(btc, hist_1h.get("BTCUSDT"))
    btc_24h = _fmt_bps(btc, hist_24h.get("BTCUSDT"))
    eth_1h  = _fmt_bps(eth, hist_1h.get("ETHUSDT"))
    eth_24h = _fmt_bps(eth, hist_24h.get("ETHUSDT"))

    # TOP20 sentiment now vs 1h ago
    top20_now  = {s: r for s, r in current_rates.items() if s in TOP20_SET}
    top20_1hago = {s: r for s, r in hist_1h.items() if s in TOP20_SET}

    def _pos_pct(rates_dict: dict) -> str:
        t = len(rates_dict)
        if t == 0:
            return "N/A"
        p = sum(1 for r in rates_dict.values() if r > 0)
        return f"{p/t*100:.1f}%"

    # Top 5 positive / negative across all coins
    sorted_rates = sorted(current_rates.items(), key=lambda x: x[1])
    top5_neg = sorted_rates[:5]
    top5_pos = list(reversed(sorted_rates[-5:]))

    neg_lines = "\n".join(f"{s:<12} {_fmt_rate(r)}" for s, r in top5_neg)
    pos_lines = "\n".join(f"{s:<12} {_fmt_rate(r)}" for s, r in top5_pos)

    ts_str = now_utc.strftime("%H:%M UTC")
    return (
        f"<pre>📊 <b>FUNDING OVERVIEW</b>  {ts_str}\n\n"
        f"<b>BTC</b>  {_fmt_rate(btc)}  (1h:{btc_1h} 24h:{btc_24h})\n"
        f"<b>ETH</b>  {_fmt_rate(eth)}  (1h:{eth_1h} 24h:{eth_24h})\n\n"
        f"<b>TOP20:</b> {_pos_pct(top20_now)} positive  "
        f"(1h ago: {_pos_pct(top20_1hago)})\n\n"
        f"🔴 <b>Top 5 Negative:</b>\n{neg_lines}\n\n"
        f"🟢 <b>Top 5 Positive:</b>\n{pos_lines}</pre>"
    )


# ── Next trigger time ─────────────────────────────────────────────────────────

def _next_overview_trigger() -> datetime.datetime:
    """Returns next :00:10 or :30:10 UTC."""
    now = datetime.datetime.now(datetime.timezone.utc)
    if now.minute < 30 or (now.minute == 30 and now.second < 10):
        target = now.replace(minute=30, second=10, microsecond=0)
    else:
        target = (now + datetime.timedelta(hours=1)).replace(
            minute=0, second=10, microsecond=0)
    return target


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


async def _run(shutdown: ShutdownHandler) -> None:
    coins = _load_coins()
    if not coins:
        logger.error("No coins found — aborting.")
        return

    valid_coins = set(coins)
    logger.info(f"Tracking {len(coins)} coins.")

    _purge_old_rates()

    next_overview = _next_overview_trigger()
    logger.info(f"First overview at {next_overview.strftime('%H:%M:%S')} UTC")

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=20)
    ) as session:
        while not shutdown.is_set():
            poll_start = time.monotonic()

            try:
                async with session.get(BINANCE_URL) as resp:
                    if resp.status != 200:
                        logger.warning(f"Binance API HTTP {resp.status}")
                        await asyncio.sleep(60)
                        continue
                    api_data = await resp.json()

            except asyncio.TimeoutError:
                logger.warning("Binance API timeout — retrying in 60s")
                await asyncio.sleep(60)
                continue
            except Exception as e:
                logger.error(f"Fetch error: {e}")
                await asyncio.sleep(60)
                continue

            now_utc = datetime.datetime.now(datetime.timezone.utc)
            now_ts  = time.time()

            # Build rates dict + rows for DB
            current_rates: dict[str, float] = {}
            db_rows: list[tuple] = []
            for item in api_data:
                sym = item.get("symbol", "")
                if sym not in valid_coins:
                    continue
                rate = float(item["lastFundingRate"])
                current_rates[sym] = rate
                db_rows.append((sym, now_utc, rate))

            # Save to DB (runs in thread pool to avoid blocking event loop)
            await asyncio.get_event_loop().run_in_executor(
                None, _save_rates, db_rows
            )
            logger.debug(f"Saved {len(db_rows)} funding rates.")

            # Extreme alert check
            _check_extreme_alert(current_rates, now_ts)

            # 30-min overview
            if now_utc >= next_overview:
                try:
                    msg = _build_overview(current_rates)
                    _send_alert(msg)
                    logger.info("Funding overview sent.")
                except Exception as e:
                    logger.error(f"Overview error: {e}")
                next_overview = _next_overview_trigger()
                logger.info(f"Next overview at {next_overview.strftime('%H:%M:%S')} UTC")

            # Sleep for remainder of poll interval
            elapsed = time.monotonic() - poll_start
            sleep_s = max(0, POLL_INTERVAL_S - elapsed)
            if shutdown.sleep(sleep_s):
                break

    logger.info("Funding monitor stopped.")


def main() -> None:
    logger.info("=" * 60)
    logger.info("Funding Monitor V4 — starting")
    logger.info("=" * 60)

    schema = verify_schema()
    if schema["missing"]:
        logger.error(f"Missing tables: {schema['missing']}")
        sys.exit(1)
    logger.info(f"Schema OK — {len(schema['ok'])} tables verified.")

    shutdown = ShutdownHandler("FUNDING")
    asyncio.run(_run(shutdown))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Funding Monitor stopped (Ctrl+C).")
