# 31_market_tracker.py
# Market Tracker V4 — hourly and half-hourly channel reports
#
# Posts 6 jobs to SENTIMENT_CHANNEL_ID:
#
#   XX:00:01  Signal Summary      — open/closed trades last 24h by category
#   XX:00:15  Main Volume Report  — BTC, ETH, Total Alt Market
#   XX:01:00  Gainers & Losers    — top/flop coins 1h/4h/24h
#   XX:00:30  Per-Bot Performance — WR table + Kelly sizing per bot
#   XX:00:30  Volume Spikes       — every 30min: coins with unusual volume
#   XX:00:45  Volatile Coins      — every 30min: highest range coins
#
# V4 schema changes vs V3:
#   V3: SELECT FROM "BTCUSDT_30m"           → V4: SELECT FROM ohlcv_30m WHERE symbol='BTCUSDT'
#   V3: active_trades_master / closed_trades_master / ai_signals
#                                           → V4: trades (status IN ('OPEN','CLOSED_TP','CLOSED_SL'))
#   V3: send_telegram() direct              → V4: INSERT INTO telegram_outbox

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import logging.handlers
import os
import sys
from datetime import timedelta, timezone

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR     = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - MARKET_TRACKER - %(levelname)s - %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            os.path.join(_LOG_DIR, "market_tracker.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

from core.config import SENTIMENT_CHANNEL_ID
from core.database import db_connection
from core.schema import verify_schema
from core.shutdown import ShutdownHandler

# ── Helpers ──────────────────────────────────────────────────────────────────

def _floor_hour(dt: "datetime.datetime", hours: int = 0) -> "datetime.datetime":
    """
    Returns the start of the last completed N-hour block.
    e.g. at 17:35 UTC:
      _floor_hour(now, 1)  → 17:00 UTC  (start of last completed 1h)
      _floor_hour(now, 4)  → 16:00 UTC  (start of last completed 4h block)
      _floor_hour(now, 24) → 2026-04-20 00:00 UTC
    """
    import datetime as _dt
    floored = dt.replace(minute=0, second=0, microsecond=0)
    if hours <= 1:
        return floored - _dt.timedelta(hours=1)
    # For multi-hour blocks: floor to nearest completed block
    block_start_h = (floored.hour // hours) * hours
    block_start = floored.replace(hour=block_start_h)
    if block_start >= floored:
        block_start -= _dt.timedelta(hours=hours)
    return block_start - _dt.timedelta(hours=hours * (hours - 1) // hours)


# ── Config ────────────────────────────────────────────────────────────────────

CHANNEL_ID = SENTIMENT_CHANNEL_ID

EXCLUDED_FROM_TOTAL = {
    "BTCUSDT", "XAUUSDT", "XAGUSDT", "PAXGUSDT", "BTCDOMUSDT",
}

# Coins to show in round level breaks (PD-1) — same list as before
MAIN_COINS = ["BTCUSDT", "ETHUSDT"]

TELEGRAM_TEXT_LIMIT = 4096
SAFETY_BUFFER       = 200

# Kelly constants
KELLY_LEVERAGE  = 20
KELLY_FRACTION  = 0.5
KELLY_MIN_WINS  = 10
KELLY_MIN_LOSSES = 10

# Outcome thresholds
OUTCOME_MIN_PNL_PCT     = 0.1
OUTCOME_MAX_ABS_PNL_PCT = 100.0


# ── Helpers ───────────────────────────────────────────────────────────────────

def _send(message: str) -> None:
    """Insert message into telegram_outbox."""
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO telegram_outbox (channel_id, message) VALUES (%s, %s)",
                    (CHANNEL_ID, message),
                )
            conn.commit()
    except Exception as e:
        logger.error(f"Outbox insert error: {e}")


def _fmt_money(val: float) -> str:
    if val >= 1e9: return f"${val/1e9:.2f}B"
    if val >= 1e6: return f"${val/1e6:.2f}M"
    if val >= 1e3: return f"${val/1e3:.0f}K"
    return f"${val:,.0f}"


def _load_altcoins() -> list[str]:
    try:
        coins_file = os.path.join(_SCRIPT_DIR, "coins.json")
        with open(coins_file) as f:
            data = json.load(f)
        coins = data if isinstance(data, list) else data.get("coins", [])
        return [c.upper() for c in coins
                if c.upper().endswith("USDT")
                and c.upper() not in EXCLUDED_FROM_TOTAL]
    except Exception as e:
        logger.error(f"Could not load coins.json: {e}")
        return []


def _classify_outcome(pnl_pct, close_reason: str = "") -> str:
    """Returns 'win', 'loss', or 'neutral'."""
    reason = (close_reason or "").upper()
    if any(k in reason for k in ("DELISTED", "CLEANUP", "ORPHAN", "REGIME")):
        return "neutral"
    if pd.isna(pnl_pct):
        return "neutral"
    pnl = float(pnl_pct)
    if abs(pnl) > OUTCOME_MAX_ABS_PNL_PCT or abs(pnl) <= OUTCOME_MIN_PNL_PCT:
        return "neutral"
    return "win" if pnl > 0 else "loss"


def _bot_category(strategy: str) -> str:
    """Maps strategy name to display category."""
    s = str(strategy).upper()
    if s in ("5 PERCENT", "FAST IN AND OUT", "ATS1") or s.startswith("MIS"):
        return "INDICATOR"
    if s in ("VOLUME INDICATOR", "EPD1", "PD-1"):
        return "VOLUME"
    if s in ("SUPPORT RESISTANCE", "ABR1", "RUB1", "SRA1"):
        return "LEVEL"
    if (s in ("AIM1", "ATB1") or s.startswith("BR") or s.startswith("TD")
            or s.startswith("BB") or s.startswith("QM") or s.startswith("SMC")):
        return "PATTERN"
    return "OTHER"


def _normalize_bot_name(name: str) -> str:
    """Normalizes bot names for display — e.g. MIS-1-168H."""
    s = str(name).strip()
    # Unify MIS variants
    import re
    m = re.match(r"(?:MIS1?|MSI1?)[-_\s]?(\d+)[Hh](?:_?(pump|dump))?", s, re.IGNORECASE)
    if m:
        hours = m.group(1)
        direction = f"_{m.group(2).lower()}" if m.group(2) else ""
        return f"MIS-1-{hours}h{direction}"
    return s


def _build_chunks(blocks: list[str], header_first: str,
                  header_cont: str, footer: str) -> list[str]:
    """Splits bot blocks into Telegram-safe chunks (<4096 chars)."""
    chunks, body, size = [], [], len(header_first) + len(footer)
    hdr = header_first
    for block in blocks:
        needed = len(block) + (2 if body else 0)
        if size + needed > TELEGRAM_TEXT_LIMIT - SAFETY_BUFFER and body:
            chunks.append(hdr + "\n\n".join(body) + footer)
            hdr, body, size = header_cont, [block], len(header_cont) + len(footer) + len(block)
        else:
            body.append(block)
            size += needed
    if body:
        chunks.append(hdr + "\n\n".join(body) + footer)
    return chunks


# ── Job 1: Main Volume Report ─────────────────────────────────────────────────

async def job_main_reports() -> None:
    logger.info("Running Main Volume Report...")
    now  = datetime.datetime.now(timezone.utc)
    # Use floored hour boundaries — reports always cover completed periods
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    t1h  = hour_start - timedelta(hours=1)   # start of last completed hour

    def _report(symbol: str, label: str, symbols: list[str]) -> None:
        try:
            sym_list = symbols if symbols else [symbol]
            placeholders = ",".join(["%s"] * len(sym_list))

            with db_connection() as conn:
                with conn.cursor() as cur:
                    # Price change over completed periods ending at hour_start
                    changes = {}
                    for hours, col in [(1,"1h"),(4,"4h"),(24,"24h"),(168,"7d"),(720,"30d")]:
                        # Get price at START of window (oldest candle in window)
                        cur.execute(
                            """
                            SELECT close FROM ohlcv_30m
                            WHERE symbol = ANY(%s::text[])
                              AND open_time >= %s
                              AND open_time < %s
                            ORDER BY open_time ASC LIMIT 1
                            """,
                            (sym_list, hour_start - timedelta(hours=hours), hour_start),
                        )
                        row = cur.fetchone()
                        # Get current price (latest candle)
                        cur.execute(
                            """
                            SELECT close FROM ohlcv_30m
                            WHERE symbol = ANY(%s::text[])
                            ORDER BY open_time DESC LIMIT 1
                            """,
                            (sym_list,),
                        )
                        curr = cur.fetchone()
                        if row and row[0] and curr and curr[0] and float(row[0]) > 0:
                            chg = (float(curr[0]) / float(row[0]) - 1) * 100
                            changes[col] = chg
                        else:
                            changes[col] = None

                    # Volume 1h
                    cur.execute(
                        """
                        SELECT
                            COALESCE(SUM(volume * (open + close) / 2), 0) as usd_vol,
                            COALESCE(SUM(CASE WHEN close >= open THEN volume*(open+close)/2 ELSE 0 END), 0) as buy_vol,
                            COALESCE(SUM(CASE WHEN close < open  THEN volume*(open+close)/2 ELSE 0 END), 0) as sell_vol
                        FROM ohlcv_30m
                        WHERE symbol = ANY(%s::text[])
                          AND open_time >= %s
                        """,
                        (sym_list, t1h),
                    )
                    vrow = cur.fetchone()
                    usd_vol  = float(vrow[0]) if vrow else 0
                    buy_vol  = float(vrow[1]) if vrow else 0
                    sell_vol = float(vrow[2]) if vrow else 0
                    buy_pct  = (buy_vol / usd_vol * 100) if usd_vol > 0 else 0
                    sell_pct = 100 - buy_pct

            def _fmt_chg(v):
                if v is None: return "---"
                return f"{'🟢' if v >= 0 else '🔴'} {v:+.2f}%"

            html = (
                f"<pre><b>📊 {label}</b>\n"
                f"<b>Price changes:</b>\n"
                f"1h: {_fmt_chg(changes.get('1h'))}  │  "
                f"4h: {_fmt_chg(changes.get('4h'))}\n"
                f"24h: {_fmt_chg(changes.get('24h'))}  │  "
                f"7d: {_fmt_chg(changes.get('7d'))}\n"
                f"<b>Volume 1h:</b> {_fmt_money(usd_vol)}\n"
                f"🟢 Buy: {_fmt_money(buy_vol)} ({buy_pct:.1f}%)  "
                f"🔴 Sell: {_fmt_money(sell_vol)} ({sell_pct:.1f}%)\n"
                f"<b>Time:</b> {now.strftime('%H:%M')} UTC</pre>"
            )
            _send(html)
        except Exception as e:
            logger.error(f"Main report error for {label}: {e}")

    altcoins = _load_altcoins()
    _report("BTCUSDT",   "BTC/USDT",        ["BTCUSDT"])
    await asyncio.sleep(1)
    _report("ETHUSDT",   "ETH/USDT",        ["ETHUSDT"])
    await asyncio.sleep(1)
    _report("TOTAL",     "TOTAL ALT MARKET", altcoins)
    logger.info("Main Volume Report done.")


# ── Job 2: Gainers & Losers ───────────────────────────────────────────────────

async def job_gainers_losers() -> None:
    logger.info("Running Gainers & Losers...")
    now = datetime.datetime.now(timezone.utc)
    # Snap to last completed hour boundary
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    altcoins = _load_altcoins()
    if not altcoins:
        return

    stats = []
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                for sym in altcoins:
                    try:
                        cur.execute(
                            """
                            SELECT open_time, close FROM ohlcv_30m
                            WHERE symbol = %s AND open_time >= %s AND open_time < %s
                            ORDER BY open_time ASC
                            """,
                            (sym, hour_start - timedelta(hours=24), hour_start),
                        )
                        rows = cur.fetchall()
                        if not rows:
                            continue
                        df = pd.DataFrame(rows, columns=["ot", "c"])
                        df["c"] = pd.to_numeric(df["c"], errors="coerce")
                        curr_p = float(df["c"].iloc[-1])
                        if curr_p <= 0:
                            continue

                        def _chg(hours):
                            cutoff = hour_start - timedelta(hours=hours)
                            sub = df[df["ot"] >= cutoff]
                            if sub.empty: return None
                            p0 = float(sub["c"].iloc[0])
                            return (curr_p / p0 - 1) * 100 if p0 > 0 else None

                        c1 = _chg(1); c4 = _chg(4); c24 = _chg(24)
                        if c1 is None: continue
                        stats.append({"sym": sym.replace("USDT",""), "1h": c1, "4h": c4, "24h": c24})
                    except Exception:
                        pass
    except Exception as e:
        logger.error(f"Gainers/Losers DB error: {e}")
        return

    if not stats:
        return

    df_s = pd.DataFrame(stats).dropna(subset=["1h"])

    def _build(tf: str, is_gain: bool, n: int = 10) -> str:
        col = tf
        sorted_df = df_s.sort_values(col, ascending=not is_gain).head(n)
        lines = []
        for _, row in sorted_df.iterrows():
            v = row[col]
            lines.append(f"  {row['sym']:<14} {v:+.2f}%")
        return "\n".join(lines)

    msg_g = (
        f"<pre><b>🏆 TOP GAINERS — {now.strftime('%H:%M')} UTC</b>\n\n"
        f"<b>1h Top 10:</b>\n{_build('1h', True)}\n\n"
        f"<b>4h Top 10:</b>\n{_build('4h', True)}\n\n"
        f"<b>24h Top 10:</b>\n{_build('24h', True)}</pre>"
    )
    msg_l = (
        f"<pre><b>💀 TOP LOSERS — {now.strftime('%H:%M')} UTC</b>\n\n"
        f"<b>1h Top 10:</b>\n{_build('1h', False)}\n\n"
        f"<b>4h Top 10:</b>\n{_build('4h', False)}\n\n"
        f"<b>24h Top 10:</b>\n{_build('24h', False)}</pre>"
    )
    _send(msg_g)
    await asyncio.sleep(1)
    _send(msg_l)
    logger.info("Gainers & Losers done.")


# ── Job 3: Volume Spikes ──────────────────────────────────────────────────────

async def job_volume_spikes() -> None:
    logger.info("Running Volume Spikes...")
    now = datetime.datetime.now(timezone.utc)
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    t1h = hour_start - timedelta(hours=1)   # last completed hour
    t4h = hour_start - timedelta(hours=4)   # 4h baseline
    altcoins = _load_altcoins()
    if not altcoins:
        return

    spikes = []
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                for sym in altcoins:
                    try:
                        # Volume last hour vs 4h baseline
                        cur.execute(
                            "SELECT SUM(volume), AVG(close) FROM ohlcv_30m WHERE symbol=%s AND open_time>=%s",
                            (sym, t1h),
                        )
                        r1 = cur.fetchone()
                        cur.execute(
                            "SELECT SUM(volume) FROM ohlcv_30m WHERE symbol=%s AND open_time>=%s AND open_time<%s",
                            (sym, t4h, t1h),
                        )
                        r4 = cur.fetchone()
                        if not r1 or not r1[0] or not r4 or not r4[0]:
                            continue
                        vol_1h = float(r1[0])
                        price  = float(r1[1]) if r1[1] else 0
                        # avg per hour from 4h baseline (3 hours)
                        avg_1h = float(r4[0]) / 3.0
                        if avg_1h <= 0 or price <= 0:
                            continue
                        ratio = vol_1h / avg_1h
                        usd_vol = vol_1h * price
                        if ratio >= 3.0 and usd_vol >= 100_000:
                            spikes.append({"sym": sym.replace("USDT",""), "ratio": ratio, "usd": usd_vol})
                    except Exception:
                        pass
    except Exception as e:
        logger.error(f"Volume spikes DB error: {e}")
        return

    if not spikes:
        logger.info("No volume spikes found.")
        return

    spikes.sort(key=lambda x: x["ratio"], reverse=True)
    lines = [f"  {s['sym']:<14} {s['ratio']:.1f}× avg  ({_fmt_money(s['usd'])})" for s in spikes[:15]]
    msg = (
        f"<pre><b>🌊 VOLUME SPIKES — {now.strftime('%H:%M')} UTC</b>\n"
        f"<i>1h volume vs 4h avg (≥3×, ≥$100K)</i>\n\n"
        + "\n".join(lines)
        + "</pre>"
    )
    _send(msg)
    logger.info(f"Volume Spikes: {len(spikes)} coins.")


# ── Job 4: Volatile Coins ─────────────────────────────────────────────────────

async def job_volatile_coins() -> None:
    logger.info("Running Volatile Coins...")
    now = datetime.datetime.now(timezone.utc)
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    t4h = hour_start - timedelta(hours=4)   # last completed 4h window
    altcoins = _load_altcoins()
    if not altcoins:
        return

    volatiles = []
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                for sym in altcoins:
                    try:
                        cur.execute(
                            """
                            SELECT MAX(high), MIN(low), (
                                SELECT close FROM ohlcv_30m
                                WHERE symbol=%s ORDER BY open_time DESC LIMIT 1
                            )
                            FROM ohlcv_30m WHERE symbol=%s AND open_time>=%s
                            """,
                            (sym, sym, t4h),
                        )
                        row = cur.fetchone()
                        if not row or not row[0] or not row[1] or not row[2]:
                            continue
                        hi, lo, price = float(row[0]), float(row[1]), float(row[2])
                        if lo <= 0 or price <= 0:
                            continue
                        range_pct = (hi - lo) / lo * 100
                        volatiles.append({"sym": sym.replace("USDT",""), "range": range_pct, "price": price})
                    except Exception:
                        pass
    except Exception as e:
        logger.error(f"Volatile coins DB error: {e}")
        return

    if not volatiles:
        return

    volatiles.sort(key=lambda x: x["range"], reverse=True)
    lines = [f"  {v['sym']:<14} {v['range']:.2f}% range" for v in volatiles[:15]]
    msg = (
        f"<pre><b>⚡ VOLATILE COINS — {now.strftime('%H:%M')} UTC</b>\n"
        f"<i>4h high-low range</i>\n\n"
        + "\n".join(lines)
        + "</pre>"
    )
    _send(msg)
    logger.info(f"Volatile Coins done: {len(volatiles)} coins.")


# ── Job 5: Signal Summary ─────────────────────────────────────────────────────

async def job_signal_summary() -> None:
    logger.info("Running Signal Summary...")
    now = datetime.datetime.now(timezone.utc)
    t24 = now - timedelta(hours=24)

    try:
        with db_connection() as conn:
            df = pd.read_sql_query(
                """
                SELECT
                    bot_name        AS strategy,
                    direction,
                    entry           AS entry,
                    close_price,
                    opened_at       AS created_at,
                    closed_at,
                    status,
                    close_reason,
                    pnl_r
                FROM trades
                WHERE opened_at >= %s OR closed_at >= %s
                """,
                conn,
                params=(t24, t24),
            )
    except Exception as e:
        logger.error(f"Signal summary DB error: {e}")
        return

    if df.empty:
        logger.info("No trades in last 24h — signal summary skipped.")
        return

    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["closed_at"]  = pd.to_datetime(df["closed_at"],  utc=True, errors="coerce")
    df["entry"]      = pd.to_numeric(df["entry"],       errors="coerce")
    df["close_price"]= pd.to_numeric(df["close_price"], errors="coerce")

    # Compute PnL and outcome
    is_short = df["direction"] == "SHORT"
    raw_pct   = (df["close_price"] - df["entry"]) / df["entry"] * 100
    df["pnl_pct"] = raw_pct.where(~is_short, -raw_pct)
    df.loc[df["status"] == "OPEN", "pnl_pct"] = pd.NA

    df["outcome"]  = df.apply(
        lambda r: _classify_outcome(r["pnl_pct"], r.get("close_reason","") or ""),
        axis=1,
    )
    df["is_win"]   = df["outcome"] == "win"
    df["is_closed"]= df["status"] != "OPEN"
    df["category"] = df["strategy"].apply(_bot_category)

    def _calc(cat: str) -> str:
        sub = df[df["category"] == cat]
        if sub.empty:
            return "(no data)"

        def _o(h):
            s = sub[sub["created_at"] >= now - timedelta(hours=h)]
            l = (s["direction"]=="LONG").sum()
            sh= (s["direction"]=="SHORT").sum()
            r = "∞" if sh==0 and l>0 else "0.0" if sh==0 else f"{l/sh:.1f}"
            return l, sh, r

        def _c(h):
            s = sub[sub["is_closed"] & (sub["closed_at"] >= now - timedelta(hours=h))]
            ls = s[s["direction"]=="LONG"]
            ss = s[s["direction"]=="SHORT"]
            ld = ls[ls["outcome"].isin(["win","loss"])]
            sd = ss[ss["outcome"].isin(["win","loss"])]
            lw = f"{ld['is_win'].mean()*100:.1f}" if len(ld)>0 else "---"
            sw = f"{sd['is_win'].mean()*100:.1f}" if len(sd)>0 else "---"
            return lw, sw

        o1l,o1s,o1r = _o(1); o4l,o4s,o4r = _o(4); o24l,o24s,o24r = _o(24)
        c1l,c1s = _c(1); c4l,c4s = _c(4); c24l,c24s = _c(24)

        return (
            f"<b>Opened:</b>\n"
            f"1h : 🟢 {o1l}L / 🔴 {o1s}S  (Ratio: {o1r})\n"
            f"4h : 🟢 {o4l}L / 🔴 {o4s}S  (Ratio: {o4r})\n"
            f"24h: 🟢 {o24l}L / 🔴 {o24s}S  (Ratio: {o24r})\n"
            f"<b>Closed (WR L/S):</b>\n"
            f"1h : {c1l}% / {c1s}%\n"
            f"4h : {c4l}% / {c4s}%\n"
            f"24h: {c24l}% / {c24s}%"
        )

    msg = (
        f"<pre>📊 <b>BOT SIGNAL SUMMARY</b> — {now.strftime('%H:%M')} UTC\n\n"
        f"⚙️ <b>INDICATOR BASED</b>\n{_calc('INDICATOR')}\n"
        f"{'─'*24}\n"
        f"🌊 <b>VOLUME BASED</b>\n{_calc('VOLUME')}\n"
        f"{'─'*24}\n"
        f"🧱 <b>LEVEL BASED</b>\n{_calc('LEVEL')}\n"
        f"{'─'*24}\n"
        f"📐 <b>PATTERN BASED</b>\n{_calc('PATTERN')}\n"
        f"</pre>"
    )
    _send(msg)
    logger.info("Signal Summary done.")


# ── Job 6: Per-Bot Performance ────────────────────────────────────────────────

async def job_per_bot_performance() -> None:
    logger.info("Running Per-Bot Performance...")
    now = datetime.datetime.now(timezone.utc)

    try:
        with db_connection() as conn:
            df = pd.read_sql_query(
                """
                SELECT
                    bot_name        AS strategy,
                    direction,
                    entry           AS entry,
                    close_price,
                    opened_at       AS created_at,
                    closed_at,
                    status,
                    close_reason,
                    tp_hit
                FROM trades
                WHERE entry IS NOT NULL
                """,
                conn,
            )
    except Exception as e:
        logger.error(f"Per-bot performance DB error: {e}")
        return

    if df.empty:
        logger.info("No trades — per-bot performance skipped.")
        return

    df["created_at"]  = pd.to_datetime(df["created_at"],  utc=True, errors="coerce")
    df["closed_at"]   = pd.to_datetime(df["closed_at"],   utc=True, errors="coerce")
    df["entry"]       = pd.to_numeric(df["entry"],        errors="coerce")
    df["close_price"] = pd.to_numeric(df["close_price"],  errors="coerce")
    df = df.dropna(subset=["entry", "created_at"])
    df = df[df["entry"] > 0]

    is_short = df["direction"] == "SHORT"
    raw_pct   = (df["close_price"] - df["entry"]) / df["entry"] * 100
    df["pnl_pct"] = raw_pct.where(~is_short, -raw_pct)
    df["is_closed"]  = df["status"] != "OPEN"
    df.loc[~df["is_closed"], "pnl_pct"] = pd.NA

    df["outcome"] = df.apply(
        lambda r: _classify_outcome(r["pnl_pct"], r.get("close_reason","") or "")
        if r["is_closed"] else "",
        axis=1,
    )
    df["is_win"]  = df["outcome"] == "win"
    df["is_loss"] = df["outcome"] == "loss"

    # Normalize bot names
    df["strategy_short"] = df["strategy"].apply(_normalize_bot_name)

    # Map tp_hit to status_num (0=SL, 1-6=TP)
    df["status_num"] = pd.to_numeric(df["tp_hit"], errors="coerce").fillna(0).astype(int)
    df.loc[df["status"] == "CLOSED_SL", "status_num"] = 0

    WINDOWS = [
        ("1h",  timedelta(hours=1)),
        ("4h",  timedelta(hours=4)),
        ("24h", timedelta(hours=24)),
        ("7d",  timedelta(days=7)),
        ("All", None),
    ]
    MIN_TRADES = 3

    def _compute_kelly(sub: pd.DataFrame) -> dict:
        closed = sub[sub["is_closed"]]
        wins   = closed[closed["outcome"] == "win"]["pnl_pct"].dropna()
        losses = closed[closed["outcome"] == "loss"]["pnl_pct"].dropna()
        if len(wins) < KELLY_MIN_WINS or len(losses) < KELLY_MIN_LOSSES:
            return {"status": "insufficient_data"}
        avg_win  = float(wins.mean())
        avg_loss = abs(float(losses.mean()))
        if avg_loss < 0.01 or avg_win <= 0:
            return {"status": "insufficient_data"}
        b = avg_win / avg_loss
        p = len(wins) / (len(wins) + len(losses))
        kelly_f = (b * p - (1-p)) / b
        if kelly_f <= 0:
            return {"status": "neg_edge"}
        hk  = kelly_f * KELLY_FRACTION * 100
        return {
            "status": "ok",
            "half_kelly_pct": hk,
            "margin_safe_pct": hk / KELLY_LEVERAGE,
            "margin_pure_pct": (hk * 100) / (KELLY_LEVERAGE * avg_loss),
            "avg_win": avg_win, "avg_loss": avg_loss,
            "n_wins": len(wins), "n_losses": len(losses),
        }

    rows = {}
    for strategy in sorted(df["strategy_short"].unique()):
        sub_full = df[df["strategy_short"] == strategy]
        stats = {"total": len(sub_full)}

        for win_name, delta in WINDOWS:
            sub_w = sub_full if delta is None else sub_full[sub_full["created_at"] >= now - delta]
            sub_c = sub_w[sub_w["is_closed"]]
            sub_d = sub_c[sub_c["outcome"].isin(["win","loss"])]
            if len(sub_d) < MIN_TRADES:
                stats[win_name] = ("---", None, len(sub_c))
            else:
                wr = sub_d["is_win"].sum() / len(sub_d) * 100
                stats[win_name] = (f"{wr:.0f}%", wr, len(sub_c))

        sub_all_c = sub_full[sub_full["is_closed"]]
        sub_all_d = sub_all_c[sub_all_c["outcome"].isin(["win","loss"])]
        stats["avg_pnl_all"]     = float(sub_all_d["pnl_pct"].mean()) if len(sub_all_d) > 0 else None
        stats["n_closed_total"]  = len(sub_all_c)
        stats["n_decisive_total"]= len(sub_all_d)

        # 4h detail
        sub4 = sub_full[sub_full["created_at"] >= now - timedelta(hours=4)]
        sub4c = sub4[sub4["is_closed"]]; sub4o = sub4[~sub4["is_closed"]]
        if len(sub4c) > 0:
            sv = sub4c["status_num"].astype(int)
            long_d  = sub4c[(sub4c["direction"]=="LONG")  & sub4c["outcome"].isin(["win","loss"])]
            short_d = sub4c[(sub4c["direction"]=="SHORT") & sub4c["outcome"].isin(["win","loss"])]
            stats["detail_4h"] = {
                "opened": len(sub4), "closed": len(sub4c), "open": len(sub4o),
                "tp1_plus": int((sv>=1).sum()), "tp2_plus": int((sv>=2).sum()),
                "tp3_plus": int((sv>=3).sum()), "sl": int((sv==0).sum()),
                "long_n": len(long_d), "long_wins": int(long_d["is_win"].sum()),
                "short_n": len(short_d), "short_wins": int(short_d["is_win"].sum()),
            }
        else:
            stats["detail_4h"] = {"opened": len(sub4), "closed": 0, "open": len(sub4o)}

        stats["kelly"] = _compute_kelly(sub_full)
        rows[strategy] = stats

    sorted_strats = sorted(rows.items(), key=lambda kv: kv[1]["n_closed_total"], reverse=True)

    def _trend(c1, call):
        w1 = c1[1]; wa = call[1]
        if w1 is None or wa is None: return " "
        return "↑" if w1 - wa >= 10 else "↓" if w1 - wa <= -10 else " "

    # Build performance table
    hdr = "Bot          │ 1h    │ 4h   │ 24h  │ 7d   │ All"
    sep = "─" * len(hdr)
    lines = [hdr, sep]

    for strategy, stats in sorted_strats:
        trend  = _trend(stats["1h"], stats["All"])
        pnl    = stats.get("avg_pnl_all")
        n      = stats["n_closed_total"]
        pnl_s  = f"n={n}, {pnl:+.2f}%" if pnl is not None else f"n={n}"
        lines.append(
            f"{strategy:<12} │ {stats['1h'][0]:>4}{trend} │ "
            f"{stats['4h'][0]:>4} │ {stats['24h'][0]:>4} │ "
            f"{stats['7d'][0]:>4} │ {stats['All'][0]:>4}   ({pnl_s})"
        )
        d = stats.get("detail_4h", {})
        if d.get("opened", 0) > 0 and d.get("closed", 0) > 0:
            lines.append(f"  4h: {d['opened']} opened → {d['closed']} closed, {d['open']} open")
            lines.append(f"    TP1+:{d['tp1_plus']} TP2+:{d['tp2_plus']} TP3+:{d['tp3_plus']} SL:{d['sl']}")
            parts = []
            if d["long_n"]  > 0: parts.append(f"LONG: {d['long_wins']}/{d['long_n']} win")
            if d["short_n"] > 0: parts.append(f"SHORT: {d['short_wins']}/{d['short_n']} win")
            if parts: lines.append(f"    {'  |  '.join(parts)}")
            lines.append("")
        elif d.get("opened", 0) > 0:
            lines.append(f"  4h: {d['opened']} opened, {d['open']} open")
            lines.append("")

    # Build Kelly block
    kelly_lines = []
    for strategy, stats in sorted_strats:
        k = stats.get("kelly", {})
        kelly_lines.append(f"<b>{strategy}</b>")
        st = k.get("status")
        if st == "insufficient_data":
            kelly_lines.append("  --- insufficient data (need ≥10 wins & losses)")
        elif st == "neg_edge":
            kelly_lines.append("  ⛔ NEGATIVE EDGE — do not trade")
        elif st == "ok":
            kelly_lines.append(f"  Half-Kelly:   {k['half_kelly_pct']:>5.1f}% of account")
            kelly_lines.append(f"  Safe Margin:  {k['margin_safe_pct']:>5.2f}%")
            kelly_lines.append(f"  Pure Margin:  {k['margin_pure_pct']:>5.1f}%")
        else:
            kelly_lines.append("  ---")
        kelly_lines.append("")

    # Split and send performance table
    def _group_blocks(src: list[str]) -> list[str]:
        blocks, cur = [], []
        for ln in src:
            if ln == "":
                if cur: blocks.append("\n".join(cur)); cur = []
            else: cur.append(ln)
        if cur: blocks.append("\n".join(cur))
        return blocks

    table_hdr  = lines[:2]
    table_col  = "\n".join(table_hdr) + "\n"
    tblocks    = _group_blocks(lines[2:])
    kblocks    = _group_blocks(kelly_lines)

    t_header_f = f"<pre>📊 <b>PER-BOT PERFORMANCE</b> 📊\n\n{table_col}"
    t_header_c = f"<pre>📊 <b>PER-BOT PERFORMANCE</b> (continued) 📊\n\n{table_col}"
    t_footer   = "\n\n<b>Legend:</b>\n  ↑ 1h WR ≥10pp above All  ↓ below\n  --- = <3 trades in window</pre>"

    k_header_f = "<pre>💰 <b>HALF-KELLY SIZING</b> 💰\n<i>20x Cross, Half-Kelly (all-time)</i>\n\n"
    k_header_c = "<pre>💰 <b>HALF-KELLY SIZING</b> (continued) 💰\n\n"
    k_footer   = "\n\n<i>⚠ With N parallel correlated trades: margin ÷ N</i></pre>"

    for chunk in _build_chunks(tblocks, t_header_f, t_header_c, t_footer):
        _send(chunk)
        await asyncio.sleep(1)

    for chunk in _build_chunks(kblocks, k_header_f, k_header_c, k_footer):
        _send(chunk)
        await asyncio.sleep(1)

    logger.info(f"Per-Bot Performance done ({len(sorted_strats)} strategies).")


# ── Scheduler ─────────────────────────────────────────────────────────────────

async def _schedule(minutes: list[int], second: int, job_func, name: str,
                    shutdown: ShutdownHandler) -> None:
    """Runs job_func at the specified minutes:second of every hour."""
    logger.debug(f"Scheduled '{name}' at min={minutes} sec={second}")
    while not shutdown.is_set():
        now      = datetime.datetime.now(timezone.utc)
        next_run = None

        for m in minutes:
            cand = now.replace(minute=m, second=second, microsecond=0)
            if cand > now and (next_run is None or cand < next_run):
                next_run = cand

        if next_run is None:
            next_run = (now + timedelta(hours=1)).replace(
                minute=minutes[0], second=second, microsecond=0
            )

        sleep_s = (next_run - now).total_seconds()
        logger.debug(f"'{name}' sleeping {sleep_s:.0f}s until {next_run.strftime('%H:%M:%S')} UTC")

        # Interruptible sleep
        await asyncio.sleep(min(sleep_s, 30))
        remaining = (next_run - datetime.datetime.now(timezone.utc)).total_seconds()
        if remaining > 0:
            await asyncio.sleep(remaining)

        if shutdown.is_set():
            break

        try:
            await job_func()
        except Exception as e:
            logger.error(f"Job '{name}' error: {e}", exc_info=True)

        await asyncio.sleep(1)  # prevent double-firing


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    logger.info("=" * 60)
    logger.info("Market Tracker V4 — starting")
    logger.info("=" * 60)

    schema = verify_schema()
    if schema["missing"]:
        logger.error(f"Missing tables: {schema['missing']}")
        sys.exit(1)
    logger.info(f"Schema OK — {len(schema['ok'])} tables verified.")

    shutdown = ShutdownHandler("MARKET_TRACKER")
    logger.info("Market Tracker active — waiting for scheduled jobs...")

    await asyncio.gather(
        # XX:00:01  Signal Summary
        _schedule([0],     1,  job_signal_summary,     "Signal_Summary",    shutdown),
        # XX:00:15  Main Volume Report
        _schedule([0],    15,  job_main_reports,       "Main_Volume_Report",shutdown),
        # XX:01:00  Gainers & Losers
        _schedule([1],     0,  job_gainers_losers,     "Gainers_Losers",    shutdown),
        # XX:00:30 + XX:00:30  Per-Bot Performance
        _schedule([0],    30,  job_per_bot_performance,"Per_Bot_Performance",shutdown),
        # XX:00:30 & XX:30:30  Volume Spikes
        _schedule([0,30], 30,  job_volume_spikes,      "Volume_Spikes",     shutdown),
        # XX:00:45 & XX:30:45  Volatile Coins
        _schedule([0,30], 45,  job_volatile_coins,     "Volatile_Coins",    shutdown),
    )

    logger.info("Market Tracker stopped cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Market Tracker stopped (Ctrl+C).")


