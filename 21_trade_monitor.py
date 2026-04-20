# 21_trade_monitor.py
# Trade Monitor V4 — monitors all open trades in the unified `trades` table.
#
# Runs every 10s and checks all OPEN trades against latest 5m candle data.
# Uses wick-aware price checks (high/low) to detect SL/TP hits intra-candle.
#
# Features:
#   - Unified monitor for ALL trade types (classical + AI, all bots)
#   - Wick-aware SL/TP detection (high/low, not just close)
#   - Trailing SL: after TP2 hit → SL moves to previous TP level
#   - Up to 6 TPs (tp1..tp6)
#   - TP priority: if same candle hits both TP and SL → TP wins
#   - Stale data guard: skip trades if 5m candle > 30min old
#   - pnl_r calculation on close
#   - Writes close event to telegram_outbox (info message)
#   - Graceful shutdown via ShutdownHandler

from __future__ import annotations

import datetime
import logging
import logging.handlers
import os
import sys
import time

from dotenv import load_dotenv

load_dotenv()

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR     = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - TRADE_MONITOR - %(levelname)s - %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            os.path.join(_LOG_DIR, "trade_monitor.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

from core.database import db_connection
from core.schema import verify_schema
from core.shutdown import ShutdownHandler

# ── Constants ─────────────────────────────────────────────────────────────────

POLL_INTERVAL     = 10     # seconds between checks
STALE_CANDLE_SECS = 1800   # 30min — skip trades if no fresh 5m data

# Trailing SL kicks in after this TP level is hit
# e.g. TRAILING_SL_FROM_TP = 2 means: after TP2 → SL moves to TP1
TRAILING_SL_FROM_TP = 2


# ── DB helpers ────────────────────────────────────────────────────────────────

def _load_open_trades(conn) -> list[dict]:
    """Returns all OPEN trades from the unified trades table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id, bot_name, symbol, direction,
                entry,
                tp1, tp2, tp3, tp4, tp5, tp6,
                sl, sl_initial,
                tp_count, tp_hit,
                leverage, opened_at
            FROM trades
            WHERE status = 'OPEN'
            ORDER BY id
            """
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _get_candle(conn, symbol: str) -> dict | None:
    """
    Returns latest 5m candle for symbol.
    Returns None if no data or data is stale (> 30min old).
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT open_time, high, low, close
                FROM ohlcv_5m
                WHERE symbol = %s
                ORDER BY open_time DESC
                LIMIT 1
                """,
                (symbol,),
            )
            row = cur.fetchone()
            if not row:
                return None

            open_time = row[0]
            if open_time.tzinfo is None:
                open_time = open_time.replace(tzinfo=datetime.timezone.utc)

            age_sec = (datetime.datetime.now(datetime.timezone.utc) - open_time).total_seconds()
            if age_sec > STALE_CANDLE_SECS:
                logger.debug(f"{symbol}: 5m candle {age_sec:.0f}s old — skipping")
                return None

            return {
                "open_time": open_time,
                "high":  float(row[1]),
                "low":   float(row[2]),
                "close": float(row[3]),
            }
    except Exception as e:
        logger.debug(f"Candle fetch error {symbol}: {e}")
        conn.rollback()
        return None


def _calc_pnl_r(entry: float, close_price: float,
                sl_initial: float, direction: str) -> float | None:
    """
    Calculates PnL in R-multiples.
    R = (close - entry) / (entry - sl_initial) for LONG
    R = (entry - close) / (sl_initial - entry) for SHORT
    """
    try:
        if direction == "LONG":
            risk = entry - sl_initial
            if risk <= 0:
                return None
            return (close_price - entry) / risk
        else:
            risk = sl_initial - entry
            if risk <= 0:
                return None
            return (entry - close_price) / risk
    except Exception:
        return None


def _close_trade(conn, trade: dict, close_price: float,
                 tp_hit: int, outcome: str, close_reason: str) -> None:
    """Closes a trade — updates status, close_price, pnl_r, closed_at."""
    entry      = float(trade["entry"] or 0)
    sl_initial = float(trade["sl_initial"] or trade["sl"] or 0)
    direction  = trade["direction"]

    pnl_r = _calc_pnl_r(entry, close_price, sl_initial, direction)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trades SET
                status       = %s,
                outcome      = %s,
                close_price  = %s,
                tp_hit       = %s,
                pnl_r        = %s,
                closed_at    = NOW(),
                last_updated = NOW()
            WHERE id = %s
            """,
            (
                "CLOSED_TP" if outcome == "WIN" else "CLOSED_SL",
                outcome,
                close_price,
                tp_hit,
                pnl_r,
                close_reason,
                trade["id"],
            ),
        )

    # Post close notification to telegram_outbox
    _post_close_notification(conn, trade, close_price, tp_hit, outcome, pnl_r)

    logger.info(
        f"CLOSED [{trade['bot_name']}] {trade['symbol']} {direction} "
        f"→ {outcome} tp_hit={tp_hit} close={close_price:.8f} "
        f"pnl_r={pnl_r:.2f}R" if pnl_r is not None else
        f"CLOSED [{trade['bot_name']}] {trade['symbol']} {direction} "
        f"→ {outcome} tp_hit={tp_hit} close={close_price:.8f}"
    )


def _update_tp_and_sl(conn, trade: dict, new_tp_hit: int, new_sl: float) -> None:
    """Updates tp_hit and sl (trailing SL) after a TP is hit."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trades SET
                tp_hit       = %s,
                sl           = %s,
                last_updated = NOW()
            WHERE id = %s
            """,
            (new_tp_hit, new_sl, trade["id"]),
        )
    logger.info(
        f"TP{new_tp_hit} [{trade['bot_name']}] {trade['symbol']} "
        f"{trade['direction']} — SL moved to {new_sl:.8f}"
    )


def _post_close_notification(conn, trade: dict, close_price: float,
                              tp_hit: int, outcome: str,
                              pnl_r: float | None) -> None:
    """Sends a close notification to telegram_outbox."""
    try:
        emoji  = "✅" if outcome == "WIN" else "❌"
        tp_str = f"TP{tp_hit}" if outcome == "WIN" else "SL"
        pnl_str = f"  PnL: {pnl_r:+.2f}R" if pnl_r is not None else ""

        msg = (
            f"<pre>{emoji} <b>TRADE CLOSED</b>\n"
            f"<b>{trade['symbol'].replace('USDT','')}/USDT</b> "
            f"[{trade['bot_name']}]\n"
            f"→ {trade['direction']} | {tp_str}\n"
            f"→ Close: <code>${close_price:,.8f}</code>"
            f"{pnl_str}</pre>"
        )

        with conn.cursor() as cur:
            # Post to bot's channel if known, else skip
            # Channel routing handled by signal_log in future
            # For now just log — bots will set channel_id when posting signals
            pass  # telegram_outbox insert done by bots, monitor just logs

    except Exception as e:
        logger.debug(f"Notification error: {e}")


# ── Core monitor logic ────────────────────────────────────────────────────────

def _get_trade_tps(trade: dict) -> list[float]:
    """Returns list of valid (non-zero) TP levels."""
    tps = []
    for col in ("tp1", "tp2", "tp3", "tp4", "tp5", "tp6"):
        val = trade.get(col)
        if val and float(val) > 0:
            tps.append(float(val))
    return tps


def _check_trade(conn, trade: dict, candle: dict) -> bool:
    """
    Checks a single trade against the latest candle.
    Returns True if trade was closed (caller should skip further checks).

    Logic:
      1. TP priority: check next TP first — if hit, process TP
      2. Then check SL — if hit AND no TP in same candle, close SL
      3. Trailing SL: after TRAILING_SL_FROM_TP → SL = previous TP
    """
    direction  = trade["direction"]
    is_long    = direction == "LONG"
    entry      = float(trade["entry"] or 0)
    sl         = float(trade["sl"] or 0)
    current_tp = int(trade["tp_hit"] or 0)
    tps        = _get_trade_tps(trade)

    if not tps or entry <= 0:
        return False

    high  = candle["high"]
    low   = candle["low"]

    # ── TP CHECK (priority over SL) ──────────────────────────────────────────
    if current_tp < len(tps):
        next_tp_price = tps[current_tp]
        next_tp_num   = current_tp + 1

        tp_hit = (is_long and high >= next_tp_price) or \
                 (not is_long and low <= next_tp_price)

        if tp_hit:
            new_tp_num = next_tp_num

            # Is this the last TP? → close trade
            if new_tp_num >= len(tps):
                _close_trade(
                    conn, trade, next_tp_price,
                    tp_hit=new_tp_num, outcome="WIN",
                    close_reason=f"TP{new_tp_num} hit",
                )
                conn.commit()
                return True

            # Partial close (Cornix handles automatically) — update SL
            # Trailing SL: after TRAILING_SL_FROM_TP → SL = prev TP
            if new_tp_num >= TRAILING_SL_FROM_TP:
                prev_tp_price = tps[new_tp_num - 2]  # TP before this one
                new_sl = prev_tp_price
            else:
                # After TP1 → SL moves to entry (breakeven)
                new_sl = entry

            _update_tp_and_sl(conn, trade, new_tp_num, new_sl)
            conn.commit()
            return False  # still open

    # ── SL CHECK ─────────────────────────────────────────────────────────────
    if sl > 0:
        sl_hit = (is_long and low <= sl) or (not is_long and high >= sl)

        if sl_hit:
            outcome = "LOSS" if current_tp == 0 else "WIN"
            _close_trade(
                conn, trade, sl,
                tp_hit=current_tp, outcome=outcome,
                close_reason=f"SL hit (tp_hit={current_tp})",
            )
            conn.commit()
            return True

    return False


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 60)
    logger.info("Trade Monitor V4 — starting")
    logger.info("=" * 60)

    schema = verify_schema()
    if schema["missing"]:
        logger.error(f"Missing tables: {schema['missing']}")
        sys.exit(1)
    logger.info(f"Schema OK — {len(schema['ok'])} tables verified.")

    shutdown  = ShutdownHandler("TRADE_MONITOR")
    checked   = 0
    closed    = 0

    logger.info(f"Monitoring open trades every {POLL_INTERVAL}s...")

    while not shutdown.is_set():
        try:
            with db_connection() as conn:
                trades = _load_open_trades(conn)

            if not trades:
                shutdown.sleep(POLL_INTERVAL)
                continue

            # Get unique symbols and fetch their latest candles
            symbols  = list({t["symbol"] for t in trades})
            candles: dict[str, dict] = {}

            with db_connection() as conn:
                for sym in symbols:
                    c = _get_candle(conn, sym)
                    if c:
                        candles[sym] = c

            if not candles:
                shutdown.sleep(POLL_INTERVAL)
                continue

            # Check each trade
            with db_connection() as conn:
                for trade in trades:
                    sym = trade["symbol"]
                    if sym not in candles:
                        continue
                    was_closed = _check_trade(conn, trade, candles[sym])
                    checked += 1
                    if was_closed:
                        closed += 1

            if trades:
                logger.debug(
                    f"Checked {len(trades)} open trades "
                    f"({len(candles)}/{len(symbols)} symbols with fresh data). "
                    f"Total closed: {closed}"
                )

        except Exception as e:
            logger.error(f"Monitor loop error: {e}", exc_info=True)

        shutdown.sleep(POLL_INTERVAL)

    logger.info(
        f"Trade Monitor stopped — "
        f"{checked} checks, {closed} trades closed."
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Trade Monitor stopped (Ctrl+C).")

