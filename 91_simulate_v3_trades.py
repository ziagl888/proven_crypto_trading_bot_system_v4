#!/usr/bin/env python3
# 91_simulate_v3_trades.py
# Post-migration trade simulation for all V3 closed trades.
#
# Replays each closed trade using 5m candles (wick-aware) to verify
# and correct outcome, close_price, pnl_r, tp_hit.
#
# Data source priority:
#   1. Local ohlcv_5m table (fast, no API calls)
#   2. Binance REST API (for data older than 30 days)
#
# Logic per candle:
#   - High >= current_tp  → TP hit (partial close, SL trails)
#   - Low  <= current_sl  → SL hit (close remaining position)
#   - TP has priority over SL in same candle
#
# Trailing SL rules:
#   - TP1 hit → SL stays (no trail on first TP)
#   - TP2 hit → SL moves to TP1
#   - TP3 hit → SL moves to TP2
#   - etc.
#
# Usage:
#   python 91_simulate_v3_trades.py --dry-run   # show what would change
#   python 91_simulate_v3_trades.py             # execute simulation
#   python 91_simulate_v3_trades.py --limit 100 # test on first 100 trades

from __future__ import annotations

import argparse
import datetime
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd

from core.database import db_connection, get_db_connection
from core.config import BASE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - SIMULATE - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

import warnings
warnings.filterwarnings("ignore")

# ── Constants ─────────────────────────────────────────────────────────────────
REST_LIMIT   = 1500
REST_SLEEP   = 0.15    # 150ms between requests — ~6 req/s
WORKERS      = 3


# ── Data structures ───────────────────────────────────────────────────────────

class TradeToSimulate:
    """Holds all data needed to simulate one trade."""
    __slots__ = ('id','symbol','direction','entry','sl','tps',
                 'opened_at','closed_at','current_status')
    def __init__(self, id, symbol, direction, entry, sl, tps,
                 opened_at, closed_at, current_status='OPEN'):
        self.id = id; self.symbol = symbol; self.direction = direction
        self.entry = entry; self.sl = sl; self.tps = tps
        self.opened_at = opened_at; self.closed_at = closed_at
        self.current_status = current_status


class SimResult:
    """Simulation result for one trade."""
    __slots__ = ('trade_id','status','outcome','tp_hit','close_price',
                 'pnl_r','weighted_pnl_r','position_pct','note')
    def __init__(self, trade_id, status, outcome, tp_hit, close_price,
                 pnl_r, weighted_pnl_r, position_pct, note=""):
        self.trade_id = trade_id; self.status = status
        self.outcome = outcome; self.tp_hit = tp_hit
        self.close_price = close_price; self.pnl_r = pnl_r
        self.weighted_pnl_r = weighted_pnl_r
        self.position_pct = position_pct; self.note = note


# ── OHLCV fetching ────────────────────────────────────────────────────────────

def _fetch_5m_local(conn, symbol: str,
                    start: datetime.datetime,
                    end: datetime.datetime) -> pd.DataFrame:
    """Fetches 5m candles from local DB."""
    try:
        df = pd.read_sql(
            """
            SELECT open_time, high, low, close
            FROM ohlcv_5m
            WHERE symbol = %s
              AND open_time >= %s
              AND open_time <= %s
            ORDER BY open_time ASC
            """,
            conn,
            params=(symbol, start, end),
        )
        if not df.empty:
            df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        return df
    except Exception:
        return pd.DataFrame()


def _fetch_5m_binance(symbol: str,
                      start: datetime.datetime,
                      end: datetime.datetime) -> pd.DataFrame:
    """Fetches 5m candles from Binance REST API."""
    url    = BASE_URL + "/fapi/v1/klines"
    rows   = []
    curr   = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    session = requests.Session()
    session.headers.update({"User-Agent": "CryptoBotV4-Simulator/1.0"})

    while curr < end_ms:
        params = {
            "symbol":    symbol,
            "interval":  "5m",
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
                rows.append({
                    "open_time": open_time,
                    "high":  float(k[2]),
                    "low":   float(k[3]),
                    "close": float(k[4]),
                })
            curr = data[-1][6] + 1
            if len(data) < REST_LIMIT:
                break
            time.sleep(REST_SLEEP)
        except Exception as e:
            logger.warning(f"Binance fetch error {symbol}: {e}")
            time.sleep(5)

    session.close()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _get_candles(conn, symbol: str,
                 start: datetime.datetime,
                 end: datetime.datetime) -> pd.DataFrame:
    """
    Returns 5m candles for simulation window.
    Tries local DB first, falls back to Binance REST.
    """
    # Add buffer: 1 candle before and after
    start_buf = start - datetime.timedelta(minutes=5)
    end_buf   = end   + datetime.timedelta(minutes=5)

    # Try local first
    df = _fetch_5m_local(conn, symbol, start_buf, end_buf)

    if not df.empty:
        # Check coverage — need at least 80% of expected candles
        expected = int((end_buf - start_buf).total_seconds() / 300)
        if len(df) >= expected * 0.8:
            return df

    # Fall back to Binance REST
    logger.debug(f"  Local data insufficient for {symbol} — fetching from Binance")
    return _fetch_5m_binance(symbol, start_buf, end_buf)


# ── Trade simulation ──────────────────────────────────────────────────────────

def _simulate_trade(trade: TradeToSimulate,
                    candles: pd.DataFrame) -> SimResult:
    """
    Replays a trade candle by candle using 5m OHLCV data.

    Rules:
    - TP has priority over SL in the same candle
    - Trailing SL: TP1 hit → no trail, TP2 hit → SL→TP1, TP3→TP2, etc.
    - Partial close: each TP closes 1/N of original position
    - Returns the final SimResult
    """
    tps      = trade.tps
    n_tps    = len(tps)
    is_long  = trade.direction.upper() == "LONG"

    if not tps:
        # No targets — can only close via SL
        tps    = []
        n_tps  = 0

    current_sl   = trade.sl
    tp_hit       = 0
    position_pct = 1.0
    close_frac   = 1.0 / n_tps if n_tps > 0 else 1.0

    # Candles from trade open to close (or end of data)
    mask = candles["open_time"] >= trade.opened_at
    if trade.closed_at:
        # Add 24h buffer after V3 close time (in case V3 timestamp was wrong)
        mask &= candles["open_time"] <= (
            trade.closed_at + datetime.timedelta(hours=24)
        )
    df = candles[mask].copy()

    if df.empty:
        return SimResult(
            trade_id=trade.id, status="CLOSED_MANUAL",
            outcome=None, tp_hit=0, close_price=None,
            pnl_r=None, weighted_pnl_r=None, position_pct=1.0,
            note="no_candle_data",
        )

    # Track partial PnL for weighted_pnl_r
    partial_pnls: list[tuple[float, float]] = []  # (pnl_r, fraction_closed)

    def calc_r(close_p: float) -> float:
        risk = abs(trade.entry - trade.sl)
        if risk == 0:
            return 0.0
        if is_long:
            return (close_p - trade.entry) / risk
        else:
            return (trade.entry - close_p) / risk

    for _, candle in df.iterrows():
        high  = float(candle["high"])
        low   = float(candle["low"])
        close = float(candle["close"])

        # Check next TP
        if tp_hit < n_tps:
            next_tp    = tps[tp_hit]
            tp_reached = high >= next_tp if is_long else low <= next_tp
        else:
            tp_reached = False

        # Check SL
        sl_hit = low <= current_sl if is_long else high >= current_sl

        # TP has priority
        if tp_reached and tp_hit < n_tps:
            tp_price  = tps[tp_hit]
            tp_hit   += 1
            fraction  = close_frac
            position_pct -= fraction
            partial_pnls.append((calc_r(tp_price), fraction))

            # Trailing SL: start trailing after TP1
            if tp_hit >= 2:
                current_sl = tps[tp_hit - 2]   # SL → previous TP

            # All TPs hit
            if tp_hit == n_tps:
                w_pnl = sum(r * f for r, f in partial_pnls)
                return SimResult(
                    trade_id=trade.id, status="CLOSED_TP",
                    outcome="WIN", tp_hit=tp_hit,
                    close_price=tps[-1],
                    pnl_r=round(calc_r(tps[-1]), 4),
                    weighted_pnl_r=round(w_pnl, 4),
                    position_pct=0.0,
                )
            continue

        if sl_hit:
            sl_price = current_sl
            # Close remaining position at SL
            if partial_pnls:
                # Already hit some TPs → trailing SL close is a WIN overall
                # if weighted_pnl_r > 0
                partial_pnls.append((calc_r(sl_price), position_pct))
                w_pnl   = sum(r * f for r, f in partial_pnls)
                outcome = "WIN" if w_pnl > 0 else ("LOSS" if w_pnl < 0 else "BREAKEVEN")
                return SimResult(
                    trade_id=trade.id, status="CLOSED_SL",
                    outcome=outcome, tp_hit=tp_hit,
                    close_price=sl_price,
                    pnl_r=round(calc_r(sl_price), 4),
                    weighted_pnl_r=round(w_pnl, 4),
                    position_pct=0.0,
                )
            else:
                return SimResult(
                    trade_id=trade.id, status="CLOSED_SL",
                    outcome="LOSS", tp_hit=0,
                    close_price=sl_price,
                    pnl_r=round(calc_r(sl_price), 4),
                    weighted_pnl_r=round(calc_r(sl_price), 4),
                    position_pct=0.0,
                )

    # End of candle data
    last_close = float(df["close"].iloc[-1])

    # If this was an OPEN trade and we reached NOW without hitting SL/TP
    # → trade is genuinely still open → keep as OPEN
    if trade.current_status == 'OPEN':
        # Update trailing SL in DB (may have moved due to partial TPs hit)
        # but keep status OPEN so V4 trade monitor continues watching it
        if partial_pnls:
            # Some TPs already hit — update tp_hit and trailing sl
            return SimResult(
                trade_id=trade.id, status="OPEN",
                outcome=None, tp_hit=tp_hit,
                close_price=None,
                pnl_r=None,
                weighted_pnl_r=None,
                position_pct=round(position_pct, 4),
                note=f"still_open_tp_hit_{tp_hit}_sl_now_{current_sl:.6f}",
            )
        else:
            return SimResult(
                trade_id=trade.id, status="OPEN",
                outcome=None, tp_hit=0,
                close_price=None, pnl_r=None, weighted_pnl_r=None,
                position_pct=1.0, note="still_open",
            )

    # Closed trade that ran out of candle data → CLOSED_MANUAL
    if partial_pnls:
        partial_pnls.append((calc_r(last_close), position_pct))
        w_pnl   = sum(r * f for r, f in partial_pnls)
        outcome = "WIN" if w_pnl > 0 else ("LOSS" if w_pnl < 0 else "BREAKEVEN")
    else:
        w_pnl   = calc_r(last_close)
        outcome = "WIN" if w_pnl > 0 else ("LOSS" if w_pnl < 0 else "BREAKEVEN")

    return SimResult(
        trade_id=trade.id, status="CLOSED_MANUAL",
        outcome=outcome, tp_hit=tp_hit,
        close_price=last_close,
        pnl_r=round(calc_r(last_close), 4),
        weighted_pnl_r=round(w_pnl, 4),
        position_pct=round(position_pct, 4),
        note="end_of_data",
    )


# ── Per-trade worker ──────────────────────────────────────────────────────────

def _process_trade(trade: TradeToSimulate) -> SimResult:
    """Fetches candles and simulates one trade."""
    conn = get_db_connection()
    try:
        # For open trades: simulate from open to NOW
        # For closed trades: simulate from open to close + 24h buffer
        if trade.current_status == 'OPEN' or trade.closed_at is None:
            end_dt = datetime.datetime.now(datetime.timezone.utc)
        else:
            end_dt = trade.closed_at + datetime.timedelta(hours=24)
        candles = _get_candles(conn, trade.symbol, trade.opened_at, end_dt)

        if candles.empty:
            return SimResult(
                trade_id=trade.id, status="CLOSED_MANUAL",
                outcome=None, tp_hit=0, close_price=None,
                pnl_r=None, weighted_pnl_r=None, position_pct=1.0,
                note="no_data_available",
            )

        return _simulate_trade(trade, candles)

    except Exception as e:
        logger.warning(f"Simulation error trade {trade.id}: {e}")
        return SimResult(
            trade_id=trade.id, status="CLOSED_MANUAL",
            outcome=None, tp_hit=0, close_price=None,
            pnl_r=None, weighted_pnl_r=None, position_pct=1.0,
            note=f"error: {e}",
        )
    finally:
        conn.close()


# ── DB operations ─────────────────────────────────────────────────────────────

def _load_trades_to_simulate(conn, limit: int | None) -> list[TradeToSimulate]:
    """Loads ALL V3 trades (open + closed) for simulation."""
    sql = """
        SELECT id, symbol, direction, entry,
               sl, tp1, tp2, tp3, tp4, tp5, tp6,
               opened_at, closed_at, status
        FROM trades
        WHERE bot_version = 'v3'
        ORDER BY opened_at ASC
    """
    if limit:
        sql += f" LIMIT {limit}"

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    trades = []
    for row in rows:
        (trade_id, symbol, direction, entry,
         sl, tp1, tp2, tp3, tp4, tp5, tp6,
         opened_at, closed_at, current_status) = row

        # Collect non-null targets
        tps = [float(t) for t in [tp1, tp2, tp3, tp4, tp5, tp6]
               if t is not None and float(t) > 0]

        if not entry or float(entry) == 0:
            continue
        if not sl or float(sl) == 0:
            continue

        # Ensure timezone aware
        if opened_at and opened_at.tzinfo is None:
            opened_at = opened_at.replace(tzinfo=datetime.timezone.utc)
        if closed_at and closed_at.tzinfo is None:
            closed_at = closed_at.replace(tzinfo=datetime.timezone.utc)

        trades.append(TradeToSimulate(
            id=trade_id, symbol=symbol, direction=direction,
            entry=float(entry), sl=float(sl), tps=tps,
            opened_at=opened_at, closed_at=closed_at,
            current_status=current_status,
        ))

    return trades


def _apply_result(conn, result: SimResult, dry_run: bool) -> None:
    """Writes simulation result back to trades table."""
    if dry_run:
        return

    if result.status == "OPEN":
        # Still open — only update tp_hit, position_pct and trailing SL
        # Extract trailing SL from note if available
        trailing_sl = None
        if result.note and "sl_now_" in result.note:
            try:
                trailing_sl = float(result.note.split("sl_now_")[1])
            except Exception:
                pass

        if trailing_sl is not None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE trades SET
                        tp_hit       = %s,
                        position_pct = %s,
                        sl           = %s,
                        last_updated = NOW()
                    WHERE id = %s
                    """,
                    (result.tp_hit, result.position_pct,
                     trailing_sl, result.trade_id),
                )
        else:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE trades SET
                        tp_hit       = %s,
                        position_pct = %s,
                        last_updated = NOW()
                    WHERE id = %s
                    """,
                    (result.tp_hit, result.position_pct, result.trade_id),
                )
        conn.commit()
        return

    # Closed trade — full update
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trades SET
                status          = %s,
                outcome         = %s,
                tp_hit          = %s,
                close_price     = %s,
                pnl_r           = %s,
                weighted_pnl_r  = %s,
                position_pct    = %s,
                closed_at       = CASE WHEN closed_at IS NULL THEN NOW()
                                       ELSE closed_at END,
                last_updated    = NOW()
            WHERE id = %s
            """,
            (
                result.status, result.outcome, result.tp_hit,
                result.close_price, result.pnl_r,
                result.weighted_pnl_r, result.position_pct,
                result.trade_id,
            ),
        )
    conn.commit()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulate V3 closed trades using 5m candles."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show results without writing to DB.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process first N trades (for testing).")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info(f"V3 Trade Simulation "
                f"({'DRY RUN' if args.dry_run else 'LIVE'})")
    logger.info("=" * 60)

    with db_connection() as conn:
        trades = _load_trades_to_simulate(conn, args.limit)

    if not trades:
        logger.info("No V3 closed trades found to simulate.")
        return

    logger.info(f"Trades to simulate: {len(trades)}")
    logger.info(f"Date range: {trades[0].opened_at.date()} → "
                f"{trades[-1].opened_at.date()}")
    logger.info(f"Workers: {WORKERS}")
    logger.info("")

    t_start   = time.time()
    results   = []
    wins = losses = no_data = still_open = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_process_trade, t): t for t in trades}
        done    = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            done += 1

            if result.status == "OPEN":
                still_open += 1
            elif result.outcome == "WIN":
                wins += 1
            elif result.outcome == "LOSS":
                losses += 1
            elif result.note in ("no_data_available", "no_candle_data"):
                no_data += 1

            if done % 50 == 0 or done == len(trades):
                elapsed = time.time() - t_start
                logger.info(
                    f"Progress: {done}/{len(trades)} "
                    f"({done/len(trades)*100:.0f}%) — "
                    f"W:{wins} L:{losses} NoData:{no_data} "
                    f"({elapsed:.0f}s)"
                )

    # Apply results
    if not args.dry_run:
        logger.info("Writing results to DB...")
        with db_connection() as conn:
            for result in results:
                _apply_result(conn, result, dry_run=False)

    # Summary
    elapsed   = time.time() - t_start
    total_r   = sum(r.pnl_r for r in results if r.pnl_r is not None)
    avg_r     = total_r / len(results) if results else 0
    win_rate  = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"Simulation complete in {elapsed:.0f}s")
    logger.info(f"  Total trades:  {len(trades)}")
    logger.info(f"  Wins:          {wins}")
    logger.info(f"  Losses:        {losses}")
    logger.info(f"  No data:       {no_data}")
    logger.info(f"  Still open:    {still_open}")
    logger.info(f"  Win rate:      {win_rate:.1f}%")
    logger.info(f"  Avg pnl_r:     {avg_r:.2f}R")
    if args.dry_run:
        logger.info("  (DRY RUN — nothing written to DB)")
    logger.info("=" * 60)

    # Show sample of changed outcomes (dry run)
    if args.dry_run and results:
        logger.info("")
        logger.info("Sample results (first 10):")
        for r in results[:10]:
            logger.info(
                f"  trade {r.trade_id}: {r.status} "
                f"outcome={r.outcome} tp_hit={r.tp_hit} "
                f"pnl_r={r.pnl_r} note={r.note}"
            )


if __name__ == "__main__":
    main()

