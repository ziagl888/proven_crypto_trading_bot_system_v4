#!/usr/bin/env python3
# 90_migrate_v3_trades.py
# One-time migration: V3 trade tables → V4 unified `trades` table.
#
# IMPORTANT: Run --dry-run first to verify counts before executing.
#
# V3 source tables (must exist in same PostgreSQL instance):
#
#   active_trades_master   columns: id, strategy, time, coin, direction, lev,
#                                   target1-4, sl, entry, posted, status
#                          status:  'WORKING' (open)
#                                   '0'  = SL hit (closed)
#                                   '1'  = TP1 hit
#                                   '2'  = TP2 hit
#                                   '3'  = TP3 hit
#                                   '4'  = TP4 hit
#
#   closed_trades_master   columns: id, strategy, time, coin, direction, lev,
#                                   entry, target1-4, sl, close_price,
#                                   posted, status
#                          status:  same numeric codes as above
#
#   ai_signals             columns: id, symbol, price, model, direction,
#                                   confidence, entry1, entry2, sl, targets (JSON),
#                                   current_target_hit, open_time, status
#                          status:  'OPEN' (open trades)
#
#   closed_ai_signals      columns: id, symbol, model, direction, entry,
#                                   close_price, targets_hit, open_time,
#                                   close_time, status (= close_reason string)
#                          status:  free-text e.g. 'SL Hit (SL: 0.123)',
#                                   'ALL TARGETS HIT', 'LEGACY TARGET HIT (+2.5%)'
#
# Usage:
#   python 90_migrate_v3_trades.py --dry-run   # preview, no writes
#   python 90_migrate_v3_trades.py             # execute migration

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from core.database import db_connection
from core.schema import verify_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - MIGRATION - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

import warnings
warnings.filterwarnings("ignore")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _table_exists(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (table,))
        return cur.fetchone()[0] is not None


def _already_migrated(conn, source_table: str, source_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM v3_migration_log "
            "WHERE source_table = %s AND source_id = %s",
            (source_table, source_id),
        )
        return cur.fetchone() is not None


def _log_migration(conn, source_table: str, source_id: int,
                   target_id: int, notes: str = "") -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO v3_migration_log (source_table, source_id, target_id, notes)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (source_table, source_id) DO NOTHING
            """,
            (source_table, source_id, target_id, notes),
        )


def _parse_leverage(lev_str) -> int:
    """Parses '20x' or '20' or None → integer."""
    try:
        return int(str(lev_str).replace("x", "").strip())
    except Exception:
        return 20


def _map_numeric_status(v3_status) -> tuple[str, str | None, int]:
    """
    Maps V3 numeric status from active_trades_master / closed_trades_master.
    Returns (v4_status, outcome, tp_hit).

    V3 status values:
        'WORKING' → open trade
        '0'       → SL hit (loss)
        '1'..'4'  → TP1..TP4 hit (win)
    """
    s = str(v3_status).strip() if v3_status else ""
    if s in ("WORKING", ""):
        # Empty status treated as OPEN only if we expect it from active table
        # but empty string from closed table = CLOSED_MANUAL
        if s == "WORKING":
            return "OPEN", None, 0
        return "CLOSED_MANUAL", None, 0
    if s == "0":
        return "CLOSED_SL", "LOSS", 0
    try:
        tp_num = int(s)
        if 1 <= tp_num <= 6:
            return "CLOSED_TP", "WIN", tp_num
    except ValueError:
        pass
    # Unknown status — treat as manual close
    return "CLOSED_MANUAL", None, 0


def _map_close_reason(reason: str) -> tuple[str, str | None]:
    """
    Maps V3 ai_signals close_reason string to (v4_status, outcome).

    Known values:
        'SL Hit (SL: 0.123)'          → CLOSED_SL, LOSS
        'ALL TARGETS HIT'             → CLOSED_TP, WIN
        'LEGACY TARGET HIT (+2.5%)'   → CLOSED_TP, WIN
        'LEGACY FALLBACK SL (-5.0%)' → CLOSED_SL, LOSS
        ''  (empty)                   → CLOSED_MANUAL, None
    """
    r = str(reason).strip().upper() if reason else ""
    if not r:
        return "CLOSED_MANUAL", None
    if "SL HIT" in r or "FALLBACK SL" in r:
        return "CLOSED_SL", "LOSS"
    if "TARGET HIT" in r or "ALL TARGETS" in r:
        return "CLOSED_TP", "WIN"
    return "CLOSED_MANUAL", None


def _calc_pnl_r(entry: float, sl: float,
                close_price: float, direction: str) -> float | None:
    """Calculates R-multiple. Returns None if risk=0."""
    try:
        risk = abs(entry - sl)
        if risk == 0 or entry == 0:
            return None
        if direction.upper() == "LONG":
            return round((close_price - entry) / risk, 4)
        else:
            return round((entry - close_price) / risk, 4)
    except Exception:
        return None


def _parse_targets(targets_json) -> list[float]:
    """Parses V3 targets JSON array → list of floats (max 6)."""
    try:
        if not targets_json:
            return []
        data = json.loads(targets_json) if isinstance(targets_json, str) \
               else list(targets_json)
        return [float(t) for t in data if t and float(t) > 0][:6]
    except Exception:
        return []


# ── Migration functions ───────────────────────────────────────────────────────

def migrate_active_trades_master(conn, dry_run: bool) -> int:
    """
    Migrates V3 active_trades_master.
    Columns: id, strategy, time, coin, direction, lev,
             target1, target2, target3, target4, sl, entry, posted, status
    Status: 'WORKING' = open, '0'=SL hit, '1'-'4'=TP hit
    """
    if not _table_exists(conn, "active_trades_master"):
        logger.warning("active_trades_master not found — skipping.")
        return 0

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, strategy, time, coin, direction, lev,
                   target1, target2, target3, target4,
                   sl, entry, posted, status
            FROM active_trades_master
            ORDER BY id
        """)
        rows = cur.fetchall()

    migrated = skipped = 0

    for row in rows:
        (v3_id, strategy, created_at, coin, direction, lev,
         tp1, tp2, tp3, tp4, sl, entry, posted, v3_status) = row

        if _already_migrated(conn, "active_trades_master", v3_id):
            skipped += 1
            continue

        v4_status, outcome, tp_hit = _map_numeric_status(v3_status)
        leverage  = _parse_leverage(lev)

        # Non-null targets
        tps      = [t for t in [tp1, tp2, tp3, tp4] if t and float(t) > 0]
        tp_count = len(tps) if tps else 1

        # For closed trades, posted = close time (V3 used posted for both)
        opened_at = created_at
        closed_at = posted if v4_status != "OPEN" else None

        close_price = None
        pnl_r       = None
        # V3 doesn't store close_price in active_trades_master
        # closed trades moved to closed_trades_master which we migrate separately

        if dry_run:
            logger.info(
                f"  [DRY] active_trades_master id={v3_id} "
                f"{coin} {direction} {strategy} → {v4_status}"
            )
            migrated += 1
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trades (
                    bot_name, bot_version, symbol, direction,
                    entry, tp1, tp2, tp3, tp4, sl, sl_initial,
                    leverage, tp_count, tp_hit,
                    status, outcome, opened_at, closed_at, last_updated
                ) VALUES (
                    %s,'v3',%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,%s,NOW()
                ) RETURNING id
                """,
                (
                    strategy, coin, direction,
                    float(entry or 0),
                    float(tp1) if tp1 and float(tp1) > 0 else None,
                    float(tp2) if tp2 and float(tp2) > 0 else None,
                    float(tp3) if tp3 and float(tp3) > 0 else None,
                    float(tp4) if tp4 and float(tp4) > 0 else None,
                    None, None,           # tp5, tp6
                    float(sl or 0), float(sl or 0),
                    leverage, tp_count, tp_hit,
                    v4_status, outcome, opened_at, closed_at,
                ),
            )
            new_id = cur.fetchone()[0]

        _log_migration(conn, "active_trades_master", v3_id, new_id)
        migrated += 1

    if not dry_run:
        conn.commit()

    logger.info(f"active_trades_master: {migrated} migrated, {skipped} skipped.")
    return migrated


def migrate_closed_trades_master(conn, dry_run: bool) -> int:
    """
    Migrates V3 closed_trades_master.
    Columns: id, strategy, time, coin, direction, lev,
             entry, target1-4, sl, close_price, posted, status
    Note: 'time' = open time, 'posted' = close time
    """
    if not _table_exists(conn, "closed_trades_master"):
        logger.warning("closed_trades_master not found — skipping.")
        return 0

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, strategy, time, coin, direction, lev,
                   entry, target1, target2, target3, target4,
                   sl, close_price, posted, status
            FROM closed_trades_master
            ORDER BY id
        """)
        rows = cur.fetchall()

    migrated = skipped = 0

    for row in rows:
        (v3_id, strategy, opened_at, coin, direction, lev,
         entry, tp1, tp2, tp3, tp4,
         sl, close_price, closed_at, v3_status) = row

        if _already_migrated(conn, "closed_trades_master", v3_id):
            skipped += 1
            continue

        v4_status, outcome, tp_hit = _map_numeric_status(v3_status)
        leverage  = _parse_leverage(lev)
        tps       = [t for t in [tp1, tp2, tp3, tp4] if t and float(t) > 0]
        tp_count  = len(tps) if tps else 1

        entry_f      = float(entry or 0)
        sl_f         = float(sl or 0)
        close_f      = float(close_price) if close_price else None
        pnl_r        = _calc_pnl_r(entry_f, sl_f, close_f, direction) \
                       if close_f and sl_f else None

        if dry_run:
            logger.info(
                f"  [DRY] closed_trades_master id={v3_id} "
                f"{coin} {direction} {strategy} → {v4_status} pnl_r={pnl_r}"
            )
            migrated += 1
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trades (
                    bot_name, bot_version, symbol, direction,
                    entry, tp1, tp2, tp3, tp4, sl, sl_initial,
                    leverage, tp_count, tp_hit,
                    status, outcome, close_price, pnl_r,
                    opened_at, closed_at, last_updated
                ) VALUES (
                    %s,'v3',%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,NOW()
                ) RETURNING id
                """,
                (
                    strategy, coin, direction,
                    entry_f,
                    float(tp1) if tp1 and float(tp1) > 0 else None,
                    float(tp2) if tp2 and float(tp2) > 0 else None,
                    float(tp3) if tp3 and float(tp3) > 0 else None,
                    float(tp4) if tp4 and float(tp4) > 0 else None,
                    None, None,
                    sl_f, sl_f,
                    leverage, tp_count, tp_hit,
                    v4_status, outcome, close_f, pnl_r,
                    opened_at, closed_at,
                ),
            )
            new_id = cur.fetchone()[0]

        _log_migration(conn, "closed_trades_master", v3_id, new_id)
        migrated += 1

    if not dry_run:
        conn.commit()

    logger.info(f"closed_trades_master: {migrated} migrated, {skipped} skipped.")
    return migrated


def migrate_ai_signals(conn, dry_run: bool) -> int:
    """
    Migrates V3 ai_signals (open AI/ML trades).
    Columns: id, symbol, price, model, direction, confidence,
             entry1, entry2, sl, targets (JSON), current_target_hit,
             open_time, status
    Status: 'OPEN'
    """
    if not _table_exists(conn, "ai_signals"):
        logger.warning("ai_signals not found — skipping.")
        return 0

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, symbol, price, model, direction, confidence,
                   entry1, entry2, sl, targets, current_target_hit,
                   open_time, status
            FROM ai_signals
            ORDER BY id
        """)
        rows = cur.fetchall()

    migrated = skipped = 0

    for row in rows:
        (v3_id, symbol, price, model, direction, confidence,
         entry1, entry2, sl, targets_json, target_hit,
         open_time, v3_status) = row

        if _already_migrated(conn, "ai_signals", v3_id):
            skipped += 1
            continue

        # ai_signals are always OPEN — closed ones go to closed_ai_signals
        v4_status = "OPEN"

        tps      = _parse_targets(targets_json)
        tp_count = len(tps) if tps else 1
        tp_hit   = int(target_hit or 0)

        entry_f = float(entry1 or price or 0)
        if entry_f == 0:
            logger.warning(f"  ai_signals id={v3_id}: no entry price — skipping.")
            continue

        sl_f = float(sl or 0)

        # Trailing SL: if tp_hit > 0, sl has already been moved by V3 monitor
        # We store current sl as both sl and sl_initial won't be exact,
        # but sl_initial approximated from entry - (entry - sl) * scaling
        sl_initial = sl_f  # best approximation available

        if dry_run:
            logger.info(
                f"  [DRY] ai_signals id={v3_id} "
                f"{symbol} {direction} {model} tp_hit={tp_hit}"
            )
            migrated += 1
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trades (
                    bot_name, bot_version, symbol, direction,
                    entry, tp1, tp2, tp3, tp4, tp5, tp6,
                    sl, sl_initial, leverage, tp_count, tp_hit,
                    ml_model, ml_confidence,
                    status, opened_at, last_updated
                ) VALUES (
                    %s,'v3',%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,
                    %s,%s,20,%s,%s,
                    %s,%s,
                    %s,%s,NOW()
                ) RETURNING id
                """,
                (
                    model or "AI_UNKNOWN", symbol, direction,
                    entry_f,
                    tps[0] if len(tps) > 0 else None,
                    tps[1] if len(tps) > 1 else None,
                    tps[2] if len(tps) > 2 else None,
                    tps[3] if len(tps) > 3 else None,
                    tps[4] if len(tps) > 4 else None,
                    tps[5] if len(tps) > 5 else None,
                    sl_f, sl_initial, tp_count, tp_hit,
                    model, float(confidence) if confidence else None,
                    v4_status, open_time,
                ),
            )
            new_id = cur.fetchone()[0]

        _log_migration(conn, "ai_signals", v3_id, new_id)
        migrated += 1

    if not dry_run:
        conn.commit()

    logger.info(f"ai_signals: {migrated} migrated, {skipped} skipped.")
    return migrated


def migrate_closed_ai_signals(conn, dry_run: bool) -> int:
    """
    Migrates V3 closed_ai_signals.
    Columns: id, symbol, model, direction, entry, close_price,
             targets_hit, open_time, close_time, status (= close_reason string)
    Note: targets not stored — only targets_hit count
    """
    if not _table_exists(conn, "closed_ai_signals"):
        logger.warning("closed_ai_signals not found — skipping.")
        return 0

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, symbol, model, direction,
                   entry, close_price, targets_hit,
                   open_time, close_time, status
            FROM closed_ai_signals
            ORDER BY id
        """)
        rows = cur.fetchall()

    migrated = skipped = 0

    for row in rows:
        (v3_id, symbol, model, direction,
         entry, close_price, targets_hit,
         open_time, close_time, close_reason) = row

        if _already_migrated(conn, "closed_ai_signals", v3_id):
            skipped += 1
            continue

        v4_status, outcome = _map_close_reason(close_reason)
        tp_hit   = int(targets_hit or 0)
        entry_f  = float(entry or 0)
        close_f  = float(close_price) if close_price else None
        pnl_r    = None  # no SL stored in closed_ai_signals

        if dry_run:
            logger.info(
                f"  [DRY] closed_ai_signals id={v3_id} "
                f"{symbol} {direction} {model} → {v4_status} "
                f"reason='{close_reason}'"
            )
            migrated += 1
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trades (
                    bot_name, bot_version, symbol, direction,
                    entry, tp1, sl, sl_initial,
                    leverage, tp_count, tp_hit,
                    ml_model,
                    status, outcome, close_price, pnl_r,
                    opened_at, closed_at, last_updated
                ) VALUES (
                    %s,'v3',%s,%s,
                    %s,0,0,0,
                    20,GREATEST(%s,1),%s,
                    %s,
                    %s,%s,%s,%s,
                    %s,%s,NOW()
                ) RETURNING id
                """,
                (
                    model or "AI_UNKNOWN", symbol, direction,
                    entry_f,
                    tp_hit, tp_hit,   # tp_count, tp_hit
                    model,
                    v4_status, outcome, close_f, pnl_r,
                    open_time, close_time,
                ),
            )
            new_id = cur.fetchone()[0]

        _log_migration(conn, "closed_ai_signals", v3_id, new_id,
                       notes=str(close_reason or ""))
        migrated += 1

    if not dry_run:
        conn.commit()

    logger.info(f"closed_ai_signals: {migrated} migrated, {skipped} skipped.")
    return migrated


# ── Summary query ─────────────────────────────────────────────────────────────

def print_summary(conn) -> None:
    """Prints migration summary from v3_migration_log."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT source_table, COUNT(*) as count
            FROM v3_migration_log
            GROUP BY source_table
            ORDER BY source_table
        """)
        rows = cur.fetchall()

    logger.info("Migration log summary:")
    total = 0
    for source_table, count in rows:
        logger.info(f"  {source_table}: {count} rows")
        total += count
    logger.info(f"  TOTAL: {total} rows in v4 trades table")

    # Check for open trades
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM trades WHERE status = 'OPEN'")
        open_count = cur.fetchone()[0]
    logger.info(f"  Open trades in V4: {open_count}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate V3 trade tables to V4 unified trades table."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be migrated without writing to DB."
    )
    args = parser.parse_args()

    dry_run = args.dry_run
    mode = "DRY RUN — no data will be written" if dry_run else "LIVE MIGRATION"

    logger.info("=" * 60)
    logger.info(f"V3 → V4 Trade Migration ({mode})")
    logger.info("=" * 60)

    # Verify V4 schema
    result = verify_schema()
    missing = [t for t in result["missing"]
               if t in ("trades", "signal_log", "bot_performance", "v3_migration_log")]
    if missing:
        logger.critical(f"V4 tables missing: {missing}. Run bootstrap first.")
        sys.exit(1)

    total = 0
    with db_connection() as conn:

        # Check which V3 tables exist
        for tbl in ["active_trades_master", "closed_trades_master",
                    "ai_signals", "closed_ai_signals"]:
            exists = _table_exists(conn, tbl)
            logger.info(f"  V3 table {tbl}: {'found' if exists else 'NOT FOUND'}")

        logger.info("")

        total += migrate_active_trades_master(conn, dry_run)
        total += migrate_closed_trades_master(conn, dry_run)
        total += migrate_ai_signals(conn, dry_run)
        total += migrate_closed_ai_signals(conn, dry_run)

        if not dry_run:
            print_summary(conn)

    logger.info("=" * 60)
    if dry_run:
        logger.info(f"DRY RUN complete — {total} rows would be migrated.")
        logger.info("Run without --dry-run to execute.")
    else:
        logger.info(f"Migration complete — {total} rows migrated.")
        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. Check trades table in pgAdmin")
        logger.info("  2. Verify open trades count matches V3")
        logger.info("  3. Start V4 trade monitor")
        logger.info("  4. Once V4 monitor confirmed working, stop V3 monitor")


if __name__ == "__main__":
    main()
