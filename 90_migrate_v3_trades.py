#!/usr/bin/env python3
# 90_migrate_v3_trades.py
# One-time migration script: V3 trade tables → V4 unified `trades` table.
#
# Run ONCE manually after V4 is deployed on the same PostgreSQL instance as V3.
# Safe to run multiple times — uses v3_migration_log to skip already-migrated rows.
#
# V3 source tables (must exist in same DB):
#   active_trades_master   — classic bots, open trades
#   closed_trades_master   — classic bots, closed trades
#   ai_signals             — AI/ML bots, open trades
#   closed_ai_signals      — AI/ML bots, closed trades
#
# Usage:
#   python 90_migrate_v3_trades.py [--dry-run]

from __future__ import annotations

import argparse
import datetime
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


def _map_status_classic(v3_status: str) -> str:
    """Maps V3 classic trade status to V4 status."""
    mapping = {
        "WORKING":  "OPEN",
        "WIN":      "CLOSED_TP",
        "LOSS":     "CLOSED_SL",
        "CLOSED":   "CLOSED_MANUAL",
        "EXPIRED":  "CLOSED_EXPIRED",
    }
    return mapping.get(str(v3_status).upper(), "CLOSED_MANUAL")


def _map_outcome(v3_status: str) -> str | None:
    """Maps V3 status to V4 outcome."""
    mapping = {
        "WIN":   "WIN",
        "LOSS":  "LOSS",
    }
    return mapping.get(str(v3_status).upper())


def _map_ai_status(v3_status: str) -> str:
    """Maps V3 AI signal status to V4 status."""
    mapping = {
        "OPEN":   "OPEN",
        "WIN":    "CLOSED_TP",
        "LOSS":   "CLOSED_SL",
        "CLOSED": "CLOSED_MANUAL",
    }
    return mapping.get(str(v3_status).upper(), "CLOSED_MANUAL")


def _calc_pnl_r(entry: float, sl: float, close_price: float,
                direction: str) -> float | None:
    """Calculates R-multiple from entry, SL and close price."""
    try:
        risk = abs(entry - sl)
        if risk == 0:
            return None
        if direction.upper() == "LONG":
            return (close_price - entry) / risk
        else:
            return (entry - close_price) / risk
    except Exception:
        return None


# ── Migration functions ───────────────────────────────────────────────────────

def migrate_active_trades_master(conn, dry_run: bool) -> int:
    """Migrates V3 active_trades_master (classic bots, open + recent closed)."""
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

    migrated = 0
    skipped  = 0

    for row in rows:
        (v3_id, strategy, created_at, coin, direction, lev,
         tp1, tp2, tp3, tp4, sl, entry, posted, v3_status) = row

        if _already_migrated(conn, "active_trades_master", v3_id):
            skipped += 1
            continue

        status  = _map_status_classic(v3_status)
        outcome = _map_outcome(v3_status)

        # Parse leverage integer from "20x" string
        try:
            leverage = int(str(lev).replace("x", "").strip())
        except Exception:
            leverage = 20

        # Count non-null targets
        tps = [t for t in [tp1, tp2, tp3, tp4] if t and float(t) > 0]
        tp_count = len(tps) if tps else 1

        # pnl_r only if closed
        close_price = None
        pnl_r       = None
        closed_at   = None
        if status != "OPEN":
            closed_at = posted  # V3 didn't store close time — use posted as approximation

        if dry_run:
            logger.info(
                f"[DRY RUN] Would migrate active_trades_master id={v3_id} "
                f"{coin} {direction} {strategy} status={status}"
            )
            migrated += 1
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trades (
                    bot_name, bot_version, symbol, direction,
                    entry, tp1, tp2, tp3, tp4, sl, sl_initial,
                    leverage, tp_count, status, outcome,
                    close_price, pnl_r, opened_at, closed_at, last_updated
                ) VALUES (
                    %s, 'v3', %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, NOW()
                ) RETURNING id
                """,
                (
                    strategy, coin, direction,
                    entry,
                    tp1 if tp1 and float(tp1) > 0 else None,
                    tp2 if tp2 and float(tp2) > 0 else None,
                    tp3 if tp3 and float(tp3) > 0 else None,
                    tp4 if tp4 and float(tp4) > 0 else None,
                    None, None,  # tp5, tp6
                    sl, sl,     # sl, sl_initial
                    leverage, tp_count, status, outcome,
                    close_price, pnl_r,
                    created_at or posted, closed_at,
                ),
            )
            new_id = cur.fetchone()[0]

        _log_migration(conn, "active_trades_master", v3_id, new_id)
        migrated += 1

    if not dry_run:
        conn.commit()

    logger.info(
        f"active_trades_master: {migrated} migrated, {skipped} already done."
    )
    return migrated


def migrate_ai_signals(conn, dry_run: bool) -> int:
    """Migrates V3 ai_signals (AI/ML bots, open trades)."""
    if not _table_exists(conn, "ai_signals"):
        logger.warning("ai_signals not found — skipping.")
        return 0

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, symbol, model, direction, entry1, price,
                   sl, targets, confidence, current_target_hit,
                   open_time, status
            FROM ai_signals
            ORDER BY id
        """)
        rows = cur.fetchall()

    migrated = 0
    skipped  = 0

    for row in rows:
        (v3_id, symbol, model, direction, entry1, price,
         sl, targets_json, confidence, tp_hit,
         open_time, v3_status) = row

        if _already_migrated(conn, "ai_signals", v3_id):
            skipped += 1
            continue

        status  = _map_ai_status(v3_status)
        outcome = _map_outcome(v3_status)

        # Parse targets JSON array
        import json
        tps = []
        try:
            if targets_json:
                tps = json.loads(targets_json) if isinstance(targets_json, str) \
                      else list(targets_json)
                tps = [float(t) for t in tps if t]
        except Exception:
            pass

        entry = float(entry1 or price or 0)
        if entry == 0:
            continue  # Can't migrate without entry

        tp1 = tps[0] if len(tps) > 0 else None
        tp2 = tps[1] if len(tps) > 1 else None
        tp3 = tps[2] if len(tps) > 2 else None
        tp4 = tps[3] if len(tps) > 3 else None
        tp5 = tps[4] if len(tps) > 4 else None
        tp6 = tps[5] if len(tps) > 5 else None
        tp_count = min(len(tps), 6) if tps else 1

        close_price = float(price) if status != "OPEN" and price else None
        pnl_r       = None
        if close_price and sl:
            pnl_r = _calc_pnl_r(entry, float(sl), close_price, direction)

        if dry_run:
            logger.info(
                f"[DRY RUN] Would migrate ai_signals id={v3_id} "
                f"{symbol} {direction} {model} status={status}"
            )
            migrated += 1
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trades (
                    bot_name, bot_version, symbol, direction,
                    entry, tp1, tp2, tp3, tp4, tp5, tp6,
                    sl, sl_initial, leverage, tp_count,
                    tp_hit, ml_model, ml_confidence,
                    status, outcome, close_price, pnl_r,
                    opened_at, last_updated
                ) VALUES (
                    %s, 'v3', %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, 20, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, NOW()
                ) RETURNING id
                """,
                (
                    model or "AI_UNKNOWN", symbol, direction,
                    entry, tp1, tp2, tp3, tp4, tp5, tp6,
                    sl, sl, tp_count,
                    tp_hit or 0, model, confidence,
                    status, outcome, close_price, pnl_r,
                    open_time,
                ),
            )
            new_id = cur.fetchone()[0]

        _log_migration(conn, "ai_signals", v3_id, new_id)
        migrated += 1

    if not dry_run:
        conn.commit()

    logger.info(f"ai_signals: {migrated} migrated, {skipped} already done.")
    return migrated


def migrate_closed_trades_master(conn, dry_run: bool) -> int:
    """Migrates V3 closed_trades_master."""
    if not _table_exists(conn, "closed_trades_master"):
        logger.warning("closed_trades_master not found — skipping.")
        return 0

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, strategy, coin, direction,
                   entry, close_price, status,
                   opened_at, closed_at
            FROM closed_trades_master
            ORDER BY id
        """)
        rows = cur.fetchall()

    migrated = 0
    skipped  = 0

    for row in rows:
        (v3_id, strategy, coin, direction,
         entry, close_price, v3_status,
         opened_at, closed_at) = row

        if _already_migrated(conn, "closed_trades_master", v3_id):
            skipped += 1
            continue

        status  = _map_status_classic(v3_status)
        outcome = _map_outcome(v3_status)

        if dry_run:
            logger.info(
                f"[DRY RUN] Would migrate closed_trades_master id={v3_id} "
                f"{coin} {direction} outcome={outcome}"
            )
            migrated += 1
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trades (
                    bot_name, bot_version, symbol, direction,
                    entry, tp1, sl, sl_initial,
                    leverage, tp_count, status, outcome,
                    close_price, opened_at, closed_at, last_updated
                ) VALUES (
                    %s, 'v3', %s, %s,
                    %s, 0, 0, 0,
                    20, 1, %s, %s,
                    %s, %s, %s, NOW()
                ) RETURNING id
                """,
                (
                    strategy, coin, direction,
                    entry or 0,
                    status, outcome,
                    close_price,
                    opened_at, closed_at,
                ),
            )
            new_id = cur.fetchone()[0]

        _log_migration(conn, "closed_trades_master", v3_id, new_id)
        migrated += 1

    if not dry_run:
        conn.commit()

    logger.info(
        f"closed_trades_master: {migrated} migrated, {skipped} already done."
    )
    return migrated


def migrate_closed_ai_signals(conn, dry_run: bool) -> int:
    """Migrates V3 closed_ai_signals."""
    if not _table_exists(conn, "closed_ai_signals"):
        logger.warning("closed_ai_signals not found — skipping.")
        return 0

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, symbol, model, direction,
                   entry, close_price, status,
                   opened_at, closed_at
            FROM closed_ai_signals
            ORDER BY id
        """)
        rows = cur.fetchall()

    migrated = 0
    skipped  = 0

    for row in rows:
        (v3_id, symbol, model, direction,
         entry, close_price, v3_status,
         opened_at, closed_at) = row

        if _already_migrated(conn, "closed_ai_signals", v3_id):
            skipped += 1
            continue

        status  = _map_ai_status(v3_status)
        outcome = _map_outcome(v3_status)

        if dry_run:
            logger.info(
                f"[DRY RUN] Would migrate closed_ai_signals id={v3_id} "
                f"{symbol} {direction} outcome={outcome}"
            )
            migrated += 1
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trades (
                    bot_name, bot_version, symbol, direction,
                    entry, tp1, sl, sl_initial,
                    leverage, tp_count, ml_model,
                    status, outcome, close_price,
                    opened_at, closed_at, last_updated
                ) VALUES (
                    %s, 'v3', %s, %s,
                    %s, 0, 0, 0,
                    20, 1, %s,
                    %s, %s, %s,
                    %s, %s, NOW()
                ) RETURNING id
                """,
                (
                    model or "AI_UNKNOWN", symbol, direction,
                    entry or 0,
                    model,
                    status, outcome, close_price,
                    opened_at, closed_at,
                ),
            )
            new_id = cur.fetchone()[0]

        _log_migration(conn, "closed_ai_signals", v3_id, new_id)
        migrated += 1

    if not dry_run:
        conn.commit()

    logger.info(
        f"closed_ai_signals: {migrated} migrated, {skipped} already done."
    )
    return migrated


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
    if dry_run:
        logger.info("=" * 60)
        logger.info("DRY RUN — no data will be written")
        logger.info("=" * 60)
    else:
        logger.info("=" * 60)
        logger.info("V3 → V4 Trade Migration")
        logger.info("=" * 60)

    # Verify V4 schema is ready
    result = verify_schema()
    missing = [t for t in result["missing"]
               if t in ("trades", "signal_log", "bot_performance", "v3_migration_log")]
    if missing:
        logger.critical(f"V4 trade tables missing: {missing}. Run bootstrap first.")
        sys.exit(1)

    total = 0
    with db_connection() as conn:
        total += migrate_active_trades_master(conn, dry_run)
        total += migrate_closed_trades_master(conn, dry_run)
        total += migrate_ai_signals(conn, dry_run)
        total += migrate_closed_ai_signals(conn, dry_run)

    logger.info("=" * 60)
    if dry_run:
        logger.info(f"DRY RUN complete — {total} rows would be migrated.")
    else:
        logger.info(f"Migration complete — {total} rows migrated.")
        logger.info(
            "Next steps:\n"
            "  1. Verify data in trades table\n"
            "  2. Check v3_migration_log for any gaps\n"
            "  3. Start trade monitor on V4\n"
            "  4. Once V4 trade monitor is confirmed working,\n"
            "     stop V3 trade monitor to avoid duplicate monitoring"
        )


if __name__ == "__main__":
    main()
