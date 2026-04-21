#!/usr/bin/env python3
# 92_migrate_v3_market_data.py
# Migrates V3 whale trades and funding rates from JSON files to V4 DB.
#
# V3 data locations (Windows server):
#   Whale trades:   C:\_BOT\proven_crypto_bot\whale_data\whale_trades_YYYY-MM-DD.json
#   Funding rates:  C:\_BOT\proven_crypto_bot\funding_data\funding_history_YYYY-MM-DD.json
#
# V3 record formats:
#   Whale:   {"ts": 1234567890.123, "sym": "BTCUSDT", "dir": "LONG", "usd": 125000.0, "prc": 42000.0}
#   Funding: {"ts": 1234567890.123, "sym": "BTCUSDT", "rate": 0.00010}
#
# V4 target tables:
#   whale_trades  (id, symbol, ts, direction, usd_value, price, qty)
#   funding_rates (symbol, ts, rate)
#
# Notes:
#   - V3 whale records have no qty field → stored as 0.0
#   - funding_rates has PRIMARY KEY (symbol, ts) → ON CONFLICT DO NOTHING
#   - whale_trades has no unique constraint → dedup via migration_log table
#   - Safe to run multiple times — already-migrated files are skipped
#
# Usage:
#   python 92_migrate_v3_market_data.py --dry-run
#   python 92_migrate_v3_market_data.py
#   python 92_migrate_v3_market_data.py --whale-only
#   python 92_migrate_v3_market_data.py --funding-only
#   python 92_migrate_v3_market_data.py --whale-dir "D:\other\path\whale_data"

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys
import time
from pathlib import Path

from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - MIG_MARKET - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── V3 default paths ──────────────────────────────────────────────────────────
# Adjust these if V3 bot is installed in a different location.

V3_ROOT         = Path(r"C:\_BOT\proven_crypto_bot")
V3_WHALE_DIR    = V3_ROOT / "whale_data"
V3_FUNDING_DIR  = V3_ROOT / "funding_data"

# ── Batch size for DB inserts ─────────────────────────────────────────────────
BATCH_SIZE = 5_000


# ── Migration log helpers ─────────────────────────────────────────────────────

def _ensure_migration_log(conn) -> None:
    """Creates a migration log table to track which files have been processed."""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS v3_market_migration_log (
                id           SERIAL PRIMARY KEY,
                source_type  TEXT        NOT NULL,  -- 'whale' or 'funding'
                source_file  TEXT        NOT NULL,
                records_in   INTEGER     NOT NULL DEFAULT 0,
                records_ok   INTEGER     NOT NULL DEFAULT 0,
                records_skip INTEGER     NOT NULL DEFAULT 0,
                migrated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (source_type, source_file)
            )
            """
        )
    conn.commit()


def _already_migrated(conn, source_type: str, source_file: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM v3_market_migration_log
            WHERE source_type = %s AND source_file = %s
            """,
            (source_type, source_file),
        )
        return cur.fetchone() is not None


def _log_migration(conn, source_type: str, source_file: str,
                    records_in: int, records_ok: int, records_skip: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO v3_market_migration_log
                (source_type, source_file, records_in, records_ok, records_skip)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source_type, source_file) DO UPDATE SET
                records_in   = EXCLUDED.records_in,
                records_ok   = EXCLUDED.records_ok,
                records_skip = EXCLUDED.records_skip,
                migrated_at  = NOW()
            """,
            (source_type, source_file, records_in, records_ok, records_skip),
        )
    conn.commit()


# ── Whale migration ───────────────────────────────────────────────────────────

def _migrate_whale_file(conn, filepath: Path, dry_run: bool) -> tuple[int, int, int]:
    """
    Migrates one whale_trades_YYYY-MM-DD.json file.
    Returns (records_in, records_ok, records_skipped).
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            records = json.load(f)
    except Exception as e:
        logger.error(f"  Cannot read {filepath.name}: {e}")
        return 0, 0, 0

    if not records:
        return 0, 0, 0

    rows = []
    skipped = 0

    for rec in records:
        try:
            ts  = datetime.datetime.fromtimestamp(
                float(rec["ts"]), tz=datetime.timezone.utc
            )
            sym = str(rec["sym"])
            dir_ = str(rec["dir"]).upper()
            usd  = float(rec["usd"])
            prc  = float(rec["prc"])
            qty  = float(rec.get("qty", 0.0))   # V3 has no qty → 0.0

            if dir_ not in ("LONG", "SHORT"):
                skipped += 1
                continue
            if usd <= 0 or prc <= 0:
                skipped += 1
                continue

            rows.append((sym, ts, dir_, usd, prc, qty))
        except Exception:
            skipped += 1
            continue

    records_in = len(records)
    records_ok = len(rows)

    if dry_run:
        logger.info(
            f"  [DRY RUN] {filepath.name}: "
            f"{records_in} records → {records_ok} valid, {skipped} skipped"
        )
        return records_in, records_ok, skipped

    # Batch insert
    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO whale_trades
                        (symbol, ts, direction, usd_value, price, qty)
                    VALUES %s
                    """,
                    batch,
                    template="(%s, %s, %s, %s, %s, %s)",
                )
            conn.commit()
            inserted += len(batch)
        except Exception as e:
            conn.rollback()
            logger.error(f"  Batch insert error at offset {i}: {e}")

    return records_in, inserted, skipped


def migrate_whale(conn, whale_dir: Path, dry_run: bool) -> None:
    """Migrates all whale trade JSON files from the given directory."""
    logger.info(f"=== Whale Migration from {whale_dir} ===")

    if not whale_dir.exists():
        logger.error(f"Directory not found: {whale_dir}")
        logger.error("Check V3_WHALE_DIR path or use --whale-dir to override.")
        return

    files = sorted(whale_dir.glob("whale_trades_*.json"))
    if not files:
        logger.warning(f"No whale_trades_*.json files found in {whale_dir}")
        return

    logger.info(f"Found {len(files)} whale trade files.")

    total_in = total_ok = total_skip = 0
    for fp in files:
        if not dry_run and _already_migrated(conn, "whale", fp.name):
            logger.info(f"  SKIP (already migrated): {fp.name}")
            continue

        t0 = time.time()
        rec_in, rec_ok, rec_skip = _migrate_whale_file(conn, fp, dry_run)
        elapsed = time.time() - t0

        total_in   += rec_in
        total_ok   += rec_ok
        total_skip += rec_skip

        if not dry_run and rec_in > 0:
            _log_migration(conn, "whale", fp.name, rec_in, rec_ok, rec_skip)

        logger.info(
            f"  {fp.name}: {rec_in} in → {rec_ok} inserted, "
            f"{rec_skip} skipped  ({elapsed:.1f}s)"
        )

    logger.info(
        f"Whale migration complete: "
        f"{total_in:,} total → {total_ok:,} inserted, {total_skip:,} skipped"
    )


# ── Funding migration ─────────────────────────────────────────────────────────

def _migrate_funding_file(conn, filepath: Path, dry_run: bool) -> tuple[int, int, int]:
    """
    Migrates one funding_history_YYYY-MM-DD.json file.
    Returns (records_in, records_ok, records_skipped).
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            records = json.load(f)
    except Exception as e:
        logger.error(f"  Cannot read {filepath.name}: {e}")
        return 0, 0, 0

    if not records:
        return 0, 0, 0

    rows = []
    skipped = 0

    for rec in records:
        try:
            ts   = datetime.datetime.fromtimestamp(
                float(rec["ts"]), tz=datetime.timezone.utc
            )
            sym  = str(rec["sym"])
            rate = float(rec["rate"])

            if not sym.endswith("USDT"):
                skipped += 1
                continue

            rows.append((sym, ts, rate))
        except Exception:
            skipped += 1
            continue

    records_in = len(records)
    records_ok = len(rows)

    if dry_run:
        logger.info(
            f"  [DRY RUN] {filepath.name}: "
            f"{records_in} records → {records_ok} valid, {skipped} skipped"
        )
        return records_in, records_ok, skipped

    # Batch insert — ON CONFLICT DO NOTHING (PRIMARY KEY symbol, ts)
    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO funding_rates (symbol, ts, rate)
                    VALUES %s
                    ON CONFLICT (symbol, ts) DO NOTHING
                    """,
                    batch,
                    template="(%s, %s, %s)",
                )
            conn.commit()
            inserted += len(batch)
        except Exception as e:
            conn.rollback()
            logger.error(f"  Batch insert error at offset {i}: {e}")

    return records_in, inserted, skipped


def migrate_funding(conn, funding_dir: Path, dry_run: bool) -> None:
    """Migrates all funding history JSON files from the given directory."""
    logger.info(f"=== Funding Migration from {funding_dir} ===")

    if not funding_dir.exists():
        logger.error(f"Directory not found: {funding_dir}")
        logger.error("Check V3_FUNDING_DIR path or use --funding-dir to override.")
        return

    files = sorted(funding_dir.glob("funding_history_*.json"))
    if not files:
        logger.warning(f"No funding_history_*.json files found in {funding_dir}")
        return

    logger.info(f"Found {len(files)} funding history files.")

    total_in = total_ok = total_skip = 0
    for fp in files:
        if not dry_run and _already_migrated(conn, "funding", fp.name):
            logger.info(f"  SKIP (already migrated): {fp.name}")
            continue

        t0 = time.time()
        rec_in, rec_ok, rec_skip = _migrate_funding_file(conn, fp, dry_run)
        elapsed = time.time() - t0

        total_in   += rec_in
        total_ok   += rec_ok
        total_skip += rec_skip

        if not dry_run and rec_in > 0:
            _log_migration(conn, "funding", fp.name, rec_in, rec_ok, rec_skip)

        logger.info(
            f"  {fp.name}: {rec_in} in → {rec_ok} inserted, "
            f"{rec_skip} skipped  ({elapsed:.1f}s)"
        )

    logger.info(
        f"Funding migration complete: "
        f"{total_in:,} total → {total_ok:,} inserted, {total_skip:,} skipped"
    )


# ── DB stats ──────────────────────────────────────────────────────────────────

def _print_db_stats(conn) -> None:
    """Prints row counts and date ranges after migration."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)              AS total,
                MIN(ts)               AS oldest,
                MAX(ts)               AS newest,
                COUNT(DISTINCT symbol) AS symbols
            FROM whale_trades
            """
        )
        r = cur.fetchone()
        logger.info(
            f"whale_trades:  {r[0]:,} rows | "
            f"{r[3]} symbols | "
            f"{r[1].date() if r[1] else 'N/A'} → {r[2].date() if r[2] else 'N/A'}"
        )

        cur.execute(
            """
            SELECT
                COUNT(*)              AS total,
                MIN(ts)               AS oldest,
                MAX(ts)               AS newest,
                COUNT(DISTINCT symbol) AS symbols
            FROM funding_rates
            """
        )
        r = cur.fetchone()
        logger.info(
            f"funding_rates: {r[0]:,} rows | "
            f"{r[3]} symbols | "
            f"{r[1].date() if r[1] else 'N/A'} → {r[2].date() if r[2] else 'N/A'}"
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate V3 whale trades and funding rates to V4 DB."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Scan files and report counts without writing to DB."
    )
    parser.add_argument(
        "--whale-only", action="store_true",
        help="Migrate only whale trades."
    )
    parser.add_argument(
        "--funding-only", action="store_true",
        help="Migrate only funding rates."
    )
    parser.add_argument(
        "--whale-dir", type=str, default=None,
        help=f"Override whale data directory (default: {V3_WHALE_DIR})"
    )
    parser.add_argument(
        "--funding-dir", type=str, default=None,
        help=f"Override funding data directory (default: {V3_FUNDING_DIR})"
    )
    args = parser.parse_args()

    whale_dir   = Path(args.whale_dir)   if args.whale_dir   else V3_WHALE_DIR
    funding_dir = Path(args.funding_dir) if args.funding_dir else V3_FUNDING_DIR

    do_whale   = not args.funding_only
    do_funding = not args.whale_only

    logger.info("=" * 60)
    logger.info(
        f"V3 Market Data Migration "
        f"({'DRY RUN' if args.dry_run else 'LIVE'})"
    )
    logger.info(f"  Whale:   {'YES' if do_whale   else 'SKIP'}  ({whale_dir})")
    logger.info(f"  Funding: {'YES' if do_funding else 'SKIP'}  ({funding_dir})")
    logger.info("=" * 60)

    # Import here so script is importable without running
    from dotenv import load_dotenv
    load_dotenv()
    from core.database import get_db_connection

    conn = get_db_connection()
    try:
        if not args.dry_run:
            _ensure_migration_log(conn)

        t_start = time.time()

        if do_whale:
            migrate_whale(conn, whale_dir, args.dry_run)

        if do_funding:
            migrate_funding(conn, funding_dir, args.dry_run)

        elapsed = time.time() - t_start
        logger.info(f"\nTotal migration time: {elapsed:.1f}s")

        if not args.dry_run:
            logger.info("\nDB stats after migration:")
            _print_db_stats(conn)

        if args.dry_run:
            logger.info("\n[DRY RUN] Nothing was written to DB.")
            logger.info("Re-run without --dry-run to execute the migration.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
