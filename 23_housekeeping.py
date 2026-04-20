#!/usr/bin/env python3
# 23_housekeeping.py
# Nightly housekeeping — runs once at 03:00 UTC, then every 24 hours.
# Also runs once immediately on startup.
#
# Tasks (in order):
#   1. Update coin list  (fetch from Binance, update coins.json)
#   2. Update max leverage (fetch from Binance, update max_leverage.json)
#   3. OHLCV cleanup     (delete rows beyond retention limits)
#   4. Outbox cleanup    (delete sent telegram_outbox rows older than 7 days)

from __future__ import annotations

import datetime
import logging
import logging.handlers
import os
import sys
import time

from core.bootstrap import (
    fetch_active_coins, save_coins,
    fetch_max_leverage, save_max_leverage,
    load_coins,
)
from core.config import INGEST_TIMEFRAMES
from core.shutdown import ShutdownHandler
from core.database import db_connection
from core.schema import verify_schema

# ── Logging ───────────────────────────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR     = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - HOUSEKEEPING - %(levelname)s - %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            os.path.join(_LOG_DIR, "housekeeping.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

# ── Retention limits ──────────────────────────────────────────────────────────
# Rows older than this are deleted from ohlcv_* tables.
# Keeps DB size manageable while preserving enough history for indicators.

RETENTION: dict[str, int] = {
    "5m":  30,    # days
    "15m": 30,
    "30m": 365,
    "1h":  365,
    "2h":  365,
    "4h":  365,
    "6h":  1095,  # 3 years
    "8h":  1095,
    "12h": 1095,
    "1d":  1095,
    "3d":  1095,
    "1w":  1095,
    "1M":  1095,
}

# Scheduled run time (UTC hour)
RUN_HOUR_UTC = 3


# ── Tasks ─────────────────────────────────────────────────────────────────────

def task_update_coins() -> None:
    """Fetches fresh coin list from Binance and updates coins.json."""
    logger.info("Task 1/4: Updating coin list...")
    old_coins = set(load_coins())
    new_coins = fetch_active_coins()
    save_coins(new_coins)

    added   = set(new_coins) - old_coins
    removed = old_coins - set(new_coins)

    if added:
        logger.info(f"  New coins added: {len(added)} — {sorted(added)}")
    if removed:
        logger.info(f"  Coins removed:  {len(removed)} — {sorted(removed)}")
    if not added and not removed:
        logger.info(f"  No changes — {len(new_coins)} coins.")


def task_update_leverage() -> None:
    """Fetches max leverage brackets from Binance and updates max_leverage.json."""
    logger.info("Task 2/4: Updating max leverage...")
    coins        = load_coins()
    leverage_map = fetch_max_leverage(coins)
    if leverage_map:
        save_max_leverage(leverage_map)
        logger.info(f"  Updated leverage for {len(leverage_map)} symbols.")
    else:
        logger.warning("  Leverage fetch failed — keeping existing file.")


def task_ohlcv_cleanup() -> None:
    """
    Deletes OHLCV rows beyond the retention limit for each timeframe.
    Uses a single DELETE per timeframe — no per-symbol loop.
    """
    logger.info("Task 3/4: OHLCV cleanup...")
    total_deleted = 0

    with db_connection() as conn:
        for tf in INGEST_TIMEFRAMES:
            days = RETENTION.get(tf)
            if days is None:
                continue

            cutoff = datetime.datetime.now(datetime.timezone.utc) - \
                     datetime.timedelta(days=days)

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM ohlcv_{tf} WHERE open_time < %s",
                        (cutoff,),
                    )
                    deleted = cur.rowcount
                conn.commit()

                if deleted > 0:
                    logger.info(
                        f"  ohlcv_{tf}: deleted {deleted:,} rows "
                        f"(older than {cutoff.strftime('%Y-%m-%d')})"
                    )
                total_deleted += deleted

            except Exception as e:
                conn.rollback()
                logger.error(f"  ohlcv_{tf} cleanup failed: {e}")

    logger.info(f"  OHLCV cleanup done — {total_deleted:,} rows deleted total.")


def task_outbox_cleanup() -> None:
    """Deletes sent telegram_outbox rows older than 7 days."""
    logger.info("Task 4/4: Telegram outbox cleanup...")
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)

    with db_connection() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM telegram_outbox WHERE sent = TRUE AND created_at < %s",
                    (cutoff,),
                )
                deleted = cur.rowcount
            conn.commit()
            logger.info(f"  Outbox cleanup done — {deleted:,} rows deleted.")
        except Exception as e:
            conn.rollback()
            logger.error(f"  Outbox cleanup failed: {e}")


# ── Main run ──────────────────────────────────────────────────────────────────

def run_all_tasks() -> None:
    """Runs all housekeeping tasks sequentially."""
    logger.info("=" * 50)
    logger.info(
        f"Housekeeping run starting — "
        f"{datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    logger.info("=" * 50)

    t_start = time.time()

    try:
        task_update_coins()
    except Exception as e:
        logger.error(f"task_update_coins failed: {e}")

    try:
        task_update_leverage()
    except Exception as e:
        logger.error(f"task_update_leverage failed: {e}")

    try:
        task_ohlcv_cleanup()
    except Exception as e:
        logger.error(f"task_ohlcv_cleanup failed: {e}")

    try:
        task_outbox_cleanup()
    except Exception as e:
        logger.error(f"task_outbox_cleanup failed: {e}")

    elapsed = time.time() - t_start
    logger.info(f"Housekeeping complete in {elapsed:.1f}s.")


def _seconds_until_next_run() -> float:
    """Returns seconds until the next 03:00 UTC."""
    now  = datetime.datetime.now(datetime.timezone.utc)
    next_run = now.replace(
        hour=RUN_HOUR_UTC, minute=0, second=0, microsecond=0
    )
    if next_run <= now:
        next_run += datetime.timedelta(days=1)
    return (next_run - now).total_seconds()


def main() -> None:
    logger.info("Housekeeping starting...")

    # Schema check
    result = verify_schema()
    if result["missing"]:
        logger.critical(f"Missing DB tables: {result['missing']}. Aborting.")
        sys.exit(1)

    # Run immediately on startup
    run_all_tasks()

    # Then run every day at 03:00 UTC
    shutdown = ShutdownHandler("HOUSEKEEPING")

    while not shutdown.is_set():
        wait = _seconds_until_next_run()
        next_dt = datetime.datetime.now(datetime.timezone.utc) + \
                  datetime.timedelta(seconds=wait)
        logger.info(
            f"Next housekeeping run at "
            f"{next_dt.strftime('%Y-%m-%d %H:%M UTC')} "
            f"(in {wait/3600:.1f}h)."
        )
        if shutdown.sleep(wait):
            break
        run_all_tasks()


if __name__ == "__main__":
    main()




