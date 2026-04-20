# core/system_state.py
# Shared key-value store for inter-process coordination.
# Wraps the system_state DB table with simple get/set/wait helpers.

from __future__ import annotations

import datetime
import logging
import time

from core.database import db_connection

logger = logging.getLogger(__name__)

# ── Known keys ────────────────────────────────────────────────────────────────
KEY_BACKFILL_DONE = "initial_backfill_done"


def set_state(key: str, value: str = "true") -> None:
    """Upserts a key-value pair into system_state."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO system_state (key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value      = EXCLUDED.value,
                    updated_at = NOW()
                """,
                (key, value),
            )
        conn.commit()


def get_state(key: str) -> str | None:
    """Returns the value for a key, or None if not set."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM system_state WHERE key = %s",
                (key,),
            )
            row = cur.fetchone()
    return row[0] if row else None


def is_set(key: str) -> bool:
    """Returns True if key exists and value is 'true'."""
    return get_state(key) == "true"


def wait_for(
    key: str,
    poll_interval: int = 30,
    log_interval: int = 300,
    timeout: int | None = None,
) -> bool:
    """
    Blocks until system_state[key] == 'true'.
    Uses threading.Event.wait() so it responds immediately to
    SIGINT (Ctrl+C) instead of blocking in time.sleep().

    poll_interval: seconds between DB checks
    log_interval:  seconds between log messages
    timeout:       max seconds to wait (None = wait forever)
    Returns True when condition met, False on timeout or interrupt.
    """
    import threading
    _wake = threading.Event()

    start    = time.time()
    last_log = start

    while True:
        try:
            if is_set(key):
                return True
        except Exception as e:
            logger.warning(f"system_state check failed: {e}")

        now = time.time()

        if timeout and (now - start) >= timeout:
            logger.warning(
                f"Timed out waiting for system_state[{key!r}] "
                f"after {timeout}s."
            )
            return False

        if now - last_log >= log_interval:
            elapsed = int(now - start)
            logger.info(f"Waiting for {key!r} ... ({elapsed}s elapsed)")
            last_log = now

        # Interruptible sleep — wakes immediately on Ctrl+C
        try:
            _wake.wait(timeout=poll_interval)
        except (KeyboardInterrupt, SystemExit):
            logger.info(f"wait_for({key!r}) interrupted — shutting down.")
            return False

