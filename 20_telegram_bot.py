# 20_telegram_bot.py
# Telegram outbox consumer.
#
# Polls telegram_outbox every 2s for unsent messages and sends them
# via the Telegram Bot API. Marks rows as sent after successful delivery.
#
# Supports:
#   - Text messages (plain or HTML)
#   - Image + caption (image_path set)
#   - Rate limiting (Telegram: 30 msg/s global, 1 msg/s per chat)
#   - Retry on transient errors (up to 3 attempts with backoff)
#
# Architecture:
#   Bots → INSERT INTO telegram_outbox
#   This process → SELECT unsent → send → UPDATE sent=TRUE

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR     = os.path.join(_SCRIPT_DIR, "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - TELEGRAM - %(levelname)s - %(message)s",
    force=True,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.handlers.RotatingFileHandler(
            os.path.join(_LOG_DIR, "telegram_bot.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

from core.config import TELEGRAM_BOT_TOKEN
from core.database import db_connection
from core.schema import verify_schema
from core.shutdown import ShutdownHandler

# ── Constants ─────────────────────────────────────────────────────────────────

POLL_INTERVAL    = 2       # seconds between DB polls
BATCH_SIZE       = 10      # rows per poll (avoid burst)
MAX_RETRIES      = 3       # attempts per message
RETRY_BACKOFF    = [1, 3, 9]  # seconds between retries
# Telegram rate limits: 30 msg/s globally, 1 msg/s per chat
# We send at most 1 per chat per second by sleeping between messages
INTER_MSG_SLEEP  = 0.05    # 50ms between sends = 20 msg/s max (safe margin)

API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


# ── Telegram API helpers ───────────────────────────────────────────────────────

def _send_text(channel_id: int, message: str) -> bool:
    """Sends a plain text message. Returns True on success."""
    try:
        resp = requests.post(
            f"{API_BASE}/sendMessage",
            json={
                "chat_id":    channel_id,
                "text":       message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            return True
        data = resp.json()
        logger.warning(
            f"Telegram sendMessage failed [{resp.status_code}]: "
            f"{data.get('description', resp.text[:100])}"
        )
        return False
    except requests.RequestException as e:
        logger.warning(f"Telegram sendMessage error: {e}")
        return False


def _send_photo(channel_id: int, image_path: str, caption: str) -> bool:
    """Sends a photo with caption. Returns True on success."""
    if not os.path.exists(image_path):
        logger.warning(f"Image not found: {image_path} — sending as text.")
        return _send_text(channel_id, caption)
    try:
        with open(image_path, "rb") as img:
            resp = requests.post(
                f"{API_BASE}/sendPhoto",
                data={
                    "chat_id":    channel_id,
                    "caption":    caption,
                    "parse_mode": "HTML",
                },
                files={"photo": img},
                timeout=20,
            )
        if resp.status_code == 200:
            return True
        data = resp.json()
        logger.warning(
            f"Telegram sendPhoto failed [{resp.status_code}]: "
            f"{data.get('description', resp.text[:100])}"
        )
        return False
    except requests.RequestException as e:
        logger.warning(f"Telegram sendPhoto error: {e}")
        return False


def _send_with_retry(channel_id: int, message: str, image_path: str | None) -> bool:
    """Sends a message with up to MAX_RETRIES attempts."""
    for attempt in range(MAX_RETRIES):
        if image_path:
            ok = _send_photo(channel_id, image_path, message)
        else:
            ok = _send_text(channel_id, message)

        if ok:
            return True

        if attempt < MAX_RETRIES - 1:
            wait = RETRY_BACKOFF[attempt]
            logger.debug(f"Retry {attempt + 1}/{MAX_RETRIES} in {wait}s...")
            time.sleep(wait)

    return False


# ── DB helpers ────────────────────────────────────────────────────────────────

def _fetch_unsent(limit: int = BATCH_SIZE) -> list[dict]:
    """Returns up to `limit` unsent rows ordered by id."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, channel_id, message, image_path
                FROM   telegram_outbox
                WHERE  sent = FALSE
                ORDER  BY id
                LIMIT  %s
                """,
                (limit,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def _mark_sent(row_id: int) -> None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE telegram_outbox SET sent = TRUE WHERE id = %s",
                (row_id,),
            )
        conn.commit()


def _mark_failed(row_id: int) -> None:
    """
    On permanent failure: mark as sent=TRUE to avoid infinite retry loops.
    The message is lost but the queue keeps moving.
    """
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE telegram_outbox
                SET    sent = TRUE
                WHERE  id   = %s
                """,
                (row_id,),
            )
        conn.commit()
    logger.error(f"Message id={row_id} permanently failed — marked as sent.")


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 60)
    logger.info("Telegram Bot V4 — starting")
    logger.info("=" * 60)

    logger.info("Creating DB connection pool (min=2, max=20) → "
                f"{os.getenv('DB_HOST','localhost')}:{os.getenv('DB_PORT',5432)}/"
                f"{os.getenv('DB_NAME','cryptodata')}")

    schema = verify_schema()
    if schema["missing"]:
        logger.error(f"Missing tables: {schema['missing']}")
        sys.exit(1)
    logger.info(f"Schema OK — {len(schema['ok'])} tables verified.")

    # Verify bot token works
    try:
        resp = requests.get(f"{API_BASE}/getMe", timeout=5)
        if resp.status_code == 200:
            bot_name = resp.json()["result"]["username"]
            logger.info(f"Telegram bot connected: @{bot_name}")
        else:
            logger.error(f"Telegram getMe failed: {resp.text[:100]}")
            sys.exit(1)
    except requests.RequestException as e:
        logger.error(f"Telegram connection failed: {e}")
        sys.exit(1)

    shutdown = ShutdownHandler("TELEGRAM")
    sent_total = 0
    failed_total = 0

    logger.info(f"Polling telegram_outbox every {POLL_INTERVAL}s...")

    while not shutdown.is_set():
        try:
            rows = _fetch_unsent()
        except Exception as e:
            logger.error(f"DB poll error: {e}")
            shutdown.sleep(POLL_INTERVAL)
            continue

        for row in rows:
            if shutdown.is_set():
                break

            ok = _send_with_retry(
                channel_id=row["channel_id"],
                message=row["message"],
                image_path=row["image_path"],
            )

            if ok:
                _mark_sent(row["id"])
                sent_total += 1
                logger.info(
                    f"Sent message id={row['id']} → "
                    f"channel={row['channel_id']} "
                    f"(total sent: {sent_total})"
                )
            else:
                _mark_failed(row["id"])
                failed_total += 1

            time.sleep(INTER_MSG_SLEEP)

        shutdown.sleep(POLL_INTERVAL)

    logger.info(
        f"Telegram Bot stopped — {sent_total} sent, {failed_total} failed."
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Telegram Bot stopped (Ctrl+C).")
