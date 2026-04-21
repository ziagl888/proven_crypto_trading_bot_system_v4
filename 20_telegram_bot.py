# 20_telegram_bot.py
# Telegram Outbox Consumer V4
#
# Full feature parity with V3 4_telegram_bot.py:
#   - Async (asyncio + python-telegram-bot)
#   - Per-channel rate limiting (3.1s between sends to same channel)
#   - Global rate limit (50ms = ~20 msg/s)
#   - Smart FIFO: skips blocked channels, sends to free ones immediately
#   - RetryAfter / Flood Control with exact backoff
#   - Attempt counter (max 3, then permanently failed)
#   - Chart dedup: only deletes image if no other unsent row needs it
#   - Batch size 50

from __future__ import annotations

import asyncio
import logging
import logging.handlers
import os
import re
import sys
import time

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

from telegram import Bot
from telegram.error import TelegramError, RetryAfter

from core.config import TELEGRAM_BOT_TOKEN
from core.database import get_db_connection
from core.schema import verify_schema
from core.shutdown import ShutdownHandler

FETCH_BATCH_SIZE            = 50
IDLE_SLEEP_S                = 2.0
MAX_ATTEMPTS                = 3
GLOBAL_MIN_INTERVAL_MS      = 50
PER_CHANNEL_MIN_INTERVAL_MS = 3100


def _ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        for col_sql in [
            "ALTER TABLE telegram_outbox ADD COLUMN IF NOT EXISTS attempts   INTEGER DEFAULT 0",
            "ALTER TABLE telegram_outbox ADD COLUMN IF NOT EXISTS failed     BOOLEAN DEFAULT FALSE",
            "ALTER TABLE telegram_outbox ADD COLUMN IF NOT EXISTS last_error TEXT",
        ]:
            cur.execute(col_sql)
    conn.commit()


def _fetch_batch() -> list[list]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, channel_id, message, image_path
                FROM   telegram_outbox
                WHERE  sent   = FALSE
                  AND  (failed = FALSE OR failed IS NULL)
                ORDER  BY id ASC
                LIMIT  %s
                """,
                (FETCH_BATCH_SIZE,),
            )
            return [list(row) for row in cur.fetchall()]
    finally:
        conn.close()


def _mark_sent(cur, msg_id: int, image_path) -> None:
    cur.execute("UPDATE telegram_outbox SET sent = TRUE WHERE id = %s", (msg_id,))
    _delete_chart_if_safe(cur, image_path, msg_id)


def _mark_failure(cur, msg_id: int, error: str, image_path) -> bool:
    cur.execute(
        """
        UPDATE telegram_outbox
        SET    attempts   = COALESCE(attempts, 0) + 1,
               last_error = %s,
               failed     = CASE WHEN COALESCE(attempts, 0) + 1 >= %s THEN TRUE ELSE failed END
        WHERE  id = %s
        RETURNING failed
        """,
        (error[:1000], MAX_ATTEMPTS, msg_id),
    )
    row = cur.fetchone()
    now_failed = bool(row and row[0])
    if now_failed:
        _delete_chart_if_safe(cur, image_path, msg_id)
    return now_failed


def _delete_chart_if_safe(cur, image_path, current_id: int) -> None:
    if not image_path:
        return
    try:
        cur.execute(
            "SELECT 1 FROM telegram_outbox WHERE image_path=%s AND sent=FALSE AND id!=%s LIMIT 1",
            (image_path, current_id),
        )
        if cur.fetchone() is not None:
            return
        if os.path.isfile(image_path):
            os.remove(image_path)
    except Exception as e:
        logger.debug(f"Chart delete error: {e}")


async def process_outbox() -> None:
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    me  = await bot.get_me()
    logger.info(f"Telegram bot connected: @{me.username}")

    conn = get_db_connection()
    try:
        _ensure_schema(conn)
    finally:
        conn.close()

    last_send_per_channel: dict[int, float] = {}
    last_global_send_ms: float = 0.0
    sent_total = failed_total = 0

    logger.info(f"Polling telegram_outbox (batch={FETCH_BATCH_SIZE}, idle={IDLE_SLEEP_S}s)...")

    while True:
        try:
            batch = _fetch_batch()
        except Exception as e:
            logger.error(f"DB fetch error: {e}")
            await asyncio.sleep(IDLE_SLEEP_S)
            continue

        if not batch:
            await asyncio.sleep(IDLE_SLEEP_S)
            continue

        batch_aborted = False

        while batch and not batch_aborted:
            now_ms = time.time() * 1000

            sendable_idx     = None
            earliest_unblock = None

            for idx, (msg_id, channel_id, text, image_path) in enumerate(batch):
                ch_ready  = last_send_per_channel.get(channel_id, 0.0) + PER_CHANNEL_MIN_INTERVAL_MS
                glb_ready = last_global_send_ms + GLOBAL_MIN_INTERVAL_MS
                ready_at  = max(ch_ready, glb_ready)
                if ready_at <= now_ms:
                    sendable_idx = idx
                    break
                if earliest_unblock is None or ready_at < earliest_unblock:
                    earliest_unblock = ready_at

            if sendable_idx is None:
                wait_s = min(max(0.05, (earliest_unblock - now_ms) / 1000), 5.0)
                await asyncio.sleep(wait_s)
                continue

            msg_id, channel_id, text, image_path = batch.pop(sendable_idx)

            conn = get_db_connection()
            try:
                with conn.cursor() as cur:
                    try:
                        if image_path and os.path.isfile(image_path):
                            with open(image_path, "rb") as photo:
                                await bot.send_photo(
                                    chat_id=channel_id,
                                    photo=photo,
                                    caption=text,
                                    parse_mode="HTML",
                                )
                        else:
                            if image_path and not os.path.isfile(image_path):
                                logger.warning(f"Image not found: {image_path} — text only")
                            await bot.send_message(
                                chat_id=channel_id,
                                text=text,
                                parse_mode="HTML",
                                disable_web_page_preview=True,
                            )

                        now_after = time.time() * 1000
                        last_send_per_channel[channel_id] = now_after
                        last_global_send_ms = now_after
                        _mark_sent(cur, msg_id, image_path)
                        conn.commit()
                        sent_total += 1
                        logger.info(
                            f"Sent message id={msg_id} → channel={channel_id} "
                            f"(total sent: {sent_total})"
                        )

                    except RetryAfter as e:
                        wait_s = float(e.retry_after)
                        logger.warning(f"Flood Control — waiting {wait_s:.0f}s (channel {channel_id})")
                        batch.insert(sendable_idx, [msg_id, channel_id, text, image_path])
                        last_send_per_channel[channel_id] = time.time() * 1000 + wait_s * 1000
                        await asyncio.sleep(wait_s + 1)
                        batch_aborted = True

                    except TelegramError as e:
                        err = str(e)
                        m   = re.search(r"Retry in (\d+)", err)
                        if m:
                            wait_s = int(m.group(1))
                            logger.warning(f"Flood Control (text) — waiting {wait_s}s")
                            batch.insert(sendable_idx, [msg_id, channel_id, text, image_path])
                            last_send_per_channel[channel_id] = time.time() * 1000 + wait_s * 1000
                            await asyncio.sleep(wait_s + 1)
                            batch_aborted = True
                            continue

                        if "chat not found" in err.lower():
                            logger.error(f"Chat {channel_id} not found — msg {msg_id} failed permanently")
                            cur.execute(
                                "UPDATE telegram_outbox SET failed=TRUE, last_error=%s WHERE id=%s",
                                (err[:1000], msg_id),
                            )
                            _delete_chart_if_safe(cur, image_path, msg_id)
                            conn.commit()
                            failed_total += 1
                            continue

                        now_failed = _mark_failure(cur, msg_id, err, image_path)
                        conn.commit()
                        if now_failed:
                            logger.error(f"Message id={msg_id} permanently failed after {MAX_ATTEMPTS} attempts: {err}")
                            failed_total += 1
                        else:
                            logger.warning(f"Message id={msg_id} error (will retry): {err}")

                    except Exception as e:
                        err = str(e)
                        now_failed = _mark_failure(cur, msg_id, err, image_path)
                        conn.commit()
                        if now_failed:
                            logger.error(f"Message id={msg_id} permanently failed: {err}")
                            failed_total += 1
                        else:
                            logger.warning(f"Message id={msg_id} error (will retry): {err}")

            except Exception as outer:
                logger.error(f"Outer send error msg {msg_id}: {outer}")
                try:
                    conn.rollback()
                except Exception:
                    pass
            finally:
                conn.close()

        await asyncio.sleep(0.1)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Telegram Bot V4 — starting")
    logger.info("=" * 60)

    schema = verify_schema()
    if schema["missing"]:
        logger.error(f"Missing tables: {schema['missing']}")
        sys.exit(1)
    logger.info(f"Schema OK — {len(schema['ok'])} tables verified.")

    try:
        asyncio.run(process_outbox())
    except KeyboardInterrupt:
        logger.info("Telegram Bot stopped (Ctrl+C).")


if __name__ == "__main__":
    main()
