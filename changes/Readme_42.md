# PR #42 — 20_telegram_bot.py: outbox consumer

**Branch:** `dev`
**Date:** 2026-04-20

## New: `20_telegram_bot.py`

Polls `telegram_outbox` every 2s and delivers messages via Telegram Bot API.

### Features
- Text messages (HTML parse mode)
- Photo + caption (image_path set in outbox row)
- Retry logic: up to 3 attempts with backoff (1s → 3s → 9s)
- Rate limiting: 50ms between sends (max 20 msg/s, well under Telegram's 30/s limit)
- Permanent failure handling: marks row sent=TRUE after 3 failures to avoid queue block
- Graceful shutdown via ShutdownHandler
- Rotating logs: 10MB × 5 backups

### Message flow
```
Bot script
  → INSERT INTO telegram_outbox (channel_id, message, image_path)
  
20_telegram_bot.py
  → SELECT unsent rows (batch=10)
  → send via Telegram API
  → UPDATE sent=TRUE
```

### Added to watchdog
`20_telegram_bot.py` added to PROCESSES list (start_delay=20s).
