# PR #66 — Telegram Bot: delete chart file after successful send

**Branch:** `dev`
**Date:** 2026-04-20

## Change (`20_telegram_bot.py`)
After successfully sending a photo message, the chart PNG file is now
deleted from disk. Prevents `charts/` directory from filling up over time.
