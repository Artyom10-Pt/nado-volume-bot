"""Simple Telegram alert sender for nado-volume-bot.

Usage:
  Set env vars TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.
  If not set, all alerts are silently skipped.

Create a bot via @BotFather, get the token.
Get chat_id by messaging @userinfobot or checking getUpdates.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.request

logger = logging.getLogger(__name__)


class TelegramAlerter:
    def __init__(self) -> None:
        self._token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self._chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self._enabled = bool(self._token and self._chat_id)
        if self._enabled:
            logger.info("telegram: alerts enabled (chat_id=%s)", self._chat_id)
        else:
            logger.info("telegram: alerts disabled (set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID to enable)")

    def _send_sync(self, text: str) -> None:
        if not self._enabled:
            return
        try:
            url = f"https://api.telegram.org/bot{self._token}/sendMessage"
            payload = json.dumps({"chat_id": self._chat_id, "text": text}).encode()
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception as exc:
            logger.warning("telegram: send failed: %s", exc)

    async def send(self, text: str) -> None:
        """Async send — runs in executor to avoid blocking the event loop."""
        if not self._enabled:
            return
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._send_sync, text)

    def send_sync(self, text: str) -> None:
        """Sync send — for use outside of async context."""
        self._send_sync(text)
