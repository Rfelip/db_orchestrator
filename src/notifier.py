"""Notification fan-out for the orchestrator.

Two channels supported, both optional and independent:

  - Discord, via webhook URL (DISCORD_WEBHOOK_URL).
  - Telegram, via bot token + chat ID (TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID).

`build_notifier(config)` returns a `Notifier` that fan-outs to whichever
channels are configured. If neither is set, returns a `NullNotifier` and
logs a warning. Callers always get a non-None object back.

The orchestrator only ever calls `send_alert(subject, body, ping=...)`;
the implementation behind it is irrelevant to the call site.
"""
from __future__ import annotations

import logging
import time
from typing import Protocol

import requests

log = logging.getLogger(__name__)

MAX_RETRIES = 3
DISCORD_MAX_LENGTH = 2000
TELEGRAM_MAX_LENGTH = 4096


class Notifier(Protocol):
    """Single method any concrete notifier must implement."""

    def send_alert(self, subject: str, message_body: str,
                    ping: str | None = None) -> None: ...


class NullNotifier:
    """No-op notifier used when no channel is configured."""

    def send_alert(self, subject: str, message_body: str,
                    ping: str | None = None) -> None:
        return


class DiscordNotifier:
    """Send messages to a Discord channel via webhook.

    Splits messages over the 2000-char limit; honours 429 rate-limit
    headers; retries with exponential backoff on transient errors.
    """

    def __init__(self, webhook_url: str, user_name: str = "Unknown") -> None:
        self.webhook_url = webhook_url
        self.user_name = user_name

    def send_alert(self, subject: str, message_body: str,
                    ping: str | None = None) -> None:
        ping_prefix = f"<@{ping}> " if ping else ""
        header = f"{ping_prefix}**{subject}** (by {self.user_name})"
        full_message = f"{header}\n\n{message_body}"

        if len(full_message) <= DISCORD_MAX_LENGTH:
            self._post(full_message)
            return

        chunks = self._split(header, message_body)
        for chunk in chunks:
            self._post(chunk)
            time.sleep(0.5)

    def _split(self, header: str, body: str) -> list[str]:
        first_prefix = f"{header}\n\n"
        cont_prefix = f"{header} (cont.)\n\n"
        chunks: list[str] = []
        remaining = body
        while remaining:
            prefix = first_prefix if not chunks else cont_prefix
            max_body = DISCORD_MAX_LENGTH - len(prefix)
            if len(remaining) <= max_body:
                chunks.append(prefix + remaining)
                break
            cut = remaining[:max_body].rfind('\n')
            if cut <= 0:
                cut = max_body
            chunks.append(prefix + remaining[:cut])
            remaining = remaining[cut:].lstrip('\n')
        return chunks

    def _post(self, content: str) -> None:
        payload = {'content': content}
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(self.webhook_url, json=payload, timeout=10)
                if response.status_code == 429:
                    retry_after = response.json().get('retry_after', 2 ** attempt)
                    log.warning("Discord rate limited. Retrying in %ss...", retry_after)
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                log.info("Discord notification sent.")
                return
            except requests.exceptions.RequestException as exc:
                if attempt < MAX_RETRIES - 1:
                    wait = 5 * 2 ** attempt
                    log.warning(
                        "Discord notification failed (attempt %d/%d). "
                        "Retrying in %ss... %s",
                        attempt + 1, MAX_RETRIES, wait, exc,
                    )
                    time.sleep(wait)
                else:
                    log.error(
                        "Failed to send Discord notification after %d attempts: %s",
                        MAX_RETRIES, exc,
                    )


class TelegramNotifier:
    """Send messages to a Telegram chat via the Bot API.

    Splits messages over the 4096-char limit and uses MarkdownV2 so the
    same `**bold**` / `` `code` `` markup the Discord side renders also
    renders here. (We translate Discord-style markdown to MarkdownV2
    rather than introducing a parallel format.)

    The `ping` argument from the call site is ignored: Telegram's
    user-mention syntax is incompatible with Discord IDs and the
    orchestrator only ever has Discord IDs handy. Pings are a Discord
    feature; on Telegram the chat itself is the recipient.
    """

    API_URL = "https://api.telegram.org/bot{token}/sendMessage"
    _MD_SPECIAL = r"_*[]()~`>#+-=|{}.!"

    def __init__(self, token: str, chat_id: str, user_name: str = "Unknown") -> None:
        self.url = self.API_URL.format(token=token)
        self.chat_id = chat_id
        self.user_name = user_name

    def send_alert(self, subject: str, message_body: str,
                    ping: str | None = None) -> None:
        # Telegram MarkdownV2 requires escaping a fixed set of special chars
        # outside of formatting markers. We render the message as plain
        # text + a bold subject; emoji-style markup from Discord (**bold**,
        # `code`) is converted, the rest is escaped.
        rendered = self._format(subject, message_body)
        # Telegram has a hard 4096-char ceiling per message.
        for chunk in self._split(rendered):
            self._post(chunk)
            time.sleep(0.3)

    def _format(self, subject: str, body: str) -> str:
        subj_md = f"*{self._escape(subject)}*"
        body_md = self._discord_to_telegram(body)
        return f"{subj_md}\n\n{body_md}\n\n_— {self._escape(self.user_name)}_"

    def _discord_to_telegram(self, text: str) -> str:
        # Convert Discord-style **bold** to Telegram-style *bold* before
        # escaping, then escape everything else. `code` spans pass through
        # unchanged on both. Newlines are preserved.
        out: list[str] = []
        i = 0
        while i < len(text):
            if text.startswith("**", i):
                end = text.find("**", i + 2)
                if end != -1:
                    out.append("*" + self._escape(text[i + 2:end]) + "*")
                    i = end + 2
                    continue
            if text[i] == "`":
                end = text.find("`", i + 1)
                if end != -1:
                    out.append("`" + text[i + 1:end] + "`")
                    i = end + 1
                    continue
            ch = text[i]
            out.append("\\" + ch if ch in self._MD_SPECIAL else ch)
            i += 1
        return "".join(out)

    def _escape(self, text: str) -> str:
        return "".join("\\" + c if c in self._MD_SPECIAL else c for c in text)

    def _split(self, text: str) -> list[str]:
        if len(text) <= TELEGRAM_MAX_LENGTH:
            return [text]
        chunks: list[str] = []
        remaining = text
        while remaining:
            if len(remaining) <= TELEGRAM_MAX_LENGTH:
                chunks.append(remaining)
                break
            cut = remaining[:TELEGRAM_MAX_LENGTH].rfind('\n')
            if cut <= 0:
                cut = TELEGRAM_MAX_LENGTH
            chunks.append(remaining[:cut])
            remaining = remaining[cut:].lstrip('\n')
        return chunks

    def _post(self, text: str) -> None:
        payload = {
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': 'MarkdownV2',
            'disable_web_page_preview': True,
        }
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(self.url, json=payload, timeout=10)
                if response.status_code == 429:
                    retry_after = int(response.json().get('parameters', {})
                                              .get('retry_after', 2 ** attempt))
                    log.warning("Telegram rate limited. Retrying in %ss...", retry_after)
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                log.info("Telegram notification sent.")
                return
            except requests.exceptions.RequestException as exc:
                if attempt < MAX_RETRIES - 1:
                    wait = 5 * 2 ** attempt
                    log.warning(
                        "Telegram notification failed (attempt %d/%d). "
                        "Retrying in %ss... %s",
                        attempt + 1, MAX_RETRIES, wait, exc,
                    )
                    time.sleep(wait)
                else:
                    log.error(
                        "Failed to send Telegram notification after %d attempts: %s",
                        MAX_RETRIES, exc,
                    )


class MultiNotifier:
    """Fan-out to multiple notifiers. Each child is called sequentially;
    a failure in one channel does not prevent the others from firing
    (each child handles its own retries and logging).
    """

    def __init__(self, children: list[Notifier]) -> None:
        self.children = children

    def send_alert(self, subject: str, message_body: str,
                    ping: str | None = None) -> None:
        for child in self.children:
            child.send_alert(subject, message_body, ping=ping)


def build_notifier(config: dict) -> Notifier:
    """Construct a notifier from a `notifier` config dict.

    Reads:
      - discord_webhook_url
      - telegram_bot_token, telegram_chat_id
      - user_name

    Returns a `MultiNotifier` if more than one channel is configured,
    a single notifier if exactly one is, and a `NullNotifier` if none
    are. The behaviour the call site sees is identical regardless of
    fan-out width — `send_alert(...)` always works.
    """
    user_name = config.get('user_name', 'Unknown')
    children: list[Notifier] = []

    discord_url = config.get('discord_webhook_url')
    if discord_url:
        children.append(DiscordNotifier(discord_url, user_name=user_name))

    tg_token = config.get('telegram_bot_token')
    tg_chat = config.get('telegram_chat_id')
    if tg_token and tg_chat:
        children.append(TelegramNotifier(tg_token, str(tg_chat), user_name=user_name))
    elif tg_token or tg_chat:
        log.warning(
            "Telegram partially configured: both TELEGRAM_BOT_TOKEN and "
            "TELEGRAM_CHAT_ID are required. Telegram notifications disabled."
        )

    if not children:
        log.warning("No notification channels configured (Discord or Telegram).")
        return NullNotifier()
    if len(children) == 1:
        return children[0]
    return MultiNotifier(children)
