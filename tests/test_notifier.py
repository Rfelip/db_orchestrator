"""Smoke tests for the notifier fan-out + Telegram MarkdownV2 escaping."""
from unittest.mock import patch, MagicMock

from src.notifier import (
    DiscordNotifier, TelegramNotifier, MultiNotifier, NullNotifier,
    build_notifier,
)


class TestBuildNotifier:
    def test_no_channels_returns_null(self):
        n = build_notifier({})
        assert isinstance(n, NullNotifier)
        # NullNotifier should accept calls without erroring.
        n.send_alert("subject", "body")

    def test_only_discord_returns_discord(self):
        n = build_notifier({"discord_webhook_url": "https://discord.test/x"})
        assert isinstance(n, DiscordNotifier)

    def test_only_telegram_returns_telegram(self):
        n = build_notifier({
            "telegram_bot_token": "123:abc",
            "telegram_chat_id": "456",
        })
        assert isinstance(n, TelegramNotifier)

    def test_partial_telegram_falls_back_to_null(self):
        # Token without chat_id is incomplete; the factory must not
        # silently use a half-configured channel.
        n = build_notifier({"telegram_bot_token": "123:abc"})
        assert isinstance(n, NullNotifier)

    def test_both_returns_multi(self):
        n = build_notifier({
            "discord_webhook_url": "https://discord.test/x",
            "telegram_bot_token": "123:abc",
            "telegram_chat_id": "456",
        })
        assert isinstance(n, MultiNotifier)
        assert len(n.children) == 2


class TestMultiNotifierFanOut:
    def test_send_alert_calls_each_child(self):
        a, b = MagicMock(), MagicMock()
        multi = MultiNotifier([a, b])
        multi.send_alert("subject", "body", ping="user_id")
        a.send_alert.assert_called_once_with("subject", "body", ping="user_id")
        b.send_alert.assert_called_once_with("subject", "body", ping="user_id")


class TestTelegramFormatting:
    def _format(self, body: str, subject: str = "S") -> str:
        n = TelegramNotifier(token="t", chat_id="c", user_name="U")
        return n._format(subject, body)

    def test_special_chars_escaped(self):
        # MarkdownV2 needs to escape these — failing to do so makes the
        # Bot API reject the message with 400.
        out = self._format("hello (world) [test]!")
        assert "\\(" in out and "\\)" in out
        assert "\\[" in out and "\\]" in out
        assert "\\!" in out

    def test_double_star_becomes_single_star(self):
        # Discord uses **bold**; Telegram MarkdownV2 uses *bold*.
        out = self._format("**important** thing")
        assert "*important*" in out
        assert "**" not in out.replace("*important*", "")

    def test_backticks_pass_through(self):
        # Inline code spans are identical between Discord and Telegram MD.
        out = self._format("see `step_name` for details")
        assert "`step_name`" in out


class TestTelegramSplitting:
    def test_short_message_single_chunk(self):
        n = TelegramNotifier(token="t", chat_id="c")
        assert len(n._split("hello")) == 1

    def test_long_message_splits_on_newlines(self):
        n = TelegramNotifier(token="t", chat_id="c")
        big = "line\n" * 2000  # ~10000 chars
        chunks = n._split(big)
        assert len(chunks) > 1
        assert all(len(c) <= 4096 for c in chunks)


class TestPostsAreNotMadeInTests:
    """Confirm the constructors don't accidentally make HTTP calls.

    A regression where __init__ POSTs to the webhook would burn webhook
    quota every test run, so this guards the construction path."""

    @patch("src.notifier.requests.post")
    def test_discord_init_no_post(self, mock_post):
        DiscordNotifier("https://discord.test/x")
        assert not mock_post.called

    @patch("src.notifier.requests.post")
    def test_telegram_init_no_post(self, mock_post):
        TelegramNotifier(token="t", chat_id="c")
        assert not mock_post.called
