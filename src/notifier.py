import time
import logging
import requests

log = logging.getLogger(__name__)

MAX_RETRIES = 3
DISCORD_MAX_LENGTH = 2000

class Notifier:
    """
    Sends notifications to a Discord channel via webhook.
    """
    def __init__(self, webhook_url=None, user_name="Unknown"):
        self.webhook_url = webhook_url
        self.user_name = user_name

        if not self.webhook_url:
            log.warning("Discord notifications disabled: no webhook URL provided.")

    def send_alert(self, subject, message_body, ping=None):
        """Sends an alert to Discord. If ping is a Discord user ID, prepends a mention."""
        if self.webhook_url:
            self._send_discord(subject, message_body, ping=ping)

    def _split_message(self, header, body):
        """Splits a message into chunks that fit Discord's character limit."""
        # First chunk includes the header
        first_prefix = f"{header}\n\n"
        cont_prefix = f"{header} (cont.)\n\n"

        chunks = []
        remaining = body

        while remaining:
            prefix = first_prefix if not chunks else cont_prefix
            max_body = DISCORD_MAX_LENGTH - len(prefix)

            if len(remaining) <= max_body:
                chunks.append(prefix + remaining)
                break

            # Split at last newline within limit to avoid cutting mid-line
            cut = remaining[:max_body].rfind('\n')
            if cut <= 0:
                cut = max_body

            chunks.append(prefix + remaining[:cut])
            remaining = remaining[cut:].lstrip('\n')

        return chunks

    def _send_discord(self, subject, message_body, ping=None):
        """Sends a message via Discord webhook with retries."""
        ping_prefix = f"<@{ping}> " if ping else ""
        header = f"{ping_prefix}**{subject}** (by {self.user_name})"
        full_message = f"{header}\n\n{message_body}"

        if len(full_message) <= DISCORD_MAX_LENGTH:
            self._post_message(full_message)
        else:
            chunks = self._split_message(header, message_body)
            for chunk in chunks:
                self._post_message(chunk)
                time.sleep(0.5)

    def _post_message(self, content):
        """Posts a single message to Discord with retries."""
        payload = {'content': content}
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(self.webhook_url, json=payload, timeout=10)
                if response.status_code == 429:
                    retry_after = response.json().get('retry_after', 2 ** attempt)
                    log.warning(f"Discord rate limited. Retrying in {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                log.info("Successfully sent Discord notification.")
                return
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    wait = 5 * 2 ** attempt
                    log.warning(f"Discord notification failed (attempt {attempt + 1}/{MAX_RETRIES}). Retrying in {wait}s... Error: {e}")
                    time.sleep(wait)
                else:
                    log.error(f"Failed to send Discord notification after {MAX_RETRIES} attempts: {e}")
