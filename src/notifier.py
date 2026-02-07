import time
import logging
import requests

log = logging.getLogger(__name__)

MAX_RETRIES = 3

class Notifier:
    """
    Sends notifications to a Discord channel via webhook.
    """
    def __init__(self, webhook_url=None, user_name="Unknown"):
        self.webhook_url = webhook_url
        self.user_name = user_name

        if not self.webhook_url:
            log.warning("Discord notifications disabled: no webhook URL provided.")

    def send_alert(self, subject, message_body):
        """Sends an alert to Discord."""
        if self.webhook_url:
            self._send_discord(subject, message_body)

    def _send_discord(self, subject, message_body):
        """Sends a message via Discord webhook with retries."""
        full_message = f"**{subject}** (by {self.user_name})\n\n{message_body}"
        payload = {'content': full_message}
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
                    wait = 2 ** attempt
                    log.warning(f"Discord notification failed (attempt {attempt + 1}/{MAX_RETRIES}). Retrying in {wait}s... Error: {e}")
                    time.sleep(wait)
                else:
                    log.error(f"Failed to send Discord notification after {MAX_RETRIES} attempts: {e}")
