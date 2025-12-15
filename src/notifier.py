import logging
import requests

log = logging.getLogger(__name__)

class Notifier:
    """
    A reusable class to send notifications via Telegram.
    Configuration is provided during instantiation.
    """
    def __init__(self, telegram_token=None, telegram_chat_id=None):
        """
        Initializes the Notifier with all necessary settings.

        Args:
            telegram_token (str): Your Telegram bot's API token.
            telegram_chat_id (str): The chat ID to send the message to.
        """
        # Telegram settings
        self.telegram_token = telegram_token
        self.telegram_chat_id = telegram_chat_id
        
        if not self.telegram_token or not self.telegram_chat_id:
            log.warning("Telegram notifications disabled due to missing credentials.")

    def send_alert(self, subject, message_body):
        """Sends an alert via Telegram."""
        if self.telegram_token and self.telegram_chat_id:
            self._send_telegram(subject, message_body)

    def _send_telegram(self, subject, message_body):
        """Sends a message via a Telegram bot."""
        # Format message with Markdown for better readability
        full_message = f"<b>{subject}</b>\n\n{message_body}"
        api_url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            'chat_id': self.telegram_chat_id,
            'text': full_message,
            'parse_mode': 'HTML'
        }
        try:
            response = requests.post(api_url, json=payload, timeout=10)
            response.raise_for_status()
            log.info("Successfully sent Telegram notification.")
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to send Telegram notification: {e}")