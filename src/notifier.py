import logging
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

log = logging.getLogger(__name__)

class Notifier:
    """
    A reusable class to send notifications via Telegram and Email.
    Configuration is provided during instantiation.
    """
    def __init__(self):
        """
        Initializes the Notifier with all necessary settings.

        Args:
            enable_telegram (bool): Flag to enable/disable Telegram alerts.
            telegram_token (str): Your Telegram bot's API token.
            telegram_chat_id (str): The chat ID to send the message to.
            enable_email (bool): Flag to enable/disable Email alerts.
            email_host (str): SMTP server host (e.g., 'smtp.gmail.com').
            email_port (int): SMTP server port (e.g., 587 for TLS).
            email_user (str): The email address to send from.
            email_password (str): The password or app-specific password for the email account.
            email_recipient (str): The email address to send the alert to.
        """
        # Telegram settings
        self.enable_telegram = True
        self.telegram_token = "8136435118:AAFKu2W3bWpVumdDbrX4-G4ACTchpqJDK3Y"
        self.telegram_chat_id = "1657288716"

        # Email settings
        self.enable_email = False
        #self.email_host = email_host
        #self.email_port = email_port
        #self.email_user = email_user
        #self.email_password = email_password
        #self.email_recipient = email_recipient

    def send_alert(self, subject, message_body):
        """Sends an alert via all enabled channels."""
        log.info("Attempting to send notification...")
        if self.enable_telegram:
            self._send_telegram(subject, message_body)
        if self.enable_email:
            self._send_email(subject, message_body)

    def _send_telegram(self, subject, message_body):
        """Sends a message via a Telegram bot."""
        if not all([self.telegram_token, self.telegram_chat_id]):
            log.error("Telegram credentials are not fully set. Cannot send message.")
            return

        # Format message with Markdown for better readability
        full_message = f"*{subject}*\n\n{message_body}"
        api_url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            'chat_id': self.telegram_chat_id,
            'text': full_message,
            'parse_mode': 'HTML'
        }
        try:
            response = requests.post(api_url, json=payload, timeout=10)
            response.raise_for_status()  # Raise an exception for bad status codes
            log.info("Successfully sent Telegram notification.")
        except requests.exceptions.RequestException as e:
            log.error(f"Failed to send Telegram notification: {e}")

    def _send_email(self, subject, message_body):
        """Sends an email message."""
        if not all([self.email_host, self.email_port, self.email_user, self.email_password, self.email_recipient]):
            log.error("Email credentials are not fully set. Cannot send email.")
            return

        msg = MIMEMultipart()
        msg['From'] = self.email_user
        msg['To'] = self.email_recipient
        msg['Subject'] = subject
        msg.attach(MIMEText(message_body, 'plain'))

        try:
            with smtplib.SMTP(self.email_host, self.email_port) as server:
                server.starttls()  # Secure the connection
                server.login(self.email_user, self.email_password)
                server.send_message(msg)
                log.info("Successfully sent email notification.")
        except Exception as e:
            log.error(f"Failed to send email notification: {e}")
