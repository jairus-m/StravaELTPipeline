"""
Slack Notifications Tests

Author: Jairus Martinez
Date: 1/06/2024
"""
import os
import unittest
from unittest.mock import MagicMock, patch
import yaml
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.sys.path.insert(0,parentdir) 
from slack_sdk.errors import SlackApiError
from src.commons.slack_notifications import SlackNotifications
from datetime import datetime

CONFIG_PATH = '/Users/jairusmartinez/Desktop/strava_etl/configs/configs.yml'
config = yaml.safe_load(open(CONFIG_PATH , encoding='utf-8'))

class TestSlackNotifications(unittest.TestCase):
    """Test suite for SlackNotifications"""
    def setUp(self):
        """
        Attributes:
            mock_client: MagicMock() class to mock Slack SDK
            token: token for Slack App
            channel: channel that recieves notifications
            slack_notifications: SlackNotifications() class instance
            slack_notifications.client: mock_client generated for SlackNotifications() class instance
        """
        self.mock_client = MagicMock()
        self.token = config['slack']['token']
        self.channel = config['slack']['channel']
        self.slack_notifications = SlackNotifications(self.token, self.channel)
        self.slack_notifications.client = self.mock_client

    def test_send_custom_message(self):
        """
        Test for proper call and return of send_custom_message() method.
        """
        # send message
        text = "Test custom message"
        result = self.slack_notifications.send_custom_message(text)

        # test that chat_postMessage called
        self.mock_client.chat_postMessage.assert_called_once_with(channel=f"#{self.channel}", text=text)

        # expect send_custom_message() to return True if called succesfully
        self.assertEqual(result, True)

    def test_send_custom_message_wrong_token(self):
        """
        Test SlackAPIError with wrong token
        """
        # init with wrong token
        slack_notifications = SlackNotifications('wrong_token', self.channel)

        # attempt to send message and assertRaise error
        text = "Test custom message"
        self.assertRaises(SlackApiError, slack_notifications.send_custom_message, text)

    def test_send_custom_message_wrong_channel(self):
        """
        Test SlackAPIError with wrong token
        """
        # init with wrong token
        slack_notifications = SlackNotifications(self.token, 'incorrect_channel')

        # attempt to send message and assertRaise error
        text = "Test custom message"
        self.assertRaises(SlackApiError, slack_notifications.send_custom_message, text)

    @patch("src.commons.slack_notifications.datetime")
    def test_timing_message(self, mock_datetime):
        # Mocking datetime.now() directly
        mock_datetime.datetime.now.return_value = datetime(2024, 1, 6, 12, 34, 56)

        job = "Test Job"
        duration = 10.123
        expected_text = "Job: Test Job\nDate: 2024-01-06\nTime: 12:34:56\nDuration: 10.12s"

        self.slack_notifications.timing_message(job, duration)
        self.mock_client.chat_postMessage.assert_called_once_with(channel=f"#{self.channel}", text=expected_text)

if __name__ == "__main__":
    unittest.main()
