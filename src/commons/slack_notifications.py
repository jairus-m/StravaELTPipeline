"""
Utils Module:

Author: Jairus Martinez
Date: 12/21/2023

This module contains functions needed for Slack notifications
"""
from slack_sdk import WebClient
#from slack_sdk.errors import SlackApiError
import datetime

class SlackNotifications():
    """Class for sending Slack Notifications"""
    def __init__(self, token: str, channel: str):
        """
        Class constructor.

        :param token: Oauth Token for Slack app
        :param channel: channel name that is connected to Slack app (#channel-name)
        """
        self.token = token
        self.channel = "#"+ str(channel)
        # initialize client
        self.client = WebClient(token=self.token)
    def send_custom_message(self, text: str):
        """
        Sends a custom message to Slack Channel.

        :param text: text to send 
        """
        self.client.chat_postMessage(channel=self.channel, text=text)
        return True
    
    def timing_message(self, job: str, duration):
        """
        Sends a standard message that includes time and job status.

        :param job: name of job to be executed
        :param duration: duration of job in seconds (using time.time())
        :param duration:  
        """
        current_datetime = datetime.datetime.now()
        date = current_datetime.strftime("%Y-%m-%d")
        time = current_datetime.strftime("%H:%M:%S")

        text = f"Job: {job}\nDate: {date}\nTime: {time}\nDuration: {duration:.2f}s"
        
        # send message 
        self.client.chat_postMessage(channel=self.channel, text=text)
        return True

        

