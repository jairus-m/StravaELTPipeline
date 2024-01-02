"""
Utils Module:

Author: Jairus Martinez
Date: 12/21/2023

This module contains any utility functions needed for the ETL code.
"""
import logging
from google.cloud import bigquery
import pyarrow

class StravaAPIConnector():
    """Class for interacting with Strava API"""
    def __init__(self, strava_auth_url: str, strava_activities_url: str, strava_payload: dict):
        """
        Constructor for StravaAPIConnector class

        :param strava_auth_url: strava authorization url
        :param strava_activities_url: strava athlete activities url
        :param strava_payload: dict containing client_id, client_secret, refresh_token, grant_type, f
        """
        self.strava_auth_url = strava_auth_url
        self.strava_activities_url = strava_activities_url
        self.strava_payload = strava_payload
        self._logger = logging.getLogger(__name__)

class BigQueryConnector():
    """Class for interacting with BigQuery data wharehouse"""
    def __init__(self, service_account_json: dict, location: str = 'US', timeout: int = 30):
        """
        Constructor for BigQueryConnector class

        :param service_account_json: Google service account credentials/meta
        :param location: location of cloud dataset [default = 'US']
        :param timeout: timeout param for dataset_ref   
        """
        self.service_account_json = service_account_json
        self.location = location
        self.timeout = timeout
        # initialize GCS client
        self.client = bigquery.Client.from_service_account_info(service_account_json)

    def create_dataset(self, dataset_id: str, dataset_desciption: str):
        """
        Method to create a new dataset in BigQuery

        :param dataset_id: 'project.dataset' referring to dataset within project   
        :param dataset_description: description of dataset
        """
        # create dataset
        dataset = bigquery.Dataset(dataset_id)

        dataset.location = self.location
        dataset.description = dataset_desciption
        # upload dataset
        self.client.create_dataset(dataset, timeout=self.timeout)
        return True

    def upload_table(self, table_id: str, df):
        """
        Method to upload a table to dataset in project

        :param table_id: 'project.dataset.table' referring to table within dataset within project
        :param df: pd.DataFrame containing dataframe to upload into table
        """
        # create table for upload
        job = self.client.load_table_from_dataframe(df, table_id)
        # upload table to dataset
        job.result()

        return True

class UnitConversion():
    """Class to convert units in Strava Data"""
    def __init__(self, x : float):
        """
        Class constructor for UnitConversion.

        :param x: float number to convert
        """
        self.x = x

    def sec_to_min(self):
        """
        Converts seconds to minutes.

        :param x: seconds
        :return minutes: minutes
        :rtype minutes: float
        """
        minutes =  self.x / 60
        return round(minutes,2)

    def meters_to_miles(self):
        """
        Converts meters to miles.

        :param x: meters
        :return miles: miles
        :rtype: float
        """
        miles = self.x / 1609.344
        return round(miles, 2)

    def meters_to_feet(self):
        """
        Converts meters to feet.

        :param x: meters
        :return feet: feet
        :rtype: float
        """
        feet = self.x * 3.28084
        return round(feet, 2)

    def mps_to_mph(self):
        """
        Converts meters per second (mpd) to 
        miles per hour (mph).

        :param x: meters per second
        :return mph: miles per hour
        :rtype: float 
        """
        mph = self.x * 2.23694
        return round(mph, 2)    
