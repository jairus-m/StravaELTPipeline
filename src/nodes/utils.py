"""
Utils Module:

Author: Jairus Martinez
Date: 12/21/2023

This module contains any utility functions needed for the ETL code.
"""
import pandas as pd
import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pyarrow
from datetime import datetime, timedelta

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
    
    def newest_data(self, df: pd.DataFrame, sql_query: str, date_col_name: str):
        """
        This method filters for the freshest data.

        :param df: dataframe containing the extracted data
        :param sql_query: query to send to table (`StravaActivities.raw`)
        :param date_col_name: name of the date col to asses freshness by 
        :returns: filtered dataframe
        """

        # run the query
        query_job = self.client.query(sql_query)

        # results
        df_bqc = query_job.to_dataframe()

        # grab the latest date (latest date - 1 day)
        latest_date = pd.to_datetime(df_bqc[date_col_name]).sort_index().dt.date[0] - timedelta(days=1)

        # create mask and filter data (greater than latest data AND activity 'id' not found in latest query)
        mask = (pd.to_datetime(df[date_col_name]).sort_index().dt.date > latest_date) & (~df['id'].isin(df_bqc['id']))
        return df[mask]
    
    def append_to_table(self, table_id: str, df):
        """
        Method to append data to an existing table in BigQuery.

        :param table_id: 'project.dataset.table' referring to the existing table within dataset within project
        :param df: pd.DataFrame containing data to append into the table
        """
        # make sure no '.' in col names
        df.columns = df.columns.str.replace('.', '_')
        
        # Set job configuration to append data to the existing table
        job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND')

        # Create a job to append data to the existing table
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)

        # Wait for the job to complete
        job.result()

        return True
    
    def table_exists(self, dataset_name: str, table_name: str):
        """
        Checks to see if a table exists.

        :param dataset_name: name of dataset
        :param table_name: name of table
        """
        dataset = self.client.dataset(dataset_name)
        table_ref = dataset.table(table_name)
        try:
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False


class UnitConversion():
    """Class to convert units in Strava Data"""
    def sec_to_min(self, x: float):
        """
        Converts seconds to minutes.

        :param x: seconds
        :return minutes: minutes
        :rtype minutes: float
        """
        minutes =  x / 60
        return round(minutes,2)

    def meters_to_miles(self, x: float):
        """
        Converts meters to miles.

        :param x: meters
        :return miles: miles
        :rtype: float
        """
        miles = x / 1609.344
        return round(miles, 2)

    def meters_to_feet(self, x: float):
        """
        Converts meters to feet.

        :param x: meters
        :return feet: feet
        :rtype: float
        """
        feet = x * 3.28084
        return round(feet, 2)

    def mps_to_mph(self, x: float):
        """
        Converts meters per second (mpd) to 
        miles per hour (mph).

        :param x: meters per second
        :return mph: miles per hour
        :rtype: float 
        """
        mph = x * 2.23694
        return round(mph, 2)    
