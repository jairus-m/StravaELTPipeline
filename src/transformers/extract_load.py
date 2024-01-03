"""
Strava EL

Author: Jairus Martinez
Date: 12/21/2023
This module contains the extract and load pipeline code
"""
import logging
import pandas as pd
import numpy as np
import requests
from nodes.utils import StravaAPIConnector, BigQueryConnector

class StravaEL():
    """Reads in Strava data and writes to BigQuery (extract and load)"""

    def __init__(self, strava_auth_url: StravaAPIConnector, strava_activities_url: StravaAPIConnector,
                 strava_payload: StravaAPIConnector, max_page_num: int, actv_per_page: int):
        """
        Constructor for StravaEL class.

        :param strava_auth_url: strava authorization url
        :param strava_activities_url: strava athlete activities url
        :param strava_payload: dict containing client_id, client_secret, refresh_token, grant_type, f
        :param max_page_num: max pages to read through (pages contain activity data)
        :param actv_per_page: number of activities read per page
        """
        self.strava_auth_url = strava_auth_url
        self.strava_activities_url = strava_activities_url
        self.strava_payload = strava_payload
        self.max_page_num = max_page_num
        self.actv_per_page = actv_per_page
        self._logger = logging.getLogger(__name__)

    def extract(self):
        """
        Reads in the raw, source data.

        :returns: dataframe containing activity data
        :rtype: pd.DataFrame
        """
        try:
            self._logger.info("Requesting Token...\n")
            res = requests.post(self.strava_auth_url,data=self.strava_payload,
                                verify=False, timeout=(10,10))
            access_token = res.json()['access_token']

            header = {'Authorization': 'Bearer ' + access_token}
            # set number of pages to read through
            page_list = list(np.arange(1, self.max_page_num))
            # save data into list
            all_activities = []

            self._logger.info('Importing data...')

            # read in n activities per page
            for request_page_number in page_list:
                param = {'per_page': self.actv_per_page, 'page':request_page_number}
                my_dataset = (
                    requests.get(self.strava_activities_url, headers=header,
                                params=param, timeout=(10,10)).json()
                    )
                if len(all_activities) == 0:
                    all_activities = my_dataset
                    self._logger.info('Copying Page: %s', request_page_number)
                else:
                    all_activities.extend(my_dataset)
                    self._logger.info('Copying Page: %s', request_page_number)
            
            self._logger.info('Data imported succesfully!')
            return pd.json_normalize(all_activities)
        except Exception as e:
            self._logger(f'Error in extract method:{e}')
    
    def load(self, bqc: BigQueryConnector, project_name: str, dataset_name: str, table_name: str, num_activities: int, sql_query: str):
        """
        Uploads raw data to BigQuery

        :param bqc: BiqQueryConnector class object
        :param project_name: name of GCS project
        :param dataset_name: name of dataset
        :param table_name: name of table
        :param num_activities: number of most recent activities to load
        :param sql_query: sql_query to get the latest data to compare for freshness
        """
        try:
            df = self.extract()
            df.columns = df.columns.str.replace('.', '_')

            # project.dataset.table format
            table_id = ".".join([project_name, dataset_name, table_name])

            if bqc.table_exists(dataset_name, table_name) is True:
                df_new = bqc.newest_data(df, sql_query)
                if len(df_new) > 0:
                    self._logger.info('Appending new data... %s new activities.', len(df_new))
                    bqc.append_to_table(table_id, df_new)
                else:
                    self._logger.info('Data up to date!')
            else:
                self._logger.info('Table not found. Batch loading last 200 activities.')
                bqc.upload_table(table_id, df[:num_activities])
            return True
        except Exception as e:
            self._logger.error('Error in load method: %s', e)
    