"""
StravaETL

Author: Jairus Martinez
Date: 12/21/2023
This module contains the extract, transform, and load pipeline code.
"""
import logging
import pandas as pd
import numpy as np
import requests
from nodes.utils import StravaAPIConnector, BigQueryConnector
from nodes.utils import UnitConversion

class StravaETL():
    """Reads in Strava data and writes to BigQuery (extract, transform, and load)"""
    def __init__(self, strava_auth_url: StravaAPIConnector, strava_activities_url: StravaAPIConnector,
                 strava_payload: StravaAPIConnector, max_page_num: int, actv_per_page: int, cols_to_drop: list):
        """
        Constructor for StravaETL class.

        :param strava_auth_url: strava authorization url
        :param strava_activities_url: strava athlete activities url
        :param strava_payload: dict containing client_id, client_secret, refresh_token, grant_type, f
        :param max_page_num: max pages to read through (pages contain activity data)
        :param actv_per_page: number of activities read per page
        :param cols_to_drop: col names to drop from data
        """
        self.strava_auth_url = strava_auth_url
        self.strava_activities_url = strava_activities_url
        self.strava_payload = strava_payload
        self.max_page_num = max_page_num
        self.actv_per_page = actv_per_page
        self.cols_to_drop = cols_to_drop
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

    def transform(self):
        """
        Clean and processes raw activity data to a useable dataset.

        :returns: cleaned strava activitiy dataframe
        :rtype: pd.DataFrame
        """
        try:
            df = self.extract()

            # cols to drop
            self._logger.info('Dropping cols...')
            
            df = df.drop(columns=self.cols_to_drop)

            self._logger.info('Cols dropped...')

            uc = UnitConversion()

            # convert distance units
            df['distance'] = uc.meters_to_miles(x=df['distance'])
            df['moving_time'] = uc.sec_to_min(df['moving_time'])
            df['elapsed_time'] = uc.sec_to_min(df['elapsed_time'])
            df['total_elevation_gain'] = uc.meters_to_feet(df['total_elevation_gain'])
            self._logger.info('Converted distance units.')

            # convert speed units
            df['average_speed'] = uc.mps_to_mph(df['average_speed'])
            df['max_speed'] = uc.mps_to_mph(df['max_speed'])
            self._logger.info('Converted speed units.')

            df['elev_high'] = uc.meters_to_feet(df['elev_high'])
            df['elev_low'] = uc.meters_to_feet(df['elev_low'])
            self._logger.info('Converted elevation units.')

            # create time bins
            df['date'] = pd.to_datetime(df['start_date_local']).dt.tz_localize(None)
            df = df.drop(columns='start_date_local')
            df['time'] = df['date'].dt.hour
            df['time_bins'] = pd.cut(
                df['time'],
                bins=[-1,4,8,12,16,20,24],
                labels=['12am-4am', '4am-8am', '8am-12pm', '12pm-4pm', '4pm-8pm', '8pm-12am'],
                ordered=True
            )
            self._logger.info('Created time bins.')
            return df
        except Exception as e:
            self._logger.info(f'Error in transform method:{e}')
    
    def load(self, bqc: BigQueryConnector, project_name: str, dataset_name: str, table_name: str, num_activities: int, sql_query: str):
        """
        Uploads data to BigQuery

        :param bqc: BiqQueryConnector class object
        :param project_name: name of GCS project
        :param dataset_name: name of dataset
        :param table_name: name of table
        :param num_activities: number of most recent activities to load
        :param sql_query: sql_query to get the latest data to compare for freshness
        """
        try:
            df = self.transform()
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
                self._logger.info('Table not found. Batch loading activities.')
                bqc.upload_table(table_id, df[:num_activities])
            return True
        except Exception as e:
            self._logger.error('Error in load method: %s', e)
    