"""
Extract Module:

Author: Jairus Martinez
Date: 12/21/2023
This module contains the extract code in the ETL pipeline.
"""
import pandas as pd
import numpy as np
import requests
import utils
import config

# create logger instance
logging = utils.create_logger_instance(
    abs_path='/Users/jairusmartinez/Desktop/strava_etl/strava_etl_log.log'
    )

class Extract():
    """
    Extract class containing extract methods that correspond to
    the data source.
    Methods:
        strava_extract : extracts strava data from strava API
    """
    def __init__(self):
        """
        Class constructor for Extract.
        """
    def strava_extract(self):
        """
        Accesses Strava's API and saves the raw data as a dataframe.
        Args:
            None
        Returns:
            pandas dataframe
        """

        logging.debug("Requesting Token...\n")
        res = requests.post(config.STAVA_AUTH_URL,data=config.STRAVA_PAYLOAD,
                            verify=False, timeout=(10,10))
        access_token = res.json()['access_token']
        logging.debug("Access Token = %s", access_token)

        header = {'Authorization': 'Bearer ' + access_token}
        # set number of pages to read through
        page_list = list(np.arange(1,11))
        # save data into list
        all_activities = []

        logging.debug('Importing data...')

        # read in 200 activities per page
        for request_page_number in page_list:
            param = {'per_page': 200, 'page':request_page_number}
            my_dataset = (
                requests.get(config.STRAVA_ACTIVITIES_URL, headers=header,
                             params=param, timeout=(10,10)).json()
                ) 
            if len(all_activities) == 0:
                all_activities = my_dataset
                logging.debug('Copying Page: %s', request_page_number)
            else:
                all_activities.extend(my_dataset)
                logging.debug('Copying Page: %s', request_page_number)
            
        logging.debug('Data imported succesfully!')
        return pd.json_normalize(all_activities)
