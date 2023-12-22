import pandas as pd
import numpy as np
import requests
from pandas.io.json import json_normalize
import config
import utils

# create logger instance
logging = utils.createLoggerInstance(absPath='/Users/jairusmartinez/Desktop/strava_etl/strava_etl_log.log')

class Extract():
    def __init__(self):
        self.strava_extract = self.strava_extract()
    def strava_extract():
        """
        Accesses Strava's API and saves the raw data as a dataframe.
        Args:
            None
        Returns:
            pandas dataframe
        """

        logging.debug("Requesting Token...\n")
        res = requests.post(config.strava_auth_url, data=config.strava_payload, verify=False)
        access_token = res.json()['access_token']
        logging.debug("Access Token = {}\n".format(access_token))

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
                requests.get(config.strava_activites_url, headers=header, params=param).json()
                )
        
            if len(all_activities) == 0:
                all_activities = my_dataset
                logging.debug(f'Copying Page: {request_page_number}')
            else:
                all_activities.extend(my_dataset)
                logging.debug(f'Copying Page: {request_page_number}')
            
        logging.debug('Data imported succesfully!')
        return pd.json_normalize(all_activities)