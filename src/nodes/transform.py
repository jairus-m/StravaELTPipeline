"""
Transform Module:

Author: Jairus Martinez
Date: 12/22/2023
This module contains the transform code in the ETL pipeline.
"""
import pandas as pd
from src.nodes.utils import (meters_to_feet, meters_to_miles,
                              mps_to_mph, sec_to_min, create_logger_instance)

# create logger instance
logging = create_logger_instance(
    abs_path='../logs/strava_transform_log.log',
    mode='w'
    )

class Transform():
    """
    Class object containing methods for transformation functions.
    Methods:
        strava_transform : tranforms raw data  
    """
    def __init__(self):
        """
        Class constructor for Transform
        """
    def process_data(self, df):
        """
        Clean and processe raw activity data to a useable dataset.
        Args:
            df (pd.DataFrame) : raw activitiy dataframe
        Returns:
            df (pd.DataFra,e) : clean activitiy dataframe
        """
        # cols to drop
        cols_to_drop = ['start_date','resource_state', 'type', 'start_date', 
                    'timezone', 'utc_offset', 'location_city', 'location_state', 
                    'location_country', 'photo_count', 'trainer', 'commute',
                    'manual', 'flagged', 'gear_id', 'start_latlng', 'end_latlng', 
                    'heartrate_opt_out', 'display_hide_heartrate_option', 
                    'upload_id', 'upload_id_str', 'external_id', 
                    'from_accepted_tag', 'total_photo_count', 'athlete.id',
                    'athlete.resource_state', 'map.id', 'map.summary_polyline',
                    'map.resource_state', 'device_watts', 'workout_type']
        logging.info('Dropped cols...')
        
        df = df.drop(columns=cols_to_drop)

        # convert distance units 
        df['distance'] = meters_to_miles(df['distance'])
        df['moving_time'] = sec_to_min(df['moving_time'])
        df['elapsed_time'] = sec_to_min(df['elapsed_time'])
        df['total_elevation_gain'] = meters_to_feet(df['total_elevation_gain'])
        logging.info('Converted distance units.')

        # convert speed units
        df['average_speed'] = mps_to_mph(df['average_speed'])
        df['max_speed'] = mps_to_mph(df['max_speed'])
        logging.info('Converted speed units.')

        df['elev_high'] = meters_to_feet(df['elev_high'])
        df['elev_low'] = meters_to_feet(df['elev_low'])
        logging.info('Converted elevation units.')

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
        logging.info('Created time bins.')

        return df
    