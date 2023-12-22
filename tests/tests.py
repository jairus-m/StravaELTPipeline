"""
ETL Pipeline Tests

Author: Jairus Martinez
Date: 12/21/2023
"""
import sys
import unittest
import pandas as pd
sys.path.append('../')
from src.nodes import extract, transform, utils

logging = utils.create_logger_instance(
    abs_path='../logs/strava_test_log.log',
    mode='w'
    )

# Create an instance of Extract
ExtractClass = extract.Extract()

df = ExtractClass.strava_extract()

# Persist the raw data to test_data/raw
df.to_csv('../test_data/raw/strava_raw.csv', index=False)
logging.debug('Raw data persisted to ../tests/test_data/strava_raw.csv')

class TestExtract(unittest.TestCase):
    """
    Test suite for Extract class:
    Tests:
        test_is_dataframe : test for type(pd.DataFrame)
    """
    def test_is_dataframe(self):
        """
        Tests to see if the output of Extract.strava_extract()
        is a dataframe.
        """
        self.assertIsInstance(df, pd.DataFrame)

Create an instance of Transform
Transform = transform.Transform()

df_transform = Transform.process_data(df=pd.read_csv('../test_data/raw/strava_raw.csv'))

class TestTransform(unittest.TestCase):
    """
    Test suite for Test class:
    Tests:
        test_cols : test for col names
    """
    def test_cols(self):
        """
        Tests to see if the output of Extract.strava_extract()
        is a dataframe.
        """
        expected_cols = ['name', 'distance', 'moving_time', 'elapsed_time',
       'total_elevation_gain', 'sport_type', 'id', 'achievement_count',
       'kudos_count', 'comment_count', 'athlete_count', 'private',
       'visibility', 'average_speed', 'max_speed', 'average_cadence',
       'average_temp', 'has_heartrate', 'average_heartrate', 'max_heartrate',
       'elev_high', 'elev_low', 'pr_count', 'has_kudoed', 'average_watts',
       'kilojoules', 'max_watts', 'weighted_average_watts', 'date', 'time',
       'time_bins']
        self.assertCountEqual(df_transform.columns.tolist(), expected_cols)

if __name__ == '__main__':
    unittest.main()
