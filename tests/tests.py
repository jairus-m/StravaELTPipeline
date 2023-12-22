"""
ETL Pipeline Tests

Author: Jairus Martinez
Date: 12/21/2023
"""
import sys
import unittest
import pandas as pd
sys.path.append('../')
from src.nodes import extract, utils

logging = utils.create_logger_instance(
    abs_path='../logs/strava_etl_log.log',
    mode='a'
    )

# Create an instance of Extract
ExtractClass = extract.Extract()

# Call the instance method on the instance
df = ExtractClass.strava_extract()

# Persist the raw data to test_data/raw
df.to_csv('../test_data/raw/strava_raw.csv')
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

if __name__ == '__main__':
    unittest.main()
