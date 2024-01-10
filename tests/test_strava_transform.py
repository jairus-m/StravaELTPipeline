"""
ETL Pipeline Tests

Author: Jairus Martinez
Date: 12/21/2023
"""
import os
import unittest
from unittest.mock import MagicMock
import yaml
import pandas as pd
import numpy as np
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.sys.path.insert(0,parentdir)
from src import commons
from src.commons.connectors import StravaAPIConnector
from src.transformers.strava_etl import StravaETL

CONFIG_PATH = '/Users/jairusmartinez/Desktop/strava_etl/configs/configs.yml'
config = yaml.safe_load(open(CONFIG_PATH , encoding='utf-8'))

df_raw = pd.read_csv('/Users/jairusmartinez/Desktop/strava_etl/test_data/unit/unit_raw.csv')
df_clean = pd.read_csv('/Users/jairusmartinez/Desktop/strava_etl/test_data/unit/unit_clean.csv')

class TestExtract(unittest.TestCase):
    """
    Test suite for Extract class:
    Tests:
        test_is_dataframe : test for type(pd.DataFrame)
    """
    def setUp(self):
        # Set up any necessary objects for testing
        self.strava_api_connector = MagicMock()

        # Create a mock StravaETL instance
        strava_etl = StravaETL(self.strava_api_connector, 5, 10, config['strava_api']['cols_to_drop'])

        # Mock the extract method
        strava_etl.extract = MagicMock(return_value=df_raw)

        # Act
        self.df_transformed = strava_etl.transform(df_raw)

    def tearDown(self):
        # Clean up after the test
        pass

    def test_transform_type(self):
        """
        Tests to see if the output of StravaETL.transform()
        is a dataframe.
        """
        self.assertEqual(type(self.df_transformed), type(df_clean))

    def test_transform_columns(self):
        """
        Tests to see if columns match
        """
        expected_cols = config['strava_api']['cols_to_keep']
        self.assertCountEqual(self.df_transformed.columns.tolist(), expected_cols)
    
    def test_shape(self):
        """
        Tests for equal dimensions of data
        """
        self.assertEqual(self.df_transformed.shape, df_clean.shape)

    def test_describe(self):
        """
        Test for equal values of all columns in df
        """
        for col in df_clean.columns:
            if col != 'date':
                np.testing.assert_array_equal(df_clean[col].values, self.df_transformed[col].values)

if __name__ == '__main__':
    unittest.main()
