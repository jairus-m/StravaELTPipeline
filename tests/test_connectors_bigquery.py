"""
Connectors Tests : BigQuery

Author: Jairus Martinez
Date: 1/07/2024
"""
import os
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pyarrow
import yaml
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.sys.path.insert(0,parentdir) 
from src.commons.connectors import BigQueryConnector

CONFIG_PATH = '/Users/jairusmartinez/Desktop/strava_etl/configs/configs.yml'
config = yaml.safe_load(open(CONFIG_PATH , encoding='utf-8'))

class TestBigQueryConnector(unittest.TestCase):
    """Test suite for BigQueryConnector"""
    def setUp(self):
        self.service_account_json = config['bigquery']['SERVICE_ACCOUNT_JSON']
        self.project = config['bigquery']['project']
        self.dataset = 'test_dataset'
        self.table = 'test_table'
        self.dataset_id = f'{self.project}.{self.dataset}'
        self.table_id = f'{self.project}.{self.dataset}.{self.table}'
        self.bqc = BigQueryConnector(
            service_account_json=self.service_account_json
        )

        # create test dataset
        self.bqc.create_dataset(self.dataset_id, 'test dataset')

    def tearDown(self):
        # Clean up the test dataset
        try:
            self.bqc.client.delete_dataset(self.dataset_id, delete_contents=True, not_found_ok=True)
            print(f"Dataset {self.dataset_id} deleted successfully.")
        except NotFound:
            print(f"Dataset {self.dataset_id} not found. Deletion skipped.")
        
    def test_upload_table(self):
        """Test uploaded table to test dataset"""
        df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})

        result = self.bqc.upload_table(self.table_id, df)

        self.assertTrue(result)

    def test_newest_data(self):
        """Test for freshness of data based on date """
        # Assuming you have actual dataframes and date columns to compare
        df = pd.DataFrame({'id': [1, 2, 3], 'date_col': ['2022-01-01', '2022-01-02', '2022-01-03']})
        df_to_compare = pd.DataFrame({'id': [2], 'date_col': ['2022-01-02']})
        date_col_name = 'date_col'

        expected_df = pd.DataFrame({'id': [3], 'date_col': ['2022-01-03']})
        filtered_df = self.bqc.newest_data(df, df_to_compare, date_col_name).reset_index(drop=True)

        pd.testing.assert_frame_equal(expected_df.sort_index(axis=1), filtered_df.sort_index(axis=1), check_dtype=False)

    def test_append_to_table(self):
        """Append to existing table test"""
        df = pd.DataFrame({'col1': [3, 4], 'col2': ['c', 'd']})
        result = self.bqc.append_to_table(self.table_id, df)
        self.assertTrue(result)

    def test_table_exists(self):
       """
       Test to see if table exists.
       """
       # upload table
       df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
       self.bqc.upload_table(self.table_id, df)
       
       result = self.bqc.table_exists(self.dataset, self.table)
       self.assertTrue(result)

    def test_query_table(self):
        """
        Test a query from a table
        """
        # upload table
        expected_df = pd.DataFrame({'col1': [1, 2, 3, 4], 'col2': ['a', 'b', 'c', 'd']})
        self.bqc.upload_table(self.table_id, expected_df)

        sql_query = f"SELECT * FROM `{self.table_id}`"

        result_df = self.bqc.query_table(sql_query)

        pd.testing.assert_frame_equal(expected_df.sort_index(axis=1), result_df.sort_index(axis=1), check_dtype=False)

if __name__ == '__main__':
    unittest.main()