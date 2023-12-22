"""
ETL Pipeline Tests

Author: Jairus Martinez
Date: 12/21/2023
"""
import unittest
import pandas as pd
from extract import Extract

df = Extract().strava_extract()

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
