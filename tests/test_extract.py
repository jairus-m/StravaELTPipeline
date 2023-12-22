import unittest
import pandas as pd
import sys
from src.nodes.extract import Extract

df = Extract.strava_extract()

class TestExtract(unittest.TestCase):
    def testIsDataFrame(self):
        """
        Tests to see if the output of Extract.strava_extract()
        is a dataframe.
        """
        
        self.assertEqual(type(df), type(pd.DataFrame))

if __name__ == '__main__':
    unittest.main()