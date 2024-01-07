"""
Utils Tests

Author: Jairus Martinez
Date: 1/06/2024
"""
import os
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.sys.path.insert(0,parentdir) 
import unittest
from src.commons.utils import UnitConversion

class TestUnitConversion(unittest.TestCase):
    """
    Test suite for UnitConversion class.

    Tests:
        test_sec_to_min
        test_meters_to_miles
        test_meters_to_feet
        test_mps_to_mph
    """
    def setUp(self):
        self.converter = UnitConversion()

    def test_sec_to_min(self):
        self.assertAlmostEqual(self.converter.sec_to_min(60), 1.0)
        self.assertAlmostEqual(self.converter.sec_to_min(120), 2.0)
        self.assertAlmostEqual(self.converter.sec_to_min(180.5), 3.01)

    def test_meters_to_miles(self):
        self.assertAlmostEqual(self.converter.meters_to_miles(1609.344), 1.0)
        self.assertAlmostEqual(self.converter.meters_to_miles(3218.688), 2.0)
        self.assertAlmostEqual(self.converter.meters_to_miles(5000), 3.11)

    def test_meters_to_feet(self):
        self.assertAlmostEqual(self.converter.meters_to_feet(1), 3.28)
        self.assertAlmostEqual(self.converter.meters_to_feet(5), 16.4)
        self.assertAlmostEqual(self.converter.meters_to_feet(10), 32.81)

    def test_mps_to_mph(self):
        self.assertAlmostEqual(self.converter.mps_to_mph(1), 2.24)
        self.assertAlmostEqual(self.converter.mps_to_mph(5), 11.18)
        self.assertAlmostEqual(self.converter.mps_to_mph(10), 22.37)

if __name__ == '__main__':
    unittest.main()
