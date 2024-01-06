"""
Utils Module:

Author: Jairus Martinez
Date: 12/21/2023

This module contains any utility functions needed for the ETL code.
"""
class MyCustomError(Exception):
    pass
class UnitConversion():
    """Class to convert units in Strava Data"""
    def sec_to_min(self, x: float):
        """
        Converts seconds to minutes.

        :param x: seconds
        :return minutes: minutes
        :rtype minutes: float
        """
        minutes =  x / 60
        return round(minutes,2)

    def meters_to_miles(self, x: float):
        """
        Converts meters to miles.

        :param x: meters
        :return miles: miles
        :rtype: float
        """
        miles = x / 1609.344
        return round(miles, 2)

    def meters_to_feet(self, x: float):
        """
        Converts meters to feet.

        :param x: meters
        :return feet: feet
        :rtype: float
        """
        feet = x * 3.28084
        return round(feet, 2)

    def mps_to_mph(self, x: float):
        """
        Converts meters per second (mpd) to 
        miles per hour (mph).

        :param x: meters per second
        :return mph: miles per hour
        :rtype: float 
        """
        mph = x * 2.23694
        return round(mph, 2)    
