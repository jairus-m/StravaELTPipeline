"""
Utils Module:

Author: Jairus Martinez
Date: 12/21/2023

This module contains any utility functions needed for the ETL code.
"""
import logging

def create_logger_instance(abs_path : str, mode: str):
    """
    Creates logging instance (default='DEBUG').
    Args:
        absPath (str): absolute path to log file
        mode (str) : mode param for logging.FileHandler
    Returns:
        logger: logging instance
    """
    # Create a logger instance
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create a file handler
    handler = logging.FileHandler(abs_path, mode=mode)
    handler.setLevel(logging.DEBUG)

    # Create a formatter and set the formatter for the handler
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(lineno)d - %(message)s', \
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    return logger

def sec_to_min(x : float):
    """
    Converts seconds to minutes.
    Args:
        x (float) : seconds
    Returns:
        minutes (float) : minutes
    """
    minutes =  x / 60
    return round(minutes,2)

def meters_to_miles(x : float):
    """
    Converts meters to miles.
    Args:
        x (float) : meters
    Returns:
        miles (float) : miles
    """
    miles = x / 1609.344 
    return round(miles, 2)

def meters_to_feet(x):
    """
    Converts meters to feet.
    Args:
        x (float) : meters
    Returns:
        feet (float) : feet
    """
    feet = x * 3.28084
    return round(feet, 2)

def mps_to_mph(x):
    """
    Converts meters per second (mpd) to 
    miles per hour (mph).
    Args
        x (float) : meters per second
    Returns
        mph (float) : miles per hour
    """
    mph = x * 2.23694
    return round(mph, 2)    

def drop_cols(df, cols_to_drop : list):
    """
    Takes a list of column names and drops them from 
    pd.DataFrame.
    Args:
        df (pd.DataFrame) : input dataframe
        cols_to_drop (list) : list of column string names 
    Returns: 
        df (pd.DataFrame) : dataframe with dropped cols
    """
    return df.drop(columns=cols_to_drop)
