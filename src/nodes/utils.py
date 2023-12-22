"""
Utils Module:

Author: Jairus Martinez
Date: 12/21/2023

This module contains any utility functions needed for ETL code.
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