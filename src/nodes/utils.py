import os
import logging


def createLoggerInstance(absPath : str):
    """
    Creates logging instance (default='DEBUG').
    Args:
        absPath (str): absolute path to log file
    Returns:
        logger: logging instance
    """
    # Create a logger instance
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create a file handler
    handler = logging.FileHandler('./strava_etl_log.log', 'w')
    handler.setLevel(logging.DEBUG)

    # Create a formatter and set the formatter for the handler
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(lineno)d - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    return logger