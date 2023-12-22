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
    relPathToLogger = getRelativePath(absPath)
    handler = logging.FileHandler(relPathToLogger, 'w')
    handler.setLevel(logging.DEBUG)

    # Create a formatter and set the formatter for the handler
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(lineno)d - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    return logger


def getRelativePath(absPath : str):
    """
    Returns the relative path given the absolute path.
    Args:
        absPath (str) : absolute path
    Returns
        relPath (str) : relative path
    """
    current_dir = os.getcwd()
    relPath = os.path.relpath(absPath, current_dir)
    return relPath