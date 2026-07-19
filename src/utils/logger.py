import logging
import sys


def get_logger(name):
    """
    Creates and configures a logger instance to output logs to standard output.
    Prevents adding duplicate handlers if the logger already exists.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger