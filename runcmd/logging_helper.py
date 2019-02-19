"""
Common Logging Interface for both Python and Pyspark
"""

import logging

try:
    from pyspark import SparkContext
    LOG4J_LOGGER = SparkContext()._jvm.org.apache.log4j # pylint: disable=protected-access
except ImportError:
    LOG4J_LOGGER = None

def get_logger(name):
    """
    Gets either a python or pyspark logger.
    """
    logger_name = "runcmd_%s" % name
    if LOG4J_LOGGER:
        return LOG4J_LOGGER.LogManager.getLogger(logger_name)
    return logging.getLogger(logger_name)
