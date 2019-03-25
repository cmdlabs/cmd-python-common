"""
Utility functions for EMR.
"""
import logging
import json
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
import time
from functools import wraps
from argparse import ArgumentParser
from runcmd.logging_helper import get_logger

LOG = get_logger(__name__)

class RunCmdArgumentParser(ArgumentParser):
    """
    Customer Argument Parser to better output errors in spark.
    """

    def error(self, message):
        help_message = StringIO.StringIO()
        self.print_help(help_message)
        message = "%s: \n%s]\nError: %s\n" % (self.prog, help_message.getvalue(), message)
        LOG.error(message)
        self.exit(2, message)

def generate_args(inputs):
    """
    Generates arguements for the input to the call.
    """
    parser = ArgumentParser()
    for key, value in inputs.items():
        if isinstance(value, list):
            parser.add_argument(key, help=value[0], required=value[1])
        else:
            parser.add_argument(key, help=value, required=True)
    namespace, extra = parser.parse_known_args()
    LOG.info("Received known args %s and extra args %s" % (namespace, extra))
    return namespace

def convert_to_boolean(input_value):
    """
    Converts various input values into a boolean.
    """
    return input_value in ["True", "true", "yes", "Yes"]

def get_item_from_dict(obj, key):
    """
    Gets an item from a dictionary.
    """
    try:
        return obj[key]
    except KeyError:
        return None

def generate_processed_timestamp():
    """
    Generates a timestamp to mark components as processed.
    """
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())


def retry(exception_to_check, tries=4, delay=3, backoff=2, logger=None):
    """
    :param exception_to_check: the exception to check. may be a tuple of exceptions to check
    :type exception_to_check: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """

    def deco_retry(func): #pylint: disable=missing-docstring
        @wraps(func)
        def func_retry(*args, **kwargs): #pylint: disable=missing-docstring
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return func(*args, **kwargs)
                except exception_to_check as err_obj:
                    msg = "%s, Retrying in %d seconds..." % (str(err_obj), mdelay)
                    if logger:
                        logger.warning(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
                except Exception as err_obj: #pylint: disable=broad-except
                    logger.exception(err_obj)
                    break
            return func(*args, **kwargs)

        return func_retry # true decorator

    return deco_retry
