"""
Functions for cloudwatch logs.
"""
import time
from runcmd.boto_helper import DEFAULT_REGION, get_client
from runcmd.logging_helper import get_logger

LOGGER = get_logger(__name__)

def get_log_stream(log_group_name, log_stream_name, region=DEFAULT_REGION):
    """
    Gets the log stream message.
    """
    response = get_client("logs", region=region).describe_log_streams(
        logGroupName=log_group_name,
        logStreamNamePrefix=log_stream_name,
    )
    try:
        LOGGER.info(response)
        log_streams = response["logStreams"]
        for stream in log_streams:
            if log_stream_name == stream["logStreamName"]:
                return stream
    except KeyError:
        # Key doesn't exist if the stream doesn't exist.
        pass
    return None

def send_log(log_group_name, log_stream_name, message, region=DEFAULT_REGION):
    """
    Sends a cloudwatch log event.
    """
    try:
        token = get_log_stream(log_group_name, log_stream_name, region)["uploadSequenceToken"]
    except KeyError:
        LOGGER.info("Token is missing, is either a new stream or a missing stream!")
        token = None
    payload = {
        "logGroupName": log_group_name,
        "logStreamName": log_stream_name,
        "sequenceToken": token,
        "logEvents": [{
            "timestamp": int(round(time.time() * 1000)),
            "message": message
        }]
    }
    get_client("logs", region=region).put_log_events(**payload)

def create_log_stream(log_group_name, log_stream_name, region=DEFAULT_REGION):
    """
    Creates a log stream if one does not exist.
    """
    stream = get_log_stream(log_group_name, log_stream_name, region)
    if not stream:
        LOGGER.info("Creating log stream %s in group %s" % (log_stream_name, log_group_name))
        get_client("logs", region=region).create_log_stream(
            logGroupName=log_group_name,
            logStreamName=log_stream_name
        )
    send_log(log_group_name, log_stream_name, "Starting Execution", region)
