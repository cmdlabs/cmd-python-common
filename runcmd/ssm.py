"""
runCMD library to support AWS SSM
"""
from runcmd.boto_helper import DEFAULT_REGION, get_client
from runcmd.logging_helper import get_logger

LOG = get_logger(__name__)

def read_encrypted_value(property_key, region=DEFAULT_REGION):
    """
    Reads an encrypted value from ssm.
    """
    LOG.info("Reading encrypted value from SSM parameter %s." % property_key)
    result = get_client("ssm", region).get_parameter(Name=property_key, WithDecryption=True)
    return result["Parameter"]["Value"]
