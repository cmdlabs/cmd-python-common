"""
Helper method for boto3 operations.
"""
import boto3
from runcmd import get_item_from_dict
from runcmd.logging_helper import get_logger

LOG = get_logger(__name__)

DEFAULT_REGION = "ap-southeast-2"

BOTO_ELEMENTS = {}

def generate_key(resource_type, service, region):
    """
    Generates the key for the resources to store in the cache dict.
    """
    return "%s-%s-%s" % (resource_type, service, region)

def get_from_cache(resource_type, service, region):
    """
    Removes an element from the cache.
    """
    return get_item_from_dict(BOTO_ELEMENTS, generate_key(resource_type, service, region))

def add_to_cache(resource_type, service, region, element):
    """
    Adds an element to the cache.
    """
    BOTO_ELEMENTS[generate_key(resource_type, service, region)] = element

def get_client(service, region=DEFAULT_REGION):
    """
    Gets a boto3 client.
    """
    obj = get_from_cache("client", service, region)
    if not obj:
        LOG.info("Creating new client for service %s for region %s." % (service, region))
        obj = boto3.client(service, region_name=region)
        add_to_cache("client", service, region, obj)
    return obj

def get_resource(service, region=DEFAULT_REGION):
    """
    Gets a boto3 resource.
    """
    obj = get_from_cache("resource", service, region)
    if not obj:
        LOG.info("Creating new resource for service %s for region %s." % (service, region))
        obj = boto3.resource(service, region_name=region)
        add_to_cache("resource", service, region, obj)
    return obj
