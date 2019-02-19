"""
runCMD library to support AWS S3
"""
from botocore.exceptions import ClientError
from runcmd.boto_helper import DEFAULT_REGION, get_client, get_resource
from runcmd.logging_helper import get_logger

LOGGER = get_logger(__name__)

def save_to_s3(contents, bucket, prefix, region=DEFAULT_REGION, encryption="AES256"):
    """
    Writes the contents of the file into the data lake.
    """
    LOGGER.info("Writing object to bucket %s with prefix of %s." % (bucket, prefix))
    payload = {"Body": contents, "Bucket": bucket, "Key": prefix}
    if encryption:
        payload["ServerSideEncryption"] = encryption
    get_client("s3", region).put_object(**payload)

def read_from_s3(bucket, prefix, region=DEFAULT_REGION):
    """
    Read a object from S3
    """
    LOGGER.info("Reading object from bucket %s with prefix of %s." % (bucket, prefix))
    obj = get_client("s3", region).get_object(Bucket=bucket, Key=prefix)
    return obj

def copy(source_bucket, source_prefix, target_bucket, target_prefix, # pylint: disable=too-many-arguments
         region=DEFAULT_REGION, encryption="AES256"):
    """
    Copies an item from a bucket to another.
    """
    LOGGER.info("Coyping object from bucket s3://%s/%s to s3://%s/%s" \
             % (source_bucket, source_prefix, target_bucket, target_prefix))
    payload = {
        "target_bucket": target_bucket,
        "CopySource": {
            "Bucket": source_bucket,
            "Key": source_prefix
        },
        "Key": target_prefix
    }
    if encryption:
        payload["ServerSideEncryption"] = encryption
    get_client("s3", region).copy_object(**payload)

def delete(bucket, prefix, region=DEFAULT_REGION):
    """
    Deletes an object from a bucket.
    """
    LOGGER.info("Deleting object from bucket s3://%s/%s." % (bucket, prefix))
    get_client("s3", region).delete_object(Bucket=bucket, Key=prefix)

def move(source_bucket, source_prefix, target_bucket, target_prefix, # pylint: disable=too-many-arguments
         region=DEFAULT_REGION, encryption="AES256"):
    """
    Moves an object in S3
    """
    LOGGER.info("Moving object from bucket s3://%s/%s to s3://%s/%s" \
             % (source_bucket, source_prefix, target_bucket, target_prefix))
    copy(source_bucket, source_prefix, target_bucket, target_prefix, region, encryption)
    delete(source_bucket, source_prefix, region)

def list_items_with_prefix(bucket, prefix, region=DEFAULT_REGION):
    """
    Lists items with a common prefix.
    """
    LOGGER.info("Listing items from bucket %s with prefix %s." % (bucket, prefix))
    bucket = get_resource("s3", region=region).Bucket(bucket)
    return bucket.objects.filter(Prefix=prefix)

def find_input_file(s3_bucket, s3_prefix, extension, region=DEFAULT_REGION):
    """
    Finds a file in a prefix with a given prefix.  This is useful for finding
    single files in spark outputs.
    """
    bucket = get_resource("s3", region=region).Bucket(s3_bucket)
    results = bucket.objects.filter(Prefix=s3_prefix)
    for result in results:
        if result.key.endswith(extension):
            return "s3://%s/%s" % (s3_bucket, result.key)
    raise Exception("Unable to find %s from prefix %s" % (s3_prefix, extension))

def does_key_exist(s3_bucket, key, region=DEFAULT_REGION):
    """
    Checks if a key exists in a bucket.
    """
    try:
        response = get_resource("s3", region=region).Object(s3_bucket, key).load()
        LOGGER.info("Checking if key %s in bucket %s exists: %s" % (s3_bucket, key, response))
        return response
    except ClientError as exp:
        LOGGER.info("Error finding S3 key s3://%s/%s: %s" % (s3_bucket, key, exp))
        if exp.response['Error']['Code'] == "404":
            return False
        else:
            raise exp

def does_folder_exist(s3_bucket, key, region=DEFAULT_REGION):
    """
    Checks if a folder exists.
    """
    bucket = get_resource("s3", region=region).Bucket(s3_bucket)
    response = bucket.objects.filter(Prefix=key, MaxKeys=1)
    LOGGER.info("Checking if key %s in bucket %s exists" % (s3_bucket, key))
    for result in response: # pylint: disable=unused-variable
        return True
    return False

def _build_version_payload(s3_bucket, key, version=None):
    """
    Builds the payload for S3 operations.
    """
    payload = {"Bucket": s3_bucket, "Key": key}
    if version:
        payload["VersionId"] = version
    return payload

def _get_tag_element(tag_set, tag_name):
    """
    Gets an individual tag element from a tag set.
    """
    for tag in tag_set:
        if tag["Key"] == tag_name:
            return tag
    return None

def get_object_tags(s3_bucket, key, region=DEFAULT_REGION, version=None):
    """
    Gets the tags for an object.
    """
    payload = _build_version_payload(s3_bucket, key, version)
    return get_client("s3", region).get_object_tagging(**payload)

def get_object_tag(s3_bucket, key, tag_name, region=DEFAULT_REGION, version=None):
    """
    Gets a single tag value.
    """
    response = get_object_tags(s3_bucket, key, region, version)
    result = _get_tag_element(response["TagSet"], tag_name)
    if result:
        return result["Value"]
    return None

def update_object_tag(s3_bucket, key, tag_name, tag_value, region=DEFAULT_REGION, version=None): # pylint: disable=too-many-arguments
    """
    Updates an object tag
    """
    LOGGER.info("Adding tag %s with value %s to element s3://%s/%s" \
        % (tag_name, tag_value, key, s3_bucket))
    response = get_object_tags(s3_bucket, key, region, version)
    tag_set = response["TagSet"]
    tag = _get_tag_element(tag_set, tag_name)
    if tag:
        tag["Value"] = tag_value
    else:
        tag_set.append({"Key": tag_name, "Value": tag_value})
    payload = _build_version_payload(s3_bucket, key, version)
    payload["Tagging"] = {"TagSet": tag_set}
    get_client("s3", region).put_object_tagging(**payload)

def remove_all_tags(s3_bucket, key, region=DEFAULT_REGION, version=None):
    """
    Removes all tags from an item.
    """
    payload = _build_version_payload(s3_bucket, key, version)
    payload["Tagging"] = {"TagSet": []}
    get_client("s3", region).put_object_tagging(**payload)
