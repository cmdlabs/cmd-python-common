"""
Process to remove S3 object tags from a prefix.
"""
from runcmd import generate_args, s3 as S3
from runcmd.logging_helper import get_logger

JOB_NAME = "s3_tag_removal"
LOGGER = get_logger(JOB_NAME)

def main():
    """
    Entry point for Spark Driver Execution.
    """
    args = generate_args({
        "--bucket": "The input bucket of the files to process",
        "--prefix": "The input prefix where those files live",
    })
    items = S3.list_items_with_prefix(args.bucket, args.prefix)
    for item in items:
        LOGGER.info("Removing tags from s3://%s/%s" % (item.bucket_name, item.key))
        S3.remove_all_tags(item.bucket_name, item.key)
