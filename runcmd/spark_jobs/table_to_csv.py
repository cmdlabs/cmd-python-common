"""
Converts a glue table to a csv file
"""
from runcmd import generate_args, spark as S
from runcmd.logging_helper import get_logger

JOB_NAME = "table_to_csv"
LOGGER = get_logger(JOB_NAME)

def main():
    """
    Entry point for Spark Driver Execution.
    """
    args = generate_args({
        "--bucket": "The destination bucket",
        "--prefix": "The destination prefix",
        "--glue-table": "The name of the table to export",
        "--checkpoint-dir": "The checkpoint directory",
    })
    spark = S.create_spark_session(JOB_NAME, checkpoint_dir=args.checkpoint_dir)
    s3_path = "s3://%s/%s" % (args.bucket, args.prefix)
    LOGGER.info("Writing table %s to path %s." % (args.glue_table, s3_path))
    dataframe = spark.sql("select * from %s" % args.glue_table)
    S.write_to_csv(dataframe, args.bucket, args.prefix)
    LOGGER.info("Operation Completed.")
