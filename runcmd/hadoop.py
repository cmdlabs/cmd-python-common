"""
Hadoop Helper Functions for Spark.
"""
from runcmd.logging_helper import get_logger

LOG = get_logger(__name__)

def does_file_exist(spark, path):
    """
    Checks if a file exists within spark.
    """
    context = spark.sparkContext
    hadoop_fs = context._jvm.org.apache.hadoop.fs.FileSystem.get(context._jsc.hadoopConfiguration()) # pylint: disable=protected-access
    result = hadoop_fs.exists(context._jvm.org.apache.hadoop.fs.Path(path)) # pylint: disable=protected-access
    LOG.info("Checking file %s, exists=%s" % (path, result))
    return result
