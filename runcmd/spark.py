"""
runCMD library to support Apache Spark
"""
import sys
import traceback
import importlib
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from runcmd.logging_helper import get_logger

LOGGER = get_logger(__name__)

HIVE_STORE = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
SPARK_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"

class NoRecordFoundParquet(Exception):
    """
    Customer exception for no records found in Parquet.
    """
    pass

def driver():
    """
    Function to run a spark operation.
    """
    if len(sys.argv) == 1:
        raise SyntaxError("Please provide a module to load.")

    module_to_load = sys.argv[1]
    LOGGER.info("Loading Module %s" % module_to_load)
    try:
        module = importlib.import_module(module_to_load)
        sys.exit(module.main())
    except Exception: # pylint: disable=broad-except
        LOGGER.info("Problem processing operation: %s" % traceback.format_exc())
        sys.exit(99)

def create_spark_session(job_name, database="datalake", checkpoint_dir=None):
    """
    Creates a spark session.
    """
    spark = SparkSession.builder \
        .appName(job_name) \
        .config("hive.metastore.client.factory.class", HIVE_STORE) \
        .config("spark.serializer", SPARK_SERIALIZER) \
        .enableHiveSupport() \
        .getOrCreate()
    if database:
        spark.catalog.setCurrentDatabase(database)

    if checkpoint_dir:
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
    LOGGER.info("Created spark session, check point=%s, database=%s" % (checkpoint_dir, database))
    return spark


def remove_nulls_from_schema(spark, data_frame):
    """
    Fixes null data types in the schema as null data type cannot be saved.
    """
    LOGGER.info("Removing null data types from schema.")
    schema = data_frame.schema
    schema_updated = False
    for element in schema:
        if element.dataType.simpleString() == "null":
            element.dataType = StringType()
            schema_updated = True
    if schema_updated:
        return spark.createDataFrame(data_frame.rdd, schema)
    return data_frame


def bulk_column_rename(data_frame, renamed_prefix):
    """
    Renames all columns in data frame for a join operation.
    """
    LOGGER.info("Apppending prefix to columns in dataframe: %s." % renamed_prefix)
    schema = data_frame.schema
    for element in schema:
        data_frame = data_frame.withColumnRenamed(
            element.name, "%s_%s" % (renamed_prefix, element.name.lower()))
    return data_frame


def write_to_parquet(spark, dataframe, path, glue_table=None, mode="overwrite", remove_nulls=True): # pylint: disable=too-many-arguments
    """
    Writes a dataframe to S3 in parquet format.
    """
    if dataframe.count() == 0:
        raise NoRecordFoundParquet("No results to save.  This is an error condition")

    LOGGER.info("Writing dataframe to %s with glue table %s." % (path, glue_table))
    if remove_nulls:
        dataframe = remove_nulls_from_schema(spark, dataframe)
    operation = dataframe.write.mode(mode).format("parquet")
    if glue_table:
        operation.option("path", path).saveAsTable(glue_table)
    else:
        operation.save(path)

def convert_string_to_timestamp(input_df, input_column, output_column, ts_format):
    """
    Converting a string column to a timestamp.
    """
    LOGGER.info("Converting column %s to a timestamp column %s." % (input_column, output_column))
    return input_df.withColumn(output_column, F.to_timestamp(input_column, ts_format))

def add_raw_timestamp(input_df, column_name, raw_value, ts_format):
    """
    Adds a time stamp raw value to a data frame.
    """
    LOGGER.info("Setting %s in column %s using time format of %s." % \
        (raw_value, column_name, ts_format))
    temp_column = "%s_temp" % column_name
    temp_result = input_df.withColumn(temp_column, F.lit(raw_value))
    converted_result = convert_string_to_timestamp(temp_result, temp_column, column_name, ts_format)
    result = converted_result.drop(temp_column)
    return result

def print_schema(schema):
    """
    Prints a schema for debug purposes.
    """
    result = "Total fields: %s\n" % len(schema.fields)
    for field in schema.fields:
        result += "  Field %s\n" % field.simpleString()
    return result

def union_datasets(ds_1, ds_2):
    """
    Merges two data sets.
    """
    if ds_1:
        return ds_1.union(ds_2)
    return ds_2
