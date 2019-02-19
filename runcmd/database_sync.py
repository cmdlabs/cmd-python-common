"""
Spark operation to synchronize a database.
"""
from runcmd import generate_args, ssm
from runcmd.spark import create_spark_session, write_to_parquet
from runcmd.logging_helper import get_logger

LOG = get_logger(__name__)

def connect_to_database(spark, url, database, table, username, ssm_password, driver=None): # pylint: disable=too-many-arguments
    """
    Connects to a database and reads a table.
    """
    LOG.info("Loading table %s from url %s as user %s." % (table, url, username))
    password = ssm.read_encrypted_value(ssm_password)
    connection = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("user", username) \
        .option("password", password) \
        .option("database", database) \
        .option("dbtable", table).load()
    if driver:
        return connection.option("driver", driver)
    return connection

def main():
    """
    Entry point for Spark Database Synchronization.
    """
    args = generate_args({
        "--jdbc-url": "The JDBC URL",
        "--username": "The username to access the database",
        "--ssm-password": "SSM password property to access palace",
        "--output-bucket": "The output bucket for the parquet transformation",
        "--output-path": "The output file for the parquet transformation",
        "--jdbc-driver": ["The JDBC driver classname", False],
        "--database": "Database to connect",
        "--table": "The table to load",
        "--glue-table": "The glue table",
        "--glue-database": ["The glue database to use", False],
        "--checkpoint-dir": "The checkpoint directory"
    })
    if args.glue_database:
        spark = create_spark_session("database_sync",
                                     database=args.glue_database,
                                     checkpoint_dir=args.checkpoint_dir)
    else:
        spark = create_spark_session("database_sync", checkpoint_dir=args.checkpoint_dir)
    dataframe = connect_to_database(spark, args.jdbc_url, args.database, args.table,
                                    args.username, args.ssm_password, driver=args.jdbc_driver)
    write_to_parquet(
        spark, dataframe, args.output_bucket, args.output_path, args.glue_table)
