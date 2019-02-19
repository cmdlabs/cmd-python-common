"""
SQL Server Helper Functions for Spark.
"""
import pymssql
from runcmd import generate_args, ssm
from runcmd.spark import NoRecordFoundParquet, create_spark_session, write_to_parquet
from runcmd.logging_helper import get_logger

LOG = get_logger(__name__)
class ExecuteStoredProcedure(object): # pylint: disable=too-many-instance-attributes
    """
    Helper class to execute stored procedures in spark.
    """

    def __init__(self, spark, url, database_host, database, tmp_table, sp_name, sp_params, username, # pylint: disable=too-many-arguments
                 ssm_password, output_bucket, output_path, glue_table, mode="overwrite",
                 skip_errors=None):
        """
        :param spark: obj - Spark session object
        :param url: str - JDDB URL
        :param database_host: str - Database hostname
        :param database: str - database name
        :param tmp_table: str - temporary table name
        :param sp_name: str - stored procedure name
        :param sp_params: str - stored procedure parameters
        :param username: str - database user name
        :param ssm_password: str - ssm password key name
        :param output_bucket: str - s3 output bucket path
        :param output_path: str - s3 output bucket path
        :param glue_table: - glue table name
        :param mode: parquet file write mode ex: overwrite, append
        :param skip_errors: skip or suppress list of errors
        """
        self.spark = spark
        self.url = url
        self.database_host = database_host
        self.database = database
        self.tmp_table = tmp_table
        self.sp_name = sp_name
        self.sp_params = sp_params
        self.username = username
        self.ssm_password = ssm_password
        self.password = ssm.read_encrypted_value(spark, ssm_password)
        self.output_bucket = output_bucket
        self.output_path = output_path
        self.glue_table = glue_table
        self.mode = mode
        self.skip_errors = skip_errors

    def _execute_query(self, cursor, query, query_param):
        """
        Execute query

        :param cursor: database connection cursor
        :param query: string - sql query
        :param query_param: tuple - parameters used in sql query
        """
        try:
            LOG.info("Executing query '%s'" % query)
            if query_param:
                cursor.execute(query, query_param)
            else:
                cursor.execute(query)
        except pymssql.OperationalError as exp:
            LOG.exception("MSSQL Operation Error occurred %s" % exp)
            # Check any errors that need to be skipped
            if isinstance(self.skip_errors, list):
                skip_error_result = filter(lambda x: x in exp.message, self.skip_errors)
                if  skip_error_result:
                    LOG.info("Known error and it may be skipped: %s " % exp)

    def connect_db(self, query, query_param):
        """
        Connect Ms sql database and execute query

        :param query: string - sql query
        :param query_param: tuple - parameters used in sql query
        """
        with pymssql.connect(server=self.database_host,
                             user=self.username,
                             password=self.password,
                             database=self.database,
                             autocommit=True) as conn:
            with conn.cursor(as_dict=True) as cursor:
                if isinstance(query, list):
                    for ele in query:
                        self._execute_query(cursor, ele, query_param)
                else:
                    self._execute_query(cursor, query, query_param)

    def process_data(self):
        """
        Method does the following steps
        1. Connects to database
        2. truncate the temporary table
        3. execute stored procedure and save the result set to temporary table
        4. Read the data using spark and write the data as parquet file.
        5. truncate the temporary table
        """
        LOG.info("Loading table %s from %s as %s." % (self.tmp_table, self.url, self.username))

        query_param_dict = {"table_name": self.tmp_table,
                            "sp_name": self.sp_name,
                            "sp_params": self.sp_params}
        truncate_temp_table_query = "truncate table {table_name}".format(
            **query_param_dict)
        insert_temp_table_query = "insert into {table_name} exec {sp_name} {sp_params} ".format(
            **query_param_dict)
        LOG.info("Insert query: '%s'" % (insert_temp_table_query))
        self.connect_db(truncate_temp_table_query, query_param_dict)
        self.connect_db(insert_temp_table_query, query_param_dict)
        # read data using spark
        df_data = self.spark.read \
            .format("jdbc") \
            .option("url", self.url) \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("database", self.database) \
            .option("dbtable", self.tmp_table).load()

        try:
            # write record to parquet file
            path = "s3://%s/%s" % (self.output_bucket, self.output_path)
            write_to_parquet(self.spark, df_data, path, glue_table=self.glue_table, mode=self.mode)
        except NoRecordFoundParquet as exp:
            LOG.exception("No Record Found Parquet exception occurred %s." % exp)
        except Exception as exp2:
            LOG.exception("Parquet file write error %s." % exp2)
            raise
        # truncate the temporary table after fetching records through spark
        self.connect_db(truncate_temp_table_query, query_param_dict)
        LOG.info("Truncated temporary table after reading data through spark, query: '%s'" %
                 (truncate_temp_table_query))


def main():
    """
    Spark Entry Point
    """
    args = generate_args({
        "--jdbc-url": ["The JDBC URL", True],
        "--database-host": ["Database hostname", True],
        "--username": ["The username to access the database", True],
        "--ssm-password": ["SSM password property to access palace", True],
        "--output-bucket": ["The output bucket for the parquet transformation", True],
        "--output-path": ["The output file for the parquet transformation", True],
        "--database": ["Database to connect", True],
        "--glue-table": ["The glue table", True],
        "--checkpoint-dir": ["The checkpoint directory", True],
        "--temp-table-name": [" Temporary table to insert the result from stored proc", True],
        "--stored-procedure-name": ["Stored procedure name", True],
        "--stored-procedure-params": ["Stored procedure parameters", False]
    })
    spark = create_spark_session("exec_stored_procedure", checkpoint_dir=args.checkpoint_dir)

    exec_obj = ExecuteStoredProcedure(spark,
                                      args.jdbc_url,
                                      args.database_host,
                                      args.database,
                                      args.temp_table_name,
                                      args.stored_procedure_name,
                                      args.stored_procedure_params,
                                      args.username,
                                      args.ssm_password,
                                      args.output_bucket,
                                      args.output_path,
                                      args.glue_table)

    exec_obj.process_data()
