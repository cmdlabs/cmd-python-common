"""
Helper Object for joining of data frames.
"""
from runcmd.spark import bulk_column_rename, write_to_parquet
from runcmd.logging_helper import get_logger

LOG = get_logger(__name__)

class JoinDataFrame(object):
    """
    Helper Object for joining of data frames.
    """

    def __init__(self, # pylint: disable=too-many-arguments
                 spark,
                 output_bucket,
                 output_path,
                 glue_table,
                 list_of_joining_glue_table,
                 joining_column_name,
                 join_type="full"):
        """
        Class used for joinging glue table by converting into dataframes and
        return flat data structure data frame

        :param output_path: str - Output bucket path
        :param output_bucket: str - Output bucket name
        :param glue_table: str - output result glue table
        :param list_of_joining_glue_table: str - comma separated value of glue table names
        :param joining_column_name: str - common names in all those tables to be used for joins
        :param join_type="full": str - type of join, ex: left, right, full

        """
        self.spark = spark
        self.output_bucket = output_bucket
        self.output_path = output_path
        self.glue_table = glue_table
        self.list_of_joining_glue_table = list_of_joining_glue_table
        self.joining_column_name = joining_column_name
        self.join_type = join_type

    def renaming_column(self, table_name):
        """
        Rename columns after creating dataframe from glue table

        :param runcmd: obj - runcmd module which contains utility functions
        :param table_name: glue table name, basically it contains data from stored procedure
        :param join_column_name: column name used for joining table
        """

        LOG.info("JoinDataFrame, renaming column method")
        rename_prefix_from_table_name = table_name.split("_")[-1]
        query = "select * from {0} ".format(table_name)
        LOG.info("JoinDataFrame, query, %s" % query)
        LOG.info("JoinDataFrame, rename prefix: %s" % (rename_prefix_from_table_name))
        data_df = bulk_column_rename(self.spark.sql(query), rename_prefix_from_table_name)
        # rename back to joining column name after bulk column rename
        column_canid = "{0}_{1}".format(rename_prefix_from_table_name, self.joining_column_name)
        data_df = data_df.withColumnRenamed(column_canid, self.joining_column_name)
        return data_df

    def run_process(self):
        """
        Executes the process
        """
        LOG.info("JoinDataFrame, run_process method")
        joining_list = self.list_of_joining_glue_table.split(",")
        previous_df = self.renaming_column(joining_list[0])
        result_df = ""

        # joining dataframe using joining column name field
        for ele in joining_list[1:]:
            next_df = self.renaming_column(ele)
            result_df = previous_df.join(next_df, self.joining_column_name, how=self.join_type)
            previous_df = result_df

        LOG.info("JoinDataFrame, Resulting schema=%s" % result_df.schema)
        LOG.info("JoinDataFrame, Resulting record count=%s." % result_df.count())
        path = "s3://%s/%s" % self.output_bucket, self.output_path
        write_to_parquet(self.spark, result_df, path, glue_table=self.glue_table)
        LOG.info("JoinDataFrame, Process Completed")
