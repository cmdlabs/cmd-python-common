
"""
Prepared corporate db for an address match
"""
import pandas as pd
from runcmd import generate_args
from runcmd.spark import create_spark_session, write_to_parquet
from runcmd.logging_helper import get_logger

LOG = get_logger(__name__)

def replace_bads(string):
    """
    In order to save the CSV data as Parquet later, it is
    necessary to strip certain characters from the Headers.
    """
    result = string.replace(" ", "_")
    other_bad_list = [",", ";", "{", "}", "(", ")", "\n", "\t", "="]
    for element in other_bad_list:
        result = result.replace(element, "")
    return result

def parse_excel(spark, s3_input, s3_output, glue_table, skip_rows=None):
    """
    Parse reference excel file and convert it to parque file
    """
    LOG.info("Read data from excel: '%s', '%s'" % (s3_input, glue_table))

    # skip no. of rows from excel sheet.
    if skip_rows:
        skiprows = map(lambda x: int(x), skip_rows.split(','))
        pan_df = pd.read_excel(s3_input, skiprows=skiprows)
    else:
        pan_df = pd.read_excel(s3_input)

    # converting nan value to empty string in pandas
    pan_df_no_nan = pan_df.where((pd.notnull(pan_df)), "")

    # convert all the fields to unicode string
    pan_column_names = pan_df_no_nan.columns
    pan_df_no_nan[pan_column_names] = pan_df_no_nan[pan_column_names].astype('unicode')

    # convert pandas to pyspark data frame
    df_data = spark.createDataFrame(pan_df_no_nan)
    column_names = df_data.columns

    # removing bad characters from column names
    for ele in column_names:
        replaced_ele = replace_bads(ele)
        df_data = df_data.withColumnRenamed(ele, replaced_ele)

    # write record to parquet file
    write_to_parquet(spark, df_data, s3_output, glue_table=glue_table, mode="overwrite")
    LOG.info("After calling write_to_parquet method, count of dataframe: '%s'" % df_data.count())

def main():
    """
    Spark operation entry point.
    """
    args = generate_args({
        "--checkpoint-dir": ["checkpoint dir", True],
        "--input-path": ["Input bucket name", True],
        "--glue-table": ["GLue table", True],
        "--output-path": ["Output bucket name", True],
        "--skip-rows": ["Skip no of rows in excel", False],
        "--glue-database": ["The glue database to use", False]
    })
    if args.glue_database:
        spark = create_spark_session("excel_import",
                                     database=args.glue_database,
                                     checkpoint_dir=args.checkpoint_dir)
    else:
        spark = create_spark_session("excel_import", checkpoint_dir=args.checkpoint_dir)
    parse_excel(spark, args.input_path, args.output_path, args.glue_table, args.skip_rows)
