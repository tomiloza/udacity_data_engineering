import configparser
from datetime import datetime
import os
import pandas as pd
import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, weekofyear, dayofweek, isnan, when, count, col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StringType

CONFIG_FILENAME = "capstone.cfg"


def create_spark_session():
    """
    Creates apache part sessions
    :return: session object
    """
    # This doesn't work the park-sas7bdat:2.0.0-s_2.11 libarry is not present anymore on the maven repository,
    # if you want to loose a a week in trying to fix this issue uncomment it and fail
    # spark = SparkSession \
    #    .builder \
    #    .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
    #    .enableHiveSupport() \
    #    .getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    return spark


def process_immigration_data(spark, immigr_path, port_path, config, output_path):
    """
    Load data from immigration dataset and create necessary fact tables
    :param output_path: path to where parquet files will be written
    :param port_path: path to port code file
    :param spark: spark session object
    :param immigr_path filepath of immigration data
    :param config: config object contains configuration files
    :return:
    """
    # get filepath to song data file
    immigration_data_file_name = config['DATASETS']['IMMIGRATION_DATA_FILE']
    immigration_data_file_and_path = os.path.join(immigr_path, immigration_data_file_name)
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ[
        "PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
    os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

    print("Reading immigration data:" + immigration_data_file_and_path)
    immigration_df = spark.read.load(immigration_data_file_and_path)

    # clean immigration dataset
    immigration_df = clean_immigration_data(immigration_df)

    # create arr_date table
    arr_date_dimension = create_arr_date_table(immigration_df)

    # create visa_type dimension table
    visa_type_dimension = create_visa_type_dimension_table(immigration_df)

    # create port_code dimension table
    port_code_dimension = create_port_codes_dimension_table(spark, config, port_path)

    immigration_df = prepare_immigration_table(immigration_df, visa_type_dimension, spark)

    data_quality_check_immigration(spark, immigration_df, port_code_dimension, arr_date_dimension, visa_type_dimension)

    write_immigration_tables(output_path, arr_date_dimension, visa_type_dimension, port_code_dimension,
                             immigration_df)
    return immigration_df


def process_demographics_data(spark, demographics_path, config, output_path, immigration_fact):
    """Process the demographics data and create the usa_demographics_dim table
    Parameters:
    -----------
    spark (SparkSession): spark session instance
    input_data (string): input file path
    output_data (string): output file path
    file_name (string): usa demographics csv file name
    """
    demographic_file_name = config['DATASETS']['DEMOGRAPHIC_DATA_FILE']
    demographics_data_file_and_path = os.path.join(demographics_path, demographic_file_name)
    print('File path to demographics file: {}'.format(demographics_data_file_and_path))

    df_demographic = spark.read.csv(demographics_data_file_and_path, inferSchema=True, header=True, sep=';')
    df_demographic = clean_demographic_data(df_demographic)
    df_demographic = prepare_demographics_dimension_table(df_demographic)
    data_quality_check_demographics(spark, immigration_fact, df_demographic)
    write_demographic_table(output_path, df_demographic)


def main():
    """
    Creating sparks session and loading data from configuration.
    Processing immigration and demographics dataset and save in parquet format
    :return:
    """
    print("Creating spark session")
    spark = create_spark_session()
    config = get_config(CONFIG_FILENAME)
    local_or_remote = config['DATASETS']['LOCAL_OR_REMOTE']
    if local_or_remote == 'LOCAL':
        output_path_local = config['DATASETS']['OUTPUT_PATH_LOCAL']
        print("processing immigration data")
        immigration_df = process_immigration_data(spark, config['DATASETS']['IMMIGRATION_DATA_PATH_LOCAL'],
                                                  config['DATASETS']['PORT_DATA_PATH_LOCAL'], config,
                                                  output_path_local)
        print("processing demographics data")
        process_demographics_data(spark, config['DATASETS']['DEMOGRAPHIC_DATA_PATH_LOCAL'], config, output_path_local,
                                  immigration_df)
    else:
        output_path_remote = config['DATASETS']['OUTPUT_PATH_REMOTE']
        print("processing immigration data")
        immigration_df = process_immigration_data(spark, config['DATASETS']['IMMIGRATION_DATA_PATH_REMOTE'],
                                                  config['DATASETS']['PORT_DATA_PATH_REMOTE'], config,
                                                  output_path_remote)
        print("processing demographics data")
        process_demographics_data(spark, config['DATASETS']['DEMOGRAPHIC_DATA_PATH_REMOTE'], config, output_path_remote,
                                  immigration_df)


def get_config(file_path):
    """
    Returns configuration loaded from file_name
    :param file_path: filename for loading configuration
    :return: configuration object
    """
    config = configparser.ConfigParser()
    print("read config file from file_path:", file_path)
    assert os.path.exists(file_path)
    config.read(file_path)
    return config


def clean_immigration_data(df):
    """
    Method cleans immigration dataset of missing values in columns and duplicate/NaN values as primary key
    :param df: dataframe forwarded for cleaning
    :return: cleaned dataframe
    """
    count_all_immigration_records = df.count()
    print('Number of immigration rows before cleaning: {}'.format(count_all_immigration_records))

    # Validate if dataset has more then 1 million rows
    assert count_all_immigration_records > 1000000, 'Dataset must have more the 1 million rows'
    num_of_columns_before_cleanup = len(df.columns)
    print('Total number of immigration columns: {}'.format(num_of_columns_before_cleanup))

    # Evaluate missing values in columns
    columns_missing_values = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
    rows = columns_missing_values.collect()
    # column filtering based on your nan condition
    missing_value_df_above_70 = [''.join(key) for _ in rows for (key, val) in _.asDict().items() if
                                 val > int(float(count_all_immigration_records) * 0.7)]
    print('Following columns will be dropped from immigration dataset (NaN values above 70%): {}'.format(
        missing_value_df_above_70))
    for column in missing_value_df_above_70:
        print('Dropping column in immigration dataframe:{}'.format(column))
        df = df.drop(column)
    num_columns_after_cleanup = len(df.columns)

    # Removing possible duplicates and empty values in primary key cicid
    df = df.dropDuplicates(['cicid'])
    df = df.na.drop(subset=['cicid'])

    print('Number of immigration rows before cleanup: {}'.format(count_all_immigration_records))
    print('Number of immigration rows after cleanup: {}'.format(df.count()))
    print('Number of immigration columns before cleanup: {}'.format(num_of_columns_before_cleanup))
    print('Number of immigration columns after cleanup: {}'.format(num_columns_after_cleanup))
    return df


def clean_demographic_data(df):
    """
    Method cleans demographic dataset of missing values in columns and duplicate/NaN values as primary key
    :param df: dataframe forwarded for cleaning
    :return: cleaned dataframe
    """
    count_all_demographic_records = df.count()
    num_of_columns_before_cleanup = len(df.columns)
    # Evaluate missing values in columns
    columns_missing_values = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
    rows = columns_missing_values.collect()
    # column filtering based on your nan condition
    missing_value_df_above_70 = [''.join(key) for _ in rows for (key, val) in _.asDict().items() if
                                 val > int(float(count_all_demographic_records) * 0.7)]
    if len(missing_value_df_above_70) > 0:
        print('Following columns will be dropped from demographics dataset (NaN values above 70%): {}'.format(
            missing_value_df_above_70))
        for column in missing_value_df_above_70:
            print('Dropping column in demographics dataframe:{}'.format(column))
            df = df.drop(column)
    num_columns_after_cleanup = len(df.columns)
    clean_df_demographic = df.drop_duplicates(subset=['City', 'Race', 'State Code'])
    clean_df_demographic = clean_df_demographic.dropna()

    print('Number of demographic rows before cleanup: {}'.format(df.count()))
    print('Number of demographic rows after cleanup: {}'.format(clean_df_demographic.count()))
    print('Number of demographic columns before cleanup: {}'.format(num_of_columns_before_cleanup))
    print('Number of demographic columns after cleanup: {}'.format(num_columns_after_cleanup))
    return clean_df_demographic


def create_visa_type_dimension_table(df):
    """
     Creates visa_type dimension table from distinct values of visatpye column from immigration dataset
    :param df: Immigration dataframe as input
    :return:
    """
    distinct_visatype = df.select(['visatype']).distinct()
    distinct_visatype = distinct_visatype.withColumnRenamed('visatype', 'visa_type')
    print(
        "Creating visatype dimension from distinct values. Number of visatype values:{}".format(
            distinct_visatype.count()))
    visa_type_dimension = distinct_visatype.withColumn('visa_type_id', monotonically_increasing_id())
    return visa_type_dimension


def create_arr_date_table(df):
    """
    This method creates arrival date table from immigration data frame as input
    :param df: dataframe of immigration data
    :return: arr_date table with arrival date broken down to single values
    """

    print('Creating arr_date dimension table from arrdate column')

    # SAS date value is a value that represents the number of days between January 1, 1960, and a specified
    # This UDF converts SAS data value to standard datetime format
    arr_date_table = df.select(['arrdate']).withColumn("arr_date", convert_sas_to_datetime(df.arrdate)).distinct()
    arr_date_table = arr_date_table.withColumn('arr_day', dayofmonth('arr_date'))
    arr_date_table = arr_date_table.withColumn('arr_week', weekofyear('arr_date'))
    arr_date_table = arr_date_table.withColumn('arr_month', month('arr_date'))
    arr_date_table = arr_date_table.withColumn('arr_year', year('arr_date'))
    arr_date_table = arr_date_table.withColumn('arr_weekday', dayofweek('arr_date'))
    arr_date_table = arr_date_table.withColumn('id', monotonically_increasing_id())
    arr_date_table.createOrReplaceTempView("dimension_arr_date")
    return arr_date_table


@udf(StringType())
def convert_sas_to_datetime(x):
    if x:
        return (datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat()
    return None


def create_port_codes_dimension_table(spark, config, port_path):
    """
    Method create dimension table with port codes
    :param spark: spark session
    :param config: configuration object
    :param port_path: path to port_file
    :return:
    """
    port_file_name = config['DATASETS']['PORT_DATA_DATA_FILE']
    port_path_and_file = os.path.join(port_path, port_file_name)
    print('File path for port data: {}'.format(port_path_and_file))
    df_port = spark.read.csv(port_path_and_file, sep=';', header=True)
    df_port.createOrReplaceTempView("dimension_port_code")
    return df_port.distinct()


def prepare_immigration_table(df, visa_type_dimension, spark):
    """
    Method renames columns of immigration table and creates necessary visa_type dimension entries
    :param df: immigration dataframe
    :param visa_type_dimension visa_type dataframe
    :param spark: spark sessions object
    :return: dataframe with renames columns and filled foreign keys
    """
    visa_type_dimension.createOrReplaceTempView("dimension_visa_type")
    df = df.withColumnRenamed('cicid', 'id') \
        .withColumnRenamed('i94cit', 'birth_country_code') \
        .withColumnRenamed('i94res', 'residence_code') \
        .withColumnRenamed('i94addr', 'state') \
        .withColumnRenamed('i94port', 'port_code') \
        .withColumnRenamed('depdate', 'dep_date') \
        .withColumnRenamed('i94mode', 'arr_mode') \
        .withColumnRenamed('matflag', 'match_flag') \
        .withColumnRenamed('biryear', 'birth_year') \
        .withColumnRenamed('dtaddto', 'stay_until') \
        .withColumnRenamed('admnum', 'adm_num') \
        .withColumnRenamed('fltno', 'flight_num') \
        .withColumnRenamed('entdepa', 'arr_flag') \
        .withColumnRenamed('entdepd', 'dep_flag') \
        .withColumnRenamed('i94yr', 'year') \
        .withColumnRenamed('94visa', 'visa_code') \
        .withColumnRenamed('visapost', 'visa_post') \
        .withColumnRenamed('i94bir', 'age') \
        .withColumnRenamed('visatype', 'visa_type')

    df.createOrReplaceTempView("immigration_fact")
    df = spark.sql(
        """
        SELECT 
            immigration_fact.*, 
            dimension_visa_type.visa_type_id
        FROM immigration_fact
        LEFT JOIN dimension_visa_type ON dimension_visa_type.visa_type=immigration_fact.visa_type
        """
    )
    df = df.withColumn("arr_date", convert_sas_to_datetime(df.arrdate))
    df = df.drop(df.visa_type)
    return df


def write_immigration_tables(output_path, arr_date_dimension, visa_type_dimension, port_code_dimension,
                             immigration_fact):
    print('Write visa_type dimension table to path:{}'.format(output_path + "dim_visa_type"))
    visa_type_dimension.write.parquet(output_path + "dim_visa_type", mode="overwrite")

    print('Write fact_immigration  table to path:{}'.format(output_path + "fact_immigration"))
    immigration_fact.write.parquet(output_path + "fact_immigration", mode="overwrite")

    print('Write dim_arr_date table to path:{}'.format(output_path + "dim_arr_date"))
    arr_date_dimension.write.parquet(output_path + "dim_arr_date", partitionBy=['arr_year', 'arr_month', 'arr_week'],
                                     mode="overwrite")

    print('Write  dim_port_code table to path:{}'.format(output_path + "dim_port_code"))
    port_code_dimension.write.parquet(output_path + "dim_port_code", mode="overwrite")


def write_demographic_table(output_path, dimension_demographic):
    print('Write dim_demographic table to path:{}'.format(output_path + "dim_demographic"))
    dimension_demographic.write.parquet(output_path + "dim_demographic", mode="overwrite")


def prepare_demographics_dimension_table(df):
    """
    Method renames columns and create necessary primary key id for table
    :param df: dataframe of demographics tables
    :return: table with primary key and renamed columns
    """
    dimension_demographics_df = df.withColumnRenamed('Median Age', 'median_age') \
        .withColumnRenamed('Male Population', 'male_population') \
        .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
        .withColumnRenamed('Female Population', 'female_population') \
        .withColumnRenamed('Total Population', 'total_population') \
        .withColumnRenamed('State Code', 'state_code') \
        .withColumnRenamed('Average Household Size', 'average_household_size') \
        .withColumnRenamed('Foreign-born', 'foreign_born') \
        .withColumnRenamed('City', 'city') \
        .withColumnRenamed('State', 'state_name') \
        .withColumnRenamed('Median Age', 'median_age') \
        .withColumnRenamed('State Code', 'state_code') \
        .withColumnRenamed('Race', 'race') \
        .withColumnRenamed('Count', 'count')

    dimension_demographics_df = dimension_demographics_df.withColumn('id', monotonically_increasing_id())
    dimension_demographics_df.createOrReplaceTempView("dimension_demographics")
    return dimension_demographics_df


def data_quality_check_immigration(spark, immigration_fact, dimension_port_code, dimension_arr_date,
                                   dimension_visa_type):
    """
    Method checks if all tables dont contain null values in primary key. Also dimension table check are executed
    :param spark: spark session context
    :param immigration_fact: immigration dataframe
    :param dimension_port_code:  port_code dataframe
    :param dimension_arr_date:  arr_date dimension dataframe
    :param dimension_visa_type: visa_type dimension
    :return:
    """
    sql_check = [
        ('SELECT COUNT(*) FROM immigration_fact WHERE id IS NULL', 0),
        ('SELECT COUNT(*) FROM dimension_port_code WHERE port_code IS NULL', 0),
        ('SELECT COUNT(*) FROM dimension_arr_date WHERE arr_date IS NULL', 0),
        ('SELECT COUNT(*) FROM dimension_visa_type WHERE visa_type IS NULL', 0)
    ]
    for query in sql_check:
        print('--------------------EXECUTING SQL TEST-------------------------')
        sql_query, expected_result = query
        print("Validating query:{} , Expecting result {}".format(sql_query, expected_result))
        result = spark.sql(sql_query)
        new_count = result.collect()[0][0]
        if new_count != expected_result:
            raise ValueError("ERROR Validating query:{} , Expected result: {}, Returned_result: {}".format(sql_query,
                                                                                                           expected_result,
                                                                                                           new_count))
        else:
            print("Validation OK! Validating query:{} , Expected result: {}, Returned_result: {}".format(sql_query,
                                                                                                         expected_result,
                                                                                                         new_count))

    distinct_arr_date_fact = immigration_fact.select("arr_date").distinct().count()
    distinct_arr_date_dimension = dimension_port_code.distinct().count()
    if distinct_arr_date_fact != distinct_arr_date_dimension:
        ValueError(
            "arr_date in fact table {} are not equal arr_date in dimension table {}".format(distinct_arr_date_fact,
                                                                                            distinct_arr_date_dimension))

    distinct_visa_type_fact = immigration_fact.select("visa_type_id").distinct().count()
    distinct_visa_type_dimension = dimension_visa_type.distinct().count()
    if distinct_visa_type_fact != distinct_visa_type_dimension:
        ValueError(
            "visa_type in fact table {} are not equal visa_type in dimension table {}".format(distinct_visa_type_fact,
                                                                                              distinct_visa_type_dimension))


def data_quality_check_demographics(spark, immigration_fact,
                                    dimension_demographics):
    """
    Method checks if all tables dont contain null values in primary key. Also dimension table check are executed
    :param spark: spark session context
    :param immigration_fact: immigration dataframe
    :param dimension_demographics:  dimension_demographics dataframe
    :return:
    """
    sql_check = [
        ('SELECT COUNT(*) FROM dimension_demographics WHERE state_code IS NULL', 0)
    ]
    for query in sql_check:
        print('--------------------EXECUTING SQL TEST-------------------------')
        sql_query, expected_result = query
        print("Validating query:{} , Expecting result {}".format(sql_query, expected_result))
        result = spark.sql(sql_query)
        new_count = result.collect()[0][0]
        if new_count != expected_result:
            raise ValueError("ERROR Validating query:{} , Expected result: {}, Returned_result: {}".format(sql_query,
                                                                                                           expected_result,
                                                                                                           new_count))
        else:
            print("Validation OK! Validating query:{} , Expected result: {}, Returned_result: {}".format(sql_query,
                                                                                                         expected_result,
                                                                                                         new_count))

    distinct_state_codes_fact = immigration_fact.select("state").distinct().count()
    distinct_demogprahic_dimension = dimension_demographics.distinct().count()
    if distinct_state_codes_fact > distinct_demogprahic_dimension:
        ValueError(
            "Not all state codes are present in dimension table. Values:{},{}".format(distinct_state_codes_fact,
                                                                                      distinct_demogprahic_dimension))


if __name__ == "__main__":
    main()
