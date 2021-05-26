import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.functions import monotonically_increasing_id

CONFIG_FILENAME = "/home/hadoop/emr.cfg"
SONG_DATA_FILES = "song_data/*/*/*/*.json"
LOG_DATA_FILES = "log_data/*/*/*.json"


def create_spark_session():
    """
    Creates apache part sessions
    :return: session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load data from S3 bucket for song dataset extract columns for song and artist table
    and write to parquet files which are saved in S3
    :param spark: spark session object
    :param input_data: Path to S3 bucket with song/artist data
    :param output_data: output S3 bucket where parquet files are saved
    :return:
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, SONG_DATA_FILES)

    print("Reading song data:" + song_data)

    # read song data file
    df = spark.read.json(song_data)
    print("Number of rows in song data: %s" % df.count())

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude').dropDuplicates()

    print("artists_table writing to parquet")
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Load data from S3 bucket for song dataset extract columns for song and artist table
    and write to parquet files which are saved in S3
    :param spark: spark session object
    :param input_data: Path to S3 bucket with song/artist data
    :param output_data: output S3 bucket where parquet files are saved
    :return:
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, LOG_DATA_FILES)

    # read log data file
    actions = spark.read.json(log_data)
    print("Number of rows in action data: %s" % actions.count())

    # filter by actions for song plays
    actions = actions.filter(actions.page == "NextSong")
    print("Filtered  rows in action data: %s" % actions.count())

    # extract columns for users table    
    users_table = actions.select('userId', 'firstName', 'lastName',
                                 'gender', 'level').dropDuplicates()

    print("users_table writing to parquet")
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: str(int(int(ts) / 1000)))
    actions = actions.withColumn('timestamp', get_timestamp(actions.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: str(datetime.fromtimestamp(int(ts) / 1000)))
    actions = actions.withColumn('datetime', get_datetime(actions.ts))

    # extract columns to create time table
    time_table = actions.select('datetime').withColumn('start_time', actions.datetime).withColumn('hour',
                                                                                                  hour(
                                                                                                      'datetime')).withColumn(
        'day', dayofmonth('datetime')).withColumn('week', weekofyear('datetime')).withColumn('month', month(
        'datetime')).withColumn('year', year('datetime')).withColumn('weekday', dayofweek('datetime')).dropDuplicates()

    print("time_table writing to parquet")
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'), 'overwrite')

    print("reading files for songs df")
    # read in song data to use for songplays table
    songs = spark.read.json(os.path.join(input_data, SONG_DATA_FILES))

    # extract columns from joined song and log datasets to create songplays table 
    joined_actions = actions.join(songs, songs.title == actions.song)
    songplays_table = joined_actions[
        'datetime', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()

    print("songplays_table writing to parquet")
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), 'overwrite')


def main():
    """
    Creating sparks session and loading data from configuration.
    Processing song and log data from S3 buckets and saving to S3 in parquet format
    :return:
    """
    print("Creating spark session")
    spark = create_spark_session()
    config = get_config(CONFIG_FILENAME)
    input_data = config.get("S3", "INPUT_DATA_BUCKET_S3_PATH")
    output_data = config.get("S3", "OUTPUT_DATA_BUCKET_S3_PATH")
    print("processing song data")
    process_song_data(spark, input_data, output_data)
    print("processing log data")
    process_log_data(spark, input_data, output_data)


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
    print("Configuration:", config.items("CLUSTER"))

    return config


if __name__ == "__main__":
    main()
