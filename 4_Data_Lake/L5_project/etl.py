import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, IntegerType
from pyspark.sql.functions import monotonically_increasing_id
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create Spark Session."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data and write tables to parquet.
    
    The function reads all JSON files from input_data, extracts columns to \
    create tables for songs and artists table. The tables are written to \
    output_data as parquet files.
    
    Parameters: 
    spark: Spark session
    input_data: S3 bucket path with input data
    output_data: S3 bucket path to store output tables
    """
    logging.info('Processing song data ...')
    # get filepath to song data file
    song_data = os.path.join(input_data, 'data/song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    logging.info(f'Song data from {song_data} read into dataframe')
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    logging.info('Songs table created')
    logging.info(f'Dimensions of table: ({songs_table.count()}, {len(songs_table.columns)})')
    
    # write songs table to parquet files partitioned by year and artist
    songs_path = os.path.join(output_data, 'songs_table.parquet')
    logging.info(f'Write table to {songs_path} ...')
    songs_table.write.parquet(songs_path, partitionBy=['year', 'artist_id'])
    logging.info(f'Songs table successfully written')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    logging.info('Artists table created')
    logging.info(f'Dimensions of table: ({artists_table.count()}, {len(artists_table.columns)})')
    
    # write artists table to parquet files
    artists_path = os.path.join(output_data, 'artists_table.parquet')
    logging.info(f'Write table to {artists_path} ...')
    artists_table.write.parquet(artists_path)
    logging.info(f'Artists table successfully written')


def process_log_data(spark, input_data, output_data):
    """
    Process log data and write tables to parquet.
    
    The function reads all JSON files from input_data, filters data by \ 
    'NextSong', extracts columns to create tables for users, time and \
    songplays table. The tables are written to output_data as parquet files.
    
    Parameters: 
    spark: Spark session
    input_data: S3 bucket path with input data
    output_data: S3 bucket path to store output tables
    """
    logging.info('Processing log data ...')
    # get filepath to log data file
    log_data = os.path.join(input_data, 'data/log-data/*.json')

    # read log data file
    df = spark.read.json(log_data)
    logging.info(f'Log data from {log_data} read into dataframe')
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')
    logging.info('Log data filtered')

    # extract columns for users table    
    users_table = df.selectExpr(
        'userId AS user_id', 
        'firstName AS first_name', 
        'lastName AS last_name', 
        'gender', 
        'level'
        ).dropDuplicates()
    logging.info('Users table created')
    logging.info(f'Dimensions of table: ({users_table.count()}, {len(users_table.columns)})')
    
    # write users table to parquet files
    users_path = os.path.join(output_data, 'users_table.parquet')
    logging.info(f'Write table to {users_path} ...')
    users_table.write.parquet(users_path)
    logging.info(f'Users table successfully written')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.selectExpr(
        'timestamp AS start_time',
        'hour(timestamp) AS hour',
        'day(timestamp) AS day',
        'weekofyear(timestamp) AS week',
        'month(timestamp) AS month',
        'year(timestamp) AS year',
        'weekday(timestamp) AS weekday'
        ).dropDuplicates()
    logging.info('Time table created')
    logging.info(f'Dimensions of table: ({time_table.count()}, {len(time_table.columns)})')
    
    # write time table to parquet files partitioned by year and month
    time_path = os.path.join(output_data, 'time_table.parquet')
    logging.info(f'Write table to {time_path} ...')
    time_table.write.parquet(time_path, partitionBy=['year', 'month'])
    logging.info(f'Time table successfully written')

    # read in song data to use for songplays table
    songs_path = os.path.join(output_data, 'songs_table.parquet')
    songs_df = spark.read.parquet(songs_path)
    logging.info(f'Songs table successfully read from {songs_path}')
    
    # read in artists data to use for songplays table
    artists_path = os.path.join(output_data, 'artists_table.parquet')
    artists_df = spark.read.parquet(artists_path)
    logging.info(f'Artists table successfully read from {artists_path}')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(songs_df, df.song == songs_df.title)\
        .join(artists_df, ['artist_id'])\
        .selectExpr(
        'timestamp AS start_time',
        'userId AS user_id',
        'level',
        'song_id',
        'artist_id',
        'sessionId AS session_id',
        'location',
        'userAgent AS user_agent')\
        .withColumn("songplay_id", monotonically_increasing_id())
    logging.info('Songplays table created')
    logging.info(f'Dimensions of table: ({songplays_table.count()}, {len(songplays_table.columns)})')

    # write songplays table to parquet files partitioned by year and month
    songplays_path = os.path.join(output_data, 'songplays_table.parquet')
    logging.info(f'Write table to {songplays_path} ...')
    songplays_table.write.parquet(songplays_path)
    logging.info(f'Songplays table successfully written')


def main():
    """
    Main function to call all functions for the etl process.
    """
    spark = create_spark_session()
    logging.info('Spark Session created')
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-emr-project"
    #input_data = './'
    #output_data = '/Users/daniel/Desktop/output/'
    logging.info(f'Set input path to {input_data}')
    logging.info(f'Set output path to {output_data}')
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    logging.info('ETL process successfully finished.')


if __name__ == "__main__":
    logging.info('Starting main()')
    main()
