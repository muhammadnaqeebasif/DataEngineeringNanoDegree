import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear,
                                   date_format, dayofweek, monotonically_increasing_id)
from pyspark.sql.types import TimestampType, StringType

# CONFIG
config = configparser.ConfigParser()
config.read('dl.cfg')

# Setting up the environment variables
os.environ['AWS_ACCESS_KEY_ID']=config['CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Creates Spark Session

    Returns
    -------
    pyspark.sql.session.SparkSession
        SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Processes the song data stored in path specified by input_data and writes the result in
    the path specified by output_data

    Parameters
    ----------
    spark : pyspark.sql.session.SparkSession
        Spark Session
    input_data : str
        The path where the data is stored
    output_data : str
        The path to where result is to be written

    Returns
    -------
    None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # For debugging
    print('-----------------------------------------------------------------------------')
    print('Reading the song data \n')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration')
    
    # For debugging
    print('-----------------------------------------------------------------------------')
    print('Writing the songs table \n')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data + 'songs_table')

    # extract columns to create artists table
    artists_table = df.select('artist_id',col('artist_name').alias('name'),
                          col('artist_location').alias('location'),
                          col('artist_latitude').alias('lattitude'),
                          col('artist_longitude').alias('longitude'))\
                    .dropDuplicates()
    
    # For debugging
    print('-----------------------------------------------------------------------------')
    print('Writing the artists table \n')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table')


def process_log_data(spark, input_data, output_data):
    """ Processes the log data stored in path specified by input_data and writes the result in
    the path specified by output_data

    Parameters
    ----------
    spark : pyspark.sql.session.SparkSession
        Spark Session
    input_data : str
        The path where the data is stored
    output_data : str
        The path to where result is to be written

    Returns
    -------
    None
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # For debugging
    print('-----------------------------------------------------------------------------')
    print('Reading the log data \n')
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong')

    # extract columns for users table    
    users_table =  df.select(col('userId').alias('user_id'),
                             col('firstName').alias('first_name'),
                             col('lastName').alias('last_name'),
                             'gender','level')
    
    # For debugging
    print('-----------------------------------------------------------------------------')
    print('Writing the users table \n')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1000.0),TimestampType())
    df = df.withColumn("start_time", get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000.0).strftime('%Y-%m-%d %H:%M:%S'),StringType())
    df = df.withColumn('datetime',get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.select('start_time')\
                       .drop_duplicates()\
                       .select('start_time',
                                hour('start_time').alias('hour'),
                                dayofmonth('start_time').alias('day'),
                                weekofyear('start_time').alias('week'),
                                month('start_time').alias('month'),
                                year('start_time').alias('year'),
                                dayofweek('start_time').alias('weekday')
                              )
    
    # For debugging
    print('-----------------------------------------------------------------------------')
    print('Writing the time table \n')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data + 'time_table')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs_table/')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =  df.withColumn('songplay_id',monotonically_increasing_id())\
                             .join(song_df,df.song == song_df.title)\
                                .select('songplay_id',
                                        'start_time',
                                        col('userId').alias('user_id'),
                                        'level',
                                        'song_id',
                                        'artist_id',
                                        col('sessionId').alias('session_id'),
                                        'location',
                                        col('userAgent').alias('user_agent'),
                                        year('start_time').alias('year'),
                                        month('start_time').alias('month'))

    # For debugging
    print('-----------------------------------------------------------------------------')
    print('Writing the songplays table \n')
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data + 'songplays_table')


def main():
    """ Main function to call

    Returns
    -------
    None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://naqeeb-sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
