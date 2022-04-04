import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import dayofmonth, dayofweek, hour, month, weekofyear, year
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """ Create a spark session.
    
        Arguments:
            None
            
        Returns:
            spark (obj)
    """

    spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Process song data to create songs and artists tables.
    
        Arguments:
            spark (obj): SparkSession object.
            input_data (str): Source data S3 bucket.
            output_data (str): Target S3 bucket.
            
        Returns:
            None
    """
    
    # get filepath to song data file
    song_data = f'{input_data}*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data).dropDuplicates().cache()

    # extract columns to create songs table
    songs_table = df.select(col('song_id'), col('title'), col('artist_id'), col('year'), col('duration')).distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(f'{output_data}songs/songs_table.parquet', mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select(col('artist_id'), col('artist_name'), col('artist_location'), col('artist_latitude'), col('artist_longitude')).distinct()

    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}artists/artist_table.parquet', mode='overwrite')
    
    # create temp view of song_data for songplays table join
    df.createOrReplaceTempView('temp_song_data')
    # temp_song_data = df.createOrReplaceTempView(song_data)


def process_log_data(spark, input_data, output_data):
    """ Process log data to create users, time, and songplay tables.
    
        Arguments:
            spark: SparkSession object.
            input_data: Source data S3 bucket.
            output_data: Target S3 bucket.
            
        Returns:
            None
    """
    
    # get filepath to log data file
    log_data = f'{input_data}*/*/*.json'

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong').cache()

    # extract columns for users table
    users_table = df.select(col('userId').alias('user_id'), col('firstName').alias('first_name'), col('lastName').alias('last_name'), col('gender'), col('level')).distinct()

    # write users table to parquet files
    users_table.write.parquet(f'{output_data}users/user_table.parquet', mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp(col('ts')))

    # extract columns to create time table
    df = df.withColumn('hour', hour('start_time'))
    df = df.withColumn('day', dayofmonth('start_time'))
    df = df.withColumn('week', weekofyear('start_time'))
    df = df.withColumn('month', month('start_time'))
    df = df.withColumn('year', year('start_time'))
    df = df.withColumn('weekday', dayofweek('start_time'))

    time_table = df.select(col('start_time'), col('hour'), col('day'), col('week'),
                           col('month'), col('year'), col('weekday')).distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(f'{output_data}time/time_table.parquet', mode='overwrite')
    
    # read in song data to use for songplays table
    song_df = spark.sql('SELECT * FROM temp_song_data')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, 'inner').distinct()\
                        .select(col('start_time'), col('userId').alias('user_id'), col('level'), col('sessionId').alias('session_id'),
                                col('location'), col('userAgent').alias('user_agent'), col('song_id'), col('artist_id'),
                                df['year'], df['month'])\
                        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(f'{output_data}songplays/songplays_table.parquet', mode='overwrite')


def main():
    spark = create_spark_session()

    song_input_data = 's3a://udacity-dend/song_data/'
    log_input_data = 's3a://udacity-dend/log_data/'
    output_data = config.get('AWS', 'AWS_S3_BUCKET')

    process_song_data(spark, song_input_data, output_data)
    process_log_data(spark, log_input_data, output_data)


if __name__ == "__main__":
    main()
