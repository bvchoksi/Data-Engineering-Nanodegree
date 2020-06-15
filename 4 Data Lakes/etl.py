import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

def create_spark_session():
    """
    Create a Spark session.
        
    Args:
        None.
    
    Returns:
        Nothing.
    
    Raises:
        Any errors raised by SparkSession.
    """
    try:
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
            .getOrCreate()
        return spark
    except Exception as e:
        print("Error:", e)


def process_song_data(spark, song_input_data, output_data):
    """
    Loads and transforms song data from json files into song and artist parquet tables.
        
    Args:
        spark: Spark session instance.
        song_input_data: Location of song data json files in S3.
        output_data: Location to store parquet tables in S3.
    
    Returns:
        Nothing.
    
    Raises:
        Any errors raised by Spark.
    """
    try:
        # read song data file
        song_df = spark.read.json(song_input_data)
        song_df.createOrReplaceTempView("song")
        
        # extract columns to create songs table
        song_table = spark.sql("""
            select distinct
                song_id,
                title,
                artist_id,
                year,
                duration
            from
                song
        """)
    
        # write songs table to parquet files partitioned by year and artist
        song_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs.parquet")
        
        # extract columns to create artists table
        artist_table = spark.sql("""
            select distinct
                artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
            from
                song
        """)
        
        # write artists table to parquet files
        artist_table.write.parquet(output_data + "artists.parquet")
    except Exception as e:
        print("Error:", e)


def process_log_data(spark, log_input_data, song_input_data, output_data):
    """
    Loads and transforms log and song data from json files into user, time and songplay parquet tables.
        
    Args:
        spark: Spark session instance.
        log_input_data: Location of log data json files in S3.
        song_input_data: Location of song data json files in S3.
        output_data: Location to store parquet tables in S3.
    
    Returns:
        Nothing.
    
    Raises:
        Any errors raised by Spark.
    """
    try:
        # read log data file
        log_df = spark.read.json(log_input_data)
        
        from pyspark.sql.types import StringType
        get_datetime = udf(lambda ts: datetime.fromtimestamp(float(ts)/1000.).strftime('%Y-%m-%d %H:%M:%S'), StringType())
        
        # create datetime column from original timestamp column
        # filter by actions for song plays
        log_df = log_df.withColumn('start_time', get_datetime('ts')).filter("page='NextSong'")
        log_df.createOrReplaceTempView("log")
        
        # extract columns for users table    
        user_table = spark.sql("""
            select distint
                userId as user_id,
                firstName as first_name,
                lastName as last_name,
                gender,
                level
            from
                log
        """)
        
        # write users table to parquet files
        user_table.write.parquet(output_data + "users.parquet")
        
        # extract columns to create time table
        time_table = spark.sql("""
            select distinct
                start_time,
                hour(start_time) as hour,
                day(start_time) as day,
                weekofyear(start_time) as week,
                month(start_time) as month,
                year(start_time) as year,
                weekday(start_time) as weekday
            from
                log
        """)
        
        # write time table to parquet files partitioned by year and month
        time_table.write.partitionBy('year', 'month').parquet(output_data + "times.parquet")
        
        # read in song data to use for songplays table
        song_df = spark.read.json(song_input_data)
        song_df.createOrReplaceTempView("song")
        
        # extract columns from joined song and log datasets to create songplays table 
        songplay_table = spark.sql("""
            select
                row_number() over (partition by l.ts order by l.userId) as songplay_id,
                l.start_time,
                l.userId as user_id,
                l.level,
                s.song_id,
                s.artist_id,
                l.sessionId as session_id,
                l.location,
                l.userAgent as user_agent
            from
                song s
            inner join log l on
                s.artist_name=l.artist and
                s.title=l.song
        """)
        
        # write songplays table to parquet files partitioned by year and month
        songplay_table.withColumn('year', year(col('start_time'))).write.partitionBy('year', 'artist_id').parquet(output_data + "songplays.parquet")
    except Exception as e:
        print("Error:", e)


def main():
    """
    Parses the dl.cfg configuration file to retrieve AWS and S3 parameters.
    Establishes a Spark session.
    Calls function to load and transform song data files from S3 into parquet tables on S3.
    Calls function to load and transform log and song data files from S3 into parquet tables on S3.
    
    Args:
        None.
    
    Returns:
        Nothing.
    
    Raises:
        Any errors raised by configparser or Spark.
    """
    try:
        config = configparser.ConfigParser()
        config.read('dl.cfg')

        os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

        spark = create_spark_session()
        song_input_data = config['S3']['SONG_INPUT_DATA']
        log_input_data = config['S3']['LOG_INPUT_DATA']
        output_data = config['S3']['OUTPUT_DATA']

        process_song_data(spark, song_input_data, output_data)
        process_log_data(spark, log_input_data, song_input_data, output_data)
    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    main()
