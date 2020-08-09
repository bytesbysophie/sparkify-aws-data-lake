import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Param: None
    Return: Spark Session

    Initializes and returns a spark session
    '''

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Param: 
        spark       : Spark Session
        input_data  : Input data location
        output_data : Outout data target location
    Return: 
        None
    '''

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table =  df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'name', 'location', 'latitude', 'longitude'])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
        '''
    Param: 
        spark       : Spark Session
        input_data  : Input data location
        output_data : Outout data target location
    Return: 
        None
    '''

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where("page = 'NextSong'")

    # extract columns for users table    
    users_table = df_ns.select(['user:id', 'first_name', 'last_name', 'gender', 'level'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn('datetime', get_timestamp(df.ts)))

    # extract columns to create time table
    time_table = df.selectExpr(
        "timestamp as start_time"
        ,"hour(timestamp) as hour"
        ,"dayofyear(timestamp) as day"
        ,"weekofyear(timestamp) as week"
        ,"month(timestamp) as month"
        ,"year(timestamp) as year"
        ,"weekday(timestamp) as weekday"
        ) 

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    song_df = song_df.select(['song_id','title','artist_id','artist_name'])

    # extract columns from joined song and log datasets to create songplays table 
    log_song_df = df.selectExpr(
        "timestamp as start_time",
        "userId",
        "level",
        "sessionId",
        "location",
        "userAgent",
        "song",
        "artist"
        ) 

    songplays_table = log_song_df.join(
        song_df,
        (song_df.title == log_song_df.song) 
        & (song_df.artist_name == log_song_df.artist),
        "left"
        )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
