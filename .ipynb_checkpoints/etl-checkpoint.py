import os
from configparser import ConfigParser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType

config = ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"]     = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]
os.environ["AWS_DEFAULT_REGION"]    = config["AWS"]["AWS_DEFAULT_REGION"]

def create_spark_session():
  spark = SparkSession\
    .builder\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
    .getOrCreate()

  return spark

def get_song_data(spark, input_data):
  # define song schema
  song_schema = StructType([
      StructField("artist_id", StringType()),
      StructField("artist_name", StringType()),
      StructField("artist_latitude", DoubleType()),
      StructField("artist_longitude", DoubleType()),
      StructField("artist_location", StringType()),
      StructField("song_id", StringType()),
      StructField("duration", DoubleType()),
      StructField("title", StringType()),
      StructField("year", IntegerType()),
      StructField("num_songs", IntegerType())
  ])

  # read song files
  return spark.read.json(input_data, schema=song_schema)

def process_song_data(spark, input_data, output_data):  
  # read song data file
  df_song = get_song_data(spark, input_data)
  df_song.createOrReplaceTempView("songs")

  # create songs table and write to parquet 
  songs_table = spark.sql("""
    SELECT 
      title, 
      artist_id, 
      year, 
      duration 
    FROM songs
    """).dropDuplicates()
  songs_table.createOrReplaceTempView("songs")
  songs_table.write.partitionBy("year", "artist_id").parquet(output_data + '/songs', mode="overwrite")

  # create artists table and write to parquet 
  artists_table = spark.sql("""
    SELECT 
      DISTINCT artist_id, 
      artist_name as name, 
      artist_location as location, 
      artist_latitude as latitude, 
      artist_longitude as longitude 
    FROM songs
    """).dropDuplicates()
  artists_table.createOrReplaceTempView("artists")
  artists_table.write.partitionBy("artist_id").parquet(output_data + '/artists', mode="overwrite")


def get_log_data(spark, input_data):
  # get filepath to song data file
  log_data = f"{input_data}/log-data/*.json"

  # define log schema
  log_schema = StructType([
      StructField("artist", StringType()),
      StructField("auth", StringType()),
      StructField("firstName", StringType()),
      StructField("gender", StringType()),
      StructField("itemInSession", IntegerType()),
      StructField("lastName", StringType()),
      StructField("length", DoubleType()),
      StructField("level", StringType()),
      StructField("location", StringType()),
      StructField("method", StringType()),
      StructField("page", StringType()),
      StructField("registration", DoubleType()),
      StructField("sessionId", IntegerType()),
      StructField("song", StringType()),
      StructField("status", IntegerType()),
      StructField("ts", TimestampType()),
      StructField("userAgent", StringType()),
      StructField("userId", IntegerType())
  ])
  
  # read log files
  return spark.read.json(log_data, schema=log_schema)


def process_log_data(spark, input_data, output_data):
  # read log data file
  df_log = get_log_data(spark, input_data)
  df_log.createOrReplaceTempView("logs")

  # create users table and write to parquet 
  users_table = spark.sql("""
    SELECT 
      DISTINCT userId as user_id,
      firstName as first_name,
      lastName as last_name,
      gender,
      level
    FROM logs 
    WHERE page = 'NextSong'
  """).dropDuplicates()
  users_table.createOrReplaceTempView("users")
  users_table.write.partitionBy("user_id", "gender").parquet(output_data + '/users', mode="overwrite")

  # create time table
  time_table = spark.sql("""
    SELECT 
      DISTINCT ts as start_time,
      HOUR(ts) as hour,
      DAY(ts) as day,
      WEEK(ts) as week,
      MONTH(ts) as month,
      YEAR(ts) as year,
      WEEKDAY(ts) as weekday
    FROM logs 
    WHERE page = 'NextSong'
  """).dropDuplicates()
  time_table.createOrReplaceTempView("times")
  time_table.write.partitionBy("year", "month").parquet(output_data + '/times', mode="overwrite")

  # extract columns from joined song and log datasets to create songplays table 
  songplays_table = spark.sql("""
    SELECT DISTINCT
      l.ts,
      t.year,
      t.month as month,
      l.userId as user_id,
      l.level as level,
      s.song_id as song_id,
      s.artist_id as artist_id,
      l.sessionId as session_id,
      s.artist_location as artist_location,
      l.userAgent as user_agent
    FROM songs s
    JOIN times t
      ON t.ts = l.ts
    JOIN logs l
      ON s.artist_name = l.artist
      AND s.title = l.song
      AND s.duration = l.length
    WHERE page = 'NextSong'
  """).dropDuplicates()
  songplays_table.write.partitionBy("year", "month").parquet(output_data + '/time', mode="overwrite")


def main():
  spark = create_spark_session()
  input_data = "data"
  output_data = "data/output"
  # input_data = "s3a://udacity-dend/"
  # output_data = "s3a://sparkify-bucket-hedcler"
  
  process_song_data(spark, f"{input_data}/song_data/A/A/*/*.json", output_data)
  # process_log_data(spark, f"{input_data}/log-data/*.json", output_data)


if __name__ == "__main__":
  main()
