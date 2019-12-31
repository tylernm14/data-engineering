import os
import sys
import configparser
from datetime import datetime
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, year, month, dayofmonth, weekofyear,\
                                    hour, from_unixtime, col, when,\
                                    regexp_extract, row_number, lit, desc,\
                                    monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Creates and returns a new spark session """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def blank_to_null(x):
    """ Replace blanks (whitespace) with null value """
    return when(col(x) == regexp_extract(col(x), "(^\\s*$)", 1), None).otherwise(col(x))

def non_null_df(df, required_cols):
    """ Returns rows of dataframe where non-null values are in all required columns """
    return df.where(reduce(lambda x, y: x & y, (col(x).isNotNull() for x in required_cols)))

def process_song_data(spark, input_data, output_data):
    
    """
        Description:  This function extracts data from s3 json files to create song and
        artist tables.  It writes the new tables in parquet format to an output location
        
        Parameters:
            spark:         active spark session
            input_data:    location of folder with song_data JSON subfolders
            output_data:   location to save output parquet files
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    df.cache()

    song_fields = ['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # select distinct songs, fill null years with 0 since we need a year to partition on write
    songs = df.select(song_fields).fillna(0, subset=['year']).distinct()
    
    # remove any rows that nulls in our necessary fields
    required_cols = ['song_id', 'title', 'artist_id']
    songs = non_null_df(songs, required_cols)
    
    # write partitions to disk
    songs.write.mode('overwrite').partitionBy(
        'year', 'artist_id').parquet(os.path.join(output_data, 'songs'))
    
    artist_fields = ['artist_id', 'artist_name', 'artist_location',
                     'artist_latitude', 'artist_longitude']
    
    # select distinct artists, replace blank artist location values with null
    artists = df.select(artist_fields).distinct()\
                .withColumn('artist_location', blank_to_null('artist_location'))
    
    # remove rows missing required fields
    required_cols = ['artist_id', 'artist_name']
    artists = non_null_df(artists, required_cols)
    artists.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))
    
def process_log_data(spark, input_data, output_data):
    
    """
        Description:  This function extracts data from s3 json files to create user, time
        and songplays tables with the help of save song and artists tables.  It then outputs
        the newly created tables in parquet format to s3
        
        Parameters:
            spark:         active spark session
            input_data:    location with event/log json files in subdirectories
            output_data:   location to save parquet files
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')
    
    # read log data files
    df = spark.read.json(log_data)
    df.cache()
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong') 

    # select distinct users and drop any row that has a null for any column value
    users = df.select(col('userId').alias('user_id'),
                      col('firstName').alias('first_name'),
                      col('lastName').alias('last_name'), col('gender'),
                      col('level')).distinct().dropna('any')
    
    # write to partitioned parquest files
    users.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))
    
    # udf for day of week
    dayofweek = udf(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').strftime('%w'))

    # select distinct times and convert to timestamps, dropping nulls
    timestamps = df.select('ts').distinct().withColumn(
        'ts', from_unixtime(col('ts')/1000, 'yyyy-MM-dd HH:mm:ss')).dropna('any')
    
    # extract specifics of timestamp
    times = timestamps.select(col('ts').alias('start_time'),
                              hour('ts').alias('hour'),
                              dayofmonth('ts').alias('day'),
                              weekofyear('ts').alias('week'),
                              month('ts').alias('month'), year('ts').alias('year'), 
                              dayofweek(col('ts')).alias('weekday'))

    # write partitioned parquet files
    times.write.mode('overwrite').partitionBy('year', 'month')\
        .parquet(os.path.join(output_data, 'times'))

    # read in song and artist data to use for songplays table
    songs_df = spark.read.parquet(os.path.join(output_data, 'songs/*/*/*.parquet'))
    artists_df = spark.read.parquet(os.path.join(output_data, 'artists/*.parquet'))

    # join songs and events 
    log_song_df = df.join(songs_df, songs_df.title == df.song)
    log_song_artist_df = log_song_df.join(artists_df, artists_df.artist_name == df.artist)
                       
    # select distinct songplays and add id for unique increasing id
    songplays = log_song_artist_df.select(col('ts'),
                                          col('userId').alias('user_id'),
                                          col('level'),
                                          col('song_id'),
                                          col('artist_id'),
                                          col('sessionId').alias('session_id'),
                                          col('location'),
                                          col('userAgent').alias('user_agent'))\
        .distinct()\
        .withColumn('start_time', from_unixtime(col('ts')/1000, 'yyyy-MM-dd HH:mm:ss'))\
        .withColumn('year', year(col('start_time')))\
        .withColumn('month', month(col('start_time')))\
        .drop('ts')\
        .withColumn('sonplay_id', monotonically_increasing_id())
    
    # write partitioned parquet files
    songplays.write.mode('overwrite').partitionBy('year', 'month')\
        .parquet(os.path.join(output_data, 'songplays'))

def main(env='aws'):
    spark = create_spark_session()
    
    if env == 'local':
        input_data = './'
        output_data = './output/'
    else:
        input_data = "s3a://udacity-dend/"
        output_data = "s3a://tyler-sparkify-output/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == 'local':
        main('local')
    else:
        main()
