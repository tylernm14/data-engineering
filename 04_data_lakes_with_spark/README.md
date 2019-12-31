# Spark for Data Lakes @ Sparkify

This project creates an ETL pipeline using Spark to allow analytics on Sparkify song plays.
  Spark allows for easy schema inference, and capitializes on in-memory cache design to create
   a more performant ETL pipeline than Hadoop.  Hopefully dumping the outputs of this ETL pipeline
    will add value for Sparkify data scientists in addition to growing the Sparkify data lake.
    
The ETL script works by extracting json data out of an AWS S3 bucket, transforming it into our
fact and dimension tables, as outlined by our analysts, and loads the results into S3 as parquet files.

## Usage

To run the pipeline in the __project workspace__ simply run the file "test_main.ipynb" which just runs the ETL script
 from a jupyter notebook.

```
!python etl.py local
```

To run on AWS:
1. Create an S3 output bucket to store the output of the ETL process.
2. Put credentials in `dl.cfg` for AWS
5. Change the output S3 bucket location and or path as desired in `main()` function of etl.py
2. Create an EMR cluster with spark.  Make sure to use a known key-pair and add your IP to the master security group to
login via SSH
3. Ensure permissions for the cluster allow reading from your input and output S3 buckets
4. SCP the `etl.py` and `dl.cfg` files to the cluster
5. Configure the spark cluster to use python3:
    `sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh`
5. Log into a terminal on the master node of cluster and run `/usr/bin/spark-submit --master yarn ./etl.py`
6. The output parquet files will show up in your output s3 bucket once the jobs finish

Alternatively to run on AWS:
1. Open up a new jupyter notebook on your Spark EMR cluster
2. Upload `etl.py` and `dl.cfg`
3. Change your AWS credentials in dl.cfg and your s3 input and output location in `etl.py::main()`
3. From a new notebook run `!python etl.py` (notice there is no "local" parameter)


## Project Files

```
project root/
|
|-- data/                       # Sample data for testing
|-- dl.cfg                      # Config for connectiong to AWS
|-- etl.py                      # Main python script to do ETL of S3 data
|-- README.md                   # Project info (this file)
|-- test.ipynb                  # Scratch file for experiments on data
|-- test_main.ipynb             # Run main ETL from notebook


```
## ETL

The schema design is inferred from the ingested json files.  Because of this some 
cleaning is needed to remove nulls or assign some default values.  For instance, 
some fields are required for meaningful analytics, like a song needs a title, and an 
artist needs a name, and all tables need a unqiue id of some sort.  Also because the files are being partitioned by
 year, a default value of year=0 is assigned to songs without a year.

### Imported Schema

**Imported Songs Data Schema**
```
root
 |-- artist_id: string (nullable = true)
 |-- artist_latitude: double (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_longitude: double (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- duration: double (nullable = true)
 |-- num_songs: long (nullable = true)
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- year: long (nullable = true)
 ```
 **Imported Logs Data Schema**
 ```

 root
 |-- artist: string (nullable = true)
 |-- auth: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- itemInSession: long (nullable = true)
 |-- lastName: string (nullable = true)
 |-- length: double (nullable = true)
 |-- level: string (nullable = true)
 |-- location: string (nullable = true)
 |-- method: string (nullable = true)
 |-- page: string (nullable = true)
 |-- registration: double (nullable = true)
 |-- sessionId: long (nullable = true)
 |-- song: string (nullable = true)
 |-- status: long (nullable = true)
 |-- ts: long (nullable = true)
 |-- userAgent: string (nullable = true)
 |-- userId: string (nullable = true)
 ```
## Transformed Schema
### Fact Table
We filter for records from the logs that have a page=NextSong to only get song plays

songplays - records in log data associated with song plays i.e. records with page NextSong
- songplay_id (long)
- start_time (string) - timestamp
- user_id (long)
- level (string)
- song_id (string)
- artist_id (string)
- session_id (long)
- location (string)
- user_agent (string)

### Dimension Tables

users - users in the app
 - user_id (long)
 - first_name (string)
 - last_name (string)
 - gender (string)
 - level (string)

songs - songs in music database
- song_id (string)
- title (string)
- artist_id (string)
- year (long)
- duration (double)

artists - artists in music database
- artist_id (string)
- name (string)
- location (string)
- latitude (double)
- longitude (double)

time - timestamps of records in songplays broken down into specific units
- start_time (string) - timestamp
- hour (long)
- day (long)
- week (long)
- month (long)
- year (long)
- weekday (long)
