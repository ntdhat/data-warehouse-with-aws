### Date created
01/30/2022

## Data Warehouse for Sparkify

### Description

The analytics team at Sparkify is particularly interested in understanding what songs users are listening to. Currently, their data is stored on S3 cloud, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project is built for the purpose of creating an ETL pipeline that extracts Sparkify's data from S3, stages them in Redshift, and transforms that data into dimension and fact tables dedicated for queries on song play analysis.

### Instructions

1. This project requires the Redshift cluster is available and has the read access to S3
2. Input needed data warehouse's specification into `dwh.cfg`:
   - Your AWS key and secret
   - Redshift cluster's database credentials such as database identifier, database admin/password, database port, etc.
   - Redshift IAM Role's ARN
3. Run the `create_tables.py` to create a new clean database and tables (this file should be run to reset tables before each time running ETL scripts.)
4. Run the `etl.py` to read and process song data and log data and load the data into the data warehouse's tables
5. Access your Redshift and excersise your queries base on the newly generated database

### Files in the project

1. `dwh.cfg` keeps data warehouse configuarion, S3 bucket links, and other credentials
2. `create_tables.py` drops and creates tables (run this file to reset the tables before each time running ETL scripts.)
3. `etl.py` is the main ETL logic. It extracts files from S3 into staging tables, transforms, and loads them into dimentional tables.
4. `sql_queries.py` contains all SQL queries, and is imported into the three files above to be executed.
5. `test_queries.ipynb` access to the Redshift Cluster and provides example queries showing song play analysis
6. `dataset_explore.ipynb` explores the data on S3 before loading them to Redshift

### Database schema

**Fact Table**

1. songplays - records in log data associated with song plays
    - songplay\_id, start\_time, user\_id, level, song\_id, artist\_id, session\_id, location, user\_agent

**Dimension Tables**

2. users - users in the app
    - user\_id, first\_name, last\_name, gender, level
3. songs - songs in music database
    - song\_id, title, artist\_id, year, duration
4. artists - artists in music database
    - artist\_id, name, location, latitude, longitude
5. time - timestamps of records in songplays broken down into specific units
    - start\_time, hour, day, week, month, year, weekday

### The Datasets

There are 2 datasets which reside on S3:

**1. Song dataset** (s3://udacity-dend/song_data)

The first dataset consists of log files in JSON format containing metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

    song_data/A/B/C/TRABCEI128F424C983.json
    song_data/A/A/B/TRAABJL12903CDCF1A.json

**2. Log dataset** (s3://udacity-dend/log_data)

The second dataset consists of log files in JSON format. The log files are partitioned by year and month. For example, here are filepaths to two files in this dataset.

    log_data/2018/11/2018-11-12-events.json
    log_data/2018/11/2018-11-13-events.json
