import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')
S3_SONGS_DATASET = config.get('S3','SONG_DATA')
S3_EVENTS_DATASET = config.get('S3','LOG_DATA')
LOG_JSONPATH_FILE = config.get('S3','LOG_JSONPATH')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist          varchar,
    auth            varchar,
    firstName       varchar,
    gender          char,
    itemInSession   integer,
    lastName        varchar,
    length          float,
    level           varchar,
    location        varchar,
    method          varchar,
    page            varchar,
    registration    varchar,
    sessionId       integer,
    song            varchar,
    status          integer,
    ts              timestamp,
    userAgent       varchar,
    userId          integer);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id           varchar,
    artist_latitude     float,
    artist_location     varchar,
    artist_longitude    float,
    artist_name         varchar,
    duration            float,
    num_songs           integer,
    song_id             varchar,
    title               varchar,
    year                integer);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id   integer identity(0,1) not null,
    start_time    timestamp not null,
    user_id       varchar not null,
    level         varchar,
    song_id       varchar not null,
    artist_id     varchar not null,
    session_id    varchar not null,
    location      varchar,
    user_agent    varchar);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id       varchar not null,
    first_name    varchar,
    last_name     varchar,
    gender        char,
    level         varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id   varchar not null,
    title     varchar,
    artist_id varchar,
    year      integer,
    duration  float);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar not null,
    name      varchar,
    location  varchar,
    latitude  float,
    longitude float);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time timestamp not null,
    hour       integer not null,
    day        integer not null,
    week       integer not null,
    month      integer not null,
    year       integer not null,
    weekday    integer not null);""")

# STAGING TABLES

# 1. Staging events table
# Using COPY command to extract data from S3 bucket into staging table on Redshift.
#   Parameters include:
#       FROM        - Data source location (which is on S3)
#       CREDENTIALS - Redshift credentials (which is IAM Role's ARN)
#       JSON        - Data source is formatted as JSON with specified JSONPath file
#       TIMEFORMAT  - Specify time format of data source (which is in epoch millisecond)
#       EMPTYASNULL - Specify all empty values from data source (such as blank string '') must be converted to NULL
#       REGION      - Specify data source's region on AWS
staging_events_copy = ("""
COPY staging_events
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
JSON '{}'
TIMEFORMAT 'epochmillisecs'
EMPTYASNULL
REGION 'us-west-2';
""").format(S3_EVENTS_DATASET, DWH_ROLE_ARN, LOG_JSONPATH_FILE)

# 2. Staging songs table
# Using COPY command to extract data from S3 bucket into staging table on Redshift.
#   Parameters include:
#       FROM            - Data source location (which is on S3)
#       CREDENTIALS     - Redshift credentials (which is IAM Role's ARN)
#       JSON            - Data source is formatted as JSON 'auto'
#       TRUNCATECOLUMNS - Truncates data in columns to the appropriate number of characters so that it fits the column specification
#       EMPTYASNULL     - Specify all empty values from data source (such as blank string '') must be converted to NULL
#       REGION          - Specify data source's region on AWS
staging_songs_copy = ("""
COPY staging_songs
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
JSON 'auto'
TRUNCATECOLUMNS
EMPTYASNULL
REGION 'us-west-2'
""").format(S3_SONGS_DATASET, DWH_ROLE_ARN)

# TRANSFORM DATA & LOAD TO FINAL TABLES

# 1. songplays table
songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  se.ts,
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
FROM staging_songs ss
JOIN staging_events AS se
    ON ss.artist_name = se.artist
    AND ss.title = se.song;
""")

# 2. users table
# Log data may have rows of the same user. So we need to de-duplicate staging table,
# and load only the row with the latest information (largest timestamp). This makes sure 
# we only load the most recent user's information
user_table_insert = ("""
INSERT INTO users
(
SELECT  temp_se.userId,
        temp_se.firstName,
        temp_se.lastName,
        temp_se.gender,
        temp_se.level
FROM (
    SELECT  se.userId,
            se.firstName,
            se.lastName,
            se.gender,
            se.level,
            ROW_NUMBER () OVER(PARTITION BY se.userId ORDER BY se.ts DESC) AS ts_order
    FROM staging_events AS se
    WHERE se.userId IS NOT NULL
) AS temp_se
WHERE temp_se.ts_order = 1
);
""")

# 3. songs table
# De-duplicate and load into songs table
song_table_insert = ("""
INSERT INTO songs
(
    SELECT  ss_with_row_num.song_id,
            ss_with_row_num.title,
            ss_with_row_num.artist_id,
            ss_with_row_num.year,
            ss_with_row_num.duration
    FROM (
        SELECT  ss.song_id,
                NULLIF(ss.title, ''),
                ss.artist_id,
                NULLIF(ss.year, 0),
                ss.duration,
                ROW_NUMBER () OVER(PARTITION BY ss.song_id) AS row_num
        FROM staging_songs ss
        WHERE ss.song_id IS NOT NULL
    ) AS ss_with_row_num
    WHERE ss_with_row_num.row_num = 1
);
""")

# 4. artist table
# De-duplicate and load into artist table
artist_table_insert = ("""
INSERT INTO artists
(
    SELECT  ss_with_row_num.artist_id,
            ss_with_row_num.artist_name,
            ss_with_row_num.artist_location,
            ss_with_row_num.artist_latitude,
            ss_with_row_num.artist_longitude
    FROM (
        SELECT  ss.artist_id,
                NULLIF(ss.artist_name, ''),
                ss.artist_location,
                ss.artist_latitude,
                ss.artist_longitude,
                ROW_NUMBER () OVER(PARTITION BY ss.artist_id) AS row_num
        FROM staging_songs ss
        WHERE ss.artist_id IS NOT NULL
    ) AS ss_with_row_num
    WHERE ss_with_row_num.row_num = 1
);
""")

# 5. time table
time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT se.ts,
                DATE_PART(hr, se.ts) AS hour,
                DATE_PART(d, se.ts) AS day,
                DATE_PART(w, se.ts) AS week,
                DATE_PART(mon, se.ts) AS month,
                DATE_PART(y, se.ts) AS year,
                DATE_PART(dow, se.ts) AS weekday
FROM staging_events AS se;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
