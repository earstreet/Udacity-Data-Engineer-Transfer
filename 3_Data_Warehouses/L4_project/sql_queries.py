import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
IAM_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
DB_REGION = config.get('CLUSTER', 'DB_REGION')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
artist TEXT,
auth TEXT,
firstName TEXT,
gender TEXT,
itemInSession INTEGER,
lastName TEXT,
length FLOAT,
level TEXT,
location TEXT,
method TEXT,
page TEXT,
registration TEXT,
sessionId INTEGER,
song TEXT,
status INTEGER,
ts TIMESTAMP,
userAgent TEXT,
userId INTEGER)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
num_songs INTEGER, 
artist_id TEXT, 
artist_latitude FLOAT, 
artist_longitude FLOAT, 
artist_location TEXT, 
artist_name TEXT, 
song_id TEXT, 
title TEXT, 
duration FLOAT, 
year INTEGER)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id int IDENTITY(0,1) PRIMARY KEY, 
start_time timestamp NOT NULL REFERENCES time sortkey, 
user_id varchar NOT NULL REFERENCES users, 
level varchar NOT NULL, 
song_id varchar NOT NULL REFERENCES songs, 
artist_id varchar NOT NULL REFERENCES artists distkey, 
session_id varchar NOT NULL, 
location varchar NOT NULL, 
user_agent varchar NOT NULL);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id varchar PRIMARY KEY sortkey, 
first_name varchar NOT NULL, 
last_name varchar NOT NULL, 
gender varchar NOT NULL, 
level varchar NOT NULL)
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY sortkey, 
title varchar NOT NULL, 
artist_id varchar NOT NULL, 
year int NOT NULL, 
duration numeric NOT NULL)
DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY distkey, 
name varchar NOT NULL, 
location varchar NOT NULL, 
latitude numeric NOT NULL, 
longitude numeric NOT NULL);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY sortkey, 
hour int NOT NULL, 
day int NOT NULL, 
week int NOT NULL, 
month int NOT NULL, 
year int NOT NULL, 
weekday varchar NOT NULL);
""")

# STAGING TABLES

staging_events_copy = (f"""COPY staging_events 
FROM '{LOG_DATA}' 
IAM_ROLE '{IAM_ROLE_ARN}' 
REGION '{DB_REGION}'
JSON '{LOG_JSONPATH}'
TIMEFORMAT 'epochmillisecs';
""")

staging_songs_copy = (f"""COPY staging_songs 
FROM '{SONG_DATA}' 
IAM_ROLE '{IAM_ROLE_ARN}' 
REGION '{DB_REGION}'
JSON 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id,
                         session_id, location, user_agent)
SELECT DATE_TRUNC('hour', se.ts), se.userid, se.level, ss.song_id, ss.artist_id, se.sessionid, 
        se.location, se.useragent 
FROM staging_events se
INNER JOIN staging_songs ss
ON se.song = ss.title;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid, firstname, lastname, gender, level 
FROM staging_events
WHERE userid IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration 
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, 
    ISNULL(artist_location, 'NaN') AS location, 
    ISNULL(artist_latitude, 0.0) AS latitude, 
    ISNULL(artist_longitude,0.0) AS longitude
FROM staging_songs 
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT DATE_TRUNC('hour', ts) AS start_time,
CAST(DATE_PART('hour', ts) as Integer) AS hour,
CAST(DATE_PART('day', ts) as Integer) AS day,
CAST(DATE_PART('week', ts) as Integer) AS week,
CAST(DATE_PART('month', ts) as Integer) AS month,
CAST(DATE_PART('year', ts) as Integer) AS year,
CAST(DATE_PART('weekday', ts) as Integer) AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
