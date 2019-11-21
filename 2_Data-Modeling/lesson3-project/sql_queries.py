# DROP TABLES
"""
Collection of SQL queries to drop all existing tables in this project
"""

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
"""
Collection of SQL queries to create the needed tables 
"""

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id SERIAL, 
start_time bigint REFERENCES time, 
user_id varchar REFERENCES users, 
level varchar, 
song_id varchar REFERENCES songs, 
artist_id varchar REFERENCES artists, 
session_id varchar, 
location varchar, 
user_agent varchar, 
PRIMARY KEY (start_time, user_id));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id varchar PRIMARY KEY, 
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY, 
title varchar, 
artist_id varchar, 
year int, 
duration numeric);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY, 
name varchar, 
location varchar, 
latitude numeric, 
longitude numeric);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time bigint PRIMARY KEY, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday varchar);
""")

# INSERT RECORDS
"""
Collection of SQL queries to insert data into the specified tables 
"""

songplay_table_insert = ("""
INSERT INTO songplays ( 
start_time, 
user_id, 
level, 
song_id,  
artist_id, 
session_id, 
location, 
user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time, user_id)
DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users (
user_id, 
first_name, 
last_name, 
gender, 
level) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) 
DO NOTHING;
""")

song_table_insert = ("""
INSERT INTO songs (
song_id, 
title, 
artist_id, 
year, 
duration) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) 
DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (
artist_id, 
name, 
location, 
latitude, 
longitude) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) 
DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (
start_time, 
hour, 
day, 
week, 
month, 
year, 
weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) 
DO NOTHING;
""")

# FIND SONGS
"""
SQL query to extract song_id and artist_id by a given title, artist name 
and duration of the song
"""

song_select = ("""
SELECT s.song_id, s.artist_id
FROM songs s
JOIN artists a
ON s.artist_id = a.artist_id
WHERE (s.title = %s AND a.name = %s AND s.duration = %s);
""")

# QUERY LISTS
"""
Define lists of all the queries to create and drop tables
"""

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]