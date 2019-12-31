# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplay (
songplay_id SERIAL PRIMARY KEY,
start_time timestamp REFERENCES time,
user_id int REFERENCES users,
level varchar NOT NULL,
song_id varchar REFERENCES song,
artist_id varchar REFERENCES artist,
session_id int NOT NULL,
location varchar,
user_agent varchar);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY,
first_name varchar NOT NULL,
last_name varchar NOT NULL,
gender char NOT NULL,
level varchar NOT NULL);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS song (
song_id varchar PRIMARY KEY,
title varchar NOT NULL,
artist_id varchar REFERENCES artist,
year int NOT NULL CHECK (year >= 0),
duration numeric CHECK (duration > 0));
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artist (
artist_id varchar PRIMARY KEY,
name varchar NOT NULL,
location varchar,
latitude numeric CHECK ((latitude >= -90 AND latitude <= 90) OR (latitude = 'NaN')),
longitude numeric CHECK ((longitude >= -180 AND longitude <= 180) OR (longitude = 'NaN')));
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY,
hour int NOT NULL CHECK (hour >= 0 AND hour <= 23),
day int NOT NULL CHECK (day >= 1 AND day <= 31),
week int NOT NULL CHECK (week >= 0 AND week <= 53),
month int NOT NULL CHECK (month >= 1 AND month <= 12),
year int NOT NULL,
weekday int NOT NULL CHECK (weekday >= 0 AND weekday <= 6));
"""
# INSERT RECORDS

songplay_table_insert = """
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id,
session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""

# User data is udpated on conflict because we assume we are processing records chronslogically,
#    so it is OK to accept the updated data for a user
user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE
SET first_name=EXCLUDED.first_name,
    last_name=EXCLUDED.last_name,
    gender=EXCLUDED.gender,
    level=EXCLUDED.level;
"""

song_table_insert = """
INSERT INTO song (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO UPDATE
SET year=EXCLUDED.year WHERE song.year = 0 AND EXCLUDED.year > 0;
"""

artist_table_insert = """
INSERT INTO artist (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO UPDATE
SET location=COALESCE(EXCLUDED.location, artist.location),
    latitude=COALESCE(EXCLUDED.latitude, artist.latitude),
    longitude=COALESCE(EXCLUDED.longitude, artist.longitude);
"""


time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING
"""

# FIND SONGS

song_select = """
SELECT song_id, song.artist_id FROM song
JOIN artist ON song.artist_id=artist.artist_id
WHERE title=%s AND name=%s AND ROUND(duration, 5)=%s;
"""

# QUERY LISTS

create_table_queries = [time_table_create,
                        user_table_create,
                        artist_table_create,
                        song_table_create,
                        songplay_table_create
                        ]
drop_table_queries = [songplay_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      user_table_drop,
                      time_table_drop]