import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession integer,
    lastName varchar,
    length double precision,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration decimal,
    sessionId integer,
    song varchar,
    status integer,
    ts bigint,
    userAgent varchar,
    userId integer
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs integer, 
    artist_id varchar,
    artist_latitude double precision,
    artist_longitude double precision,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration double precision,
    year integer 
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id integer IDENTITY(0,1) NOT NULL PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id integer NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id integer,
    location varchar,
    user_agent varchar,
    foreign key(user_id) references users(user_id),
    foreign key(song_id) references songs(song_id),
    foreign key(artist_id) references artist(artist_id)
)
DISTKEY (song_id)
SORTKEY(start_time, session_id);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id integer NOT NULL PRIMARY KEY,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar NOT NULL PRIMARY KEY,
    title varchar,
    artist_id varchar,
    year varchar,
    duration double precision,
    foreign key(artist_id) references artist(artist_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist(
    artist_id varchar NOT NULL PRIMARY KEY,
    name varchar,
    location varchar,
    latitude double precision,
    longitude double precision
)

""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time timestamp NOT NULL PRIMARY KEY,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer
)
""")

# STAGING TABLES

staging_events_copy = ("""
DELETE FROM staging_events;
COPY staging_events(
    artist,
    auth,
    firstName,
    gender,
    itemInSession,
    lastName,
    length,
    level,
    location,
    method,
    page,
    registration,
    sessionId,
    song,
    status,
    ts,
    userAgent,
    userId
)
FROM {}
FORMAT JSON AS {} 
iam_role '{}' 
region '{}'
""").format(
    config.get('S3', 'LOG_DATA'), config.get('S3', 'LOG_JSONPATH'), config.get('IAM_ROLE', 'POLICY_ARN'),
    config.get('S3', 'BUCKET_REGION')
)

staging_songs_copy = ("""
DELETE FROM staging_songs;
COPY staging_songs(
    num_songs, 
    artist_id,
    artist_latitude,
    artist_longitude,
    artist_location,
    artist_name,
    song_id,
    title,
    duration,
    year  
)
FROM {}
FORMAT JSON AS 'auto'
iam_role '{}' 
region '{}'
""").format(
    config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'POLICY_ARN'), config.get('S3', 'BUCKET_REGION')
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' as start_time, 
        se.userId AS user_id,
        se.level AS level ,
        sso.song_id AS song_id,
        sso.artist_id AS artis_id,
        se.sessionId AS session_id,
        se.location AS location,
        se.userAgent AS user_agent
FROM staging_events se
INNER JOIN staging_songs sso ON se.song = sso.title AND se.artist = sso.artist_name
WHERE se.artist!='None';
""")

user_table_insert = ("""
INSERT INTO users(
    user_id, 
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT userId AS user_id, 
                firstName AS first_name, 
                lastName AS last_name, 
                gender AS gender, 
                level AS level 
FROM staging_events
WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT song_id AS song_id,
                title AS title,
                artist_id AS artist_id,
                year AS year,
                duration AS duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artist(
    artist_id,
    name,
    location,
    latitude,
    longitude
)

SELECT DISTINCT artist_id,
                artist_name as name,
                artist_location,
                artist_latitude,
                artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;

""")

time_table_insert = ("""
INSERT INTO time(
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekDay
)
SELECT start_time, 
    EXTRACT (hour from start_time),
    EXTRACT(day from start_time),
    EXTRACT(week from start_time), 
    EXTRACT(month from start_time),
    EXTRACT(year from start_time), 
    EXTRACT(dayofweek from start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, artist_table_create, song_table_create, songplay_table_create,
                        time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
