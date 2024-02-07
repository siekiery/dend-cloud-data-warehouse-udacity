import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS timetable"

# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length VARCHAR,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userID INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration NUMERIC,
    year INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id   BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time    BIGINT NOT NULL REFERENCES timetable(start_time),
    user_id       INT NOT NULL REFERENCES users(user_id),
    level         VARCHAR,
    song_id       VARCHAR NOT NULL REFERENCES songs(song_id),
    artist_id     VARCHAR NOT NULL REFERENCES artists(artist_id),
    session_id    INT,
    location      VARCHAR,
    user_agent    VARCHAR
) 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     INT PRIMARY KEY,
    first_name  VARCHAR,
    last_name   VARCHAR,
    gender      VARCHAR,
    level       VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id     VARCHAR PRIMARY KEY,
    title       VARCHAR,
    artist_id   VARCHAR NOT NULL REFERENCES artists(artist_id),
    year        INT,
    duration    INT
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id   VARCHAR PRIMARY KEY,
    name        VARCHAR,
    location    VARCHAR,
    latitude   NUMERIC,
    longitude   NUMERIC
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS timetable (
    start_time BIGINT PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
)
""")

# STAGING TABLES
staging_events_copy2 = ("""
copy staging_events from 's3://udacity-dend/log_data'
credentials 'aws_iam_role={}'
gzip delimiter ';' compupdate off region 'us-west-2';
""").format(DWH_ROLE_ARN)

staging_songs_copy2 = ("""
copy staging_songs from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={}'
gzip delimiter ';' compupdate off region 'us-west-2';
""").format(DWH_ROLE_ARN)

staging_events_copy = ("""
COPY staging_events
FROM 's3://udacity-dend/log_data'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
IGNOREHEADER 1
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
COPY staging_events
FROM 's3://udacity-dend/song_data'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
IGNOREHEADER 1
""").format(DWH_ROLE_ARN)

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
FROM staging_events e
LEFT JOIN staging_songs s on e.song = s.title and e.artist = s.artist_name
WHERE e.page = 'NextSong'
AND e.userId IS NOT NULL
;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT userId, MAX(firstName), MAX(lastName), MAX(gender), MAX(level)
FROM staging_events
WHERE userId IS NOT NULL
GROUP BY userId
;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, MAX(title), MAX(artist_id), MAX(year), MAX(duration)
FROM staging_songs
GROUP BY song_id
;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, MAX(artist_name), MAX(artist_location), MAX(artist_latitude), MAX(artist_longitude)
FROM staging_songs
GROUP BY  artist_id
;
""")

time_table_insert = ("""
INSERT INTO timetable (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
--ON CONFLICT (start_time)
--DO NOTHING
;
""") 

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, time_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, time_table_insert, song_table_insert, songplay_table_insert]
