# sql_queries.py
#
# PROGRAMMER: Brian Pederson
# DATE CREATED: 02/18/2020
# PURPOSE: Script encapsulating all SQL statements for DWH Project 2.
#
# Included functions: None
#

import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
DWH_ROLE_ARN = config.get('IAM_ROLE','ARN') 

# DROP TABLES

staging_events_table_drop =  "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop =   "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =        "DROP TABLE IF EXISTS songplays"
user_table_drop =            "DROP TABLE IF EXISTS users"
song_table_drop =            "DROP TABLE IF EXISTS songs"
artist_table_drop =          "DROP TABLE IF EXISTS artists"
time_table_drop =            "DROP TABLE IF EXISTS time"   

# COUNT TABLES

staging_events_table_count = "SELECT COUNT(*) FROM staging_events"
staging_songs_table_count =  "SELECT COUNT(*) FROM staging_songs"
songplay_table_count =       "SELECT COUNT(*) FROM songplays"
songplay_table_count2 =      "SELECT COUNT(*) FROM songplays x JOIN time t ON x.start_time = t.start_time"
user_table_count =           "SELECT COUNT(*) FROM users"
user_table_count2 =          "SELECT COUNT(*) FROM songplays x JOIN users u ON x.user_id = u.user_id"
song_table_count =           "SELECT COUNT(*) FROM songs"
song_table_count2 =          "SELECT count(*) FROM songplays x join songs s ON x.song_id = s.song_id"
artist_table_count =         "SELECT COUNT(*) FROM artists"
artist_table_count2 =        "SELECT COUNT(*) FROM songplays x JOIN artists a ON x.artist_id = a.artist_id"
time_table_count =           "SELECT COUNT(*) FROM time"
time_table_count2 =          "SELECT COUNT(*) FROM songplays x JOIN time t ON x.start_time = t.start_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR(256),
    auth VARCHAR(256),
    firstName VARCHAR(256),
    gender VARCHAR(256),
    itemInSession INT,
    lastName VARCHAR(256),
    length NUMERIC,
    level VARCHAR(256),
    location VARCHAR(256),
    method VARCHAR(256),
    page VARCHAR(256),
    registration VARCHAR(256),
    sessionId INT,
    song VARCHAR(256),
    status VARCHAR(256),
    ts BIGINT,
    userAgent VARCHAR(256),
    userId INT)  
DISTSTYLE EVEN
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INT,
    artist_id VARCHAR(256),
    artist_latitude NUMERIC(20,6),
    artist_longitude NUMERIC(20,6),
    artist_location VARCHAR(256),
    artist_name VARCHAR(256),
    song_id VARCHAR(256),
    title VARCHAR(256),
    duration NUMERIC(12,4),
    year INT) 
DISTSTYLE EVEN
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(  
    songplay_id BIGINT IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR(256),
    song_id VARCHAR(256) NOT NULL,
    artist_id VARCHAR(256) NOT NULL,
    session_id INT NOT NULL,
    location VARCHAR(256),
    user_agent VARCHAR(256),
    PRIMARY KEY (songplay_id),
    FOREIGN KEY (start_time) REFERENCES time(start_time),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (song_id) REFERENCES songs(song_id),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
)
DISTSTYLE EVEN
SORTKEY (start_time, user_id, song_id, artist_id)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INT PRIMARY KEY,
    first_name VARCHAR(256) NOT NULL,
    last_name VARCHAR(256) NOT NULL,
    gender VARCHAR(256) NOT NULL,
    level VARCHAR(256) NOT NULL)  
DISTSTYLE ALL
SORTKEY (user_id)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(256) PRIMARY KEY,
    title VARCHAR(256) NOT NULL,
    artist_id VARCHAR(256) NOT NULL,
    year INT,
    duration NUMERIC(12,4),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
)
DISTSTYLE ALL
SORTKEY (song_id)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(256) PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    location VARCHAR(256),
    latitude NUMERIC(20,6),
    longitude NUMERIC(20,6)) 
DISTSTYLE ALL
SORTKEY (artist_id)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL) 
DISTSTYLE ALL
SORTKEY (start_time)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE '{}'
REGION 'us-west-2'
JSON {};
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE '{}'
REGION 'us-west-2'
JSON 'auto';
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 Second' AS start_time, 
       e.userid as user_id, 
       e.level,
       COALESCE(s.song_id, '***UNKNOWN_SONG***') as song_id,  
       COALESCE(s.artist_id, '***UNKNOWN_ARTIST***') as artist_id,
       e.sessionid as session_id,
       e.location, 
       e.useragent as user_agent
  FROM staging_events e 
  LEFT OUTER JOIN staging_songs s ON e.artist = s.artist_name AND e.song = s.title
 WHERE page = 'NextSong' 
   AND (start_time, user_id, session_id) NOT IN (SELECT start_time, user_id, session_id FROM songplays)
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) 
SELECT userid, MIN(firstname), MIN(lastname), MIN(gender), MIN(level) 
  FROM staging_events
 WHERE page = 'NextSong'
   AND userid NOT IN (SELECT user_id FROM users) -- no dups
 GROUP BY userid;
""")

user_table_update = ("""
UPDATE users 
   SET level = s.level,
       first_name = s.firstname,
       last_name = s.lastname,
       gender = s.gender
  FROM staging_events s
 WHERE user_id = s.userid;   -- update existing rows with more recent info
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT song_id, MIN(title), MIN(artist_id), MIN(year), MIN(duration) 
  FROM staging_songs
 WHERE song_id NOT IN (SELECT song_id FROM songs) -- no dups; retain original
 GROUP BY song_id;
""")

# augment song table with dummy row representing unknown songs
song_table_insert2 = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT '***UNKNOWN_SONG***', '***Unknown Song***', '***UNKNOWN_ARTIST***', NULL, NULL
 WHERE '***UNKNOWN_SONG***' NOT IN (SELECT song_id FROM songs);
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude) 
SELECT artist_id, MIN(artist_name), MIN(artist_location), MIN(artist_latitude), MIN(artist_longitude) 
  FROM staging_songs
 WHERE artist_id NOT IN (SELECT artist_id FROM artists) -- no dups; retain original
 GROUP BY artist_id;
""")

# augment artist table with dummy row representing unknown artists
artist_table_insert2 = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT '***UNKNOWN_ARTIST***', '*** Unknown Artist ***', NULL, NULL, NULL
 WHERE '***UNKNOWN_ARTIST***' NOT IN (SELECT artist_id FROM artists);
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
SELECT start_time, 
       EXTRACT(hour from start_time),  
       EXTRACT(day from start_time),  
       EXTRACT(week from start_time),
       EXTRACT(month from start_time), 
       EXTRACT(year from start_time), 
       EXTRACT(dayofweek from start_time)
  FROM (
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second' AS start_time 
  FROM staging_events 
 WHERE page = 'NextSong'
   AND start_time NOT IN (SELECT start_time FROM time) -- no dups; retain original
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, 
                        time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create]

drop_table_queries =   [staging_events_table_drop, staging_songs_table_drop, 
                        songplay_table_drop, song_table_drop, artist_table_drop, user_table_drop, time_table_drop]

copy_table_queries =   [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, user_table_update, time_table_insert,
                        artist_table_insert, artist_table_insert2, song_table_insert, song_table_insert2,
                        songplay_table_insert]

count_table_queries =  [staging_events_table_count, staging_songs_table_count, 
                        songplay_table_count, user_table_count, song_table_count, artist_table_count, time_table_count] 
