import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
ARN = config.get('IAM_ROLE','ARN')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist           VARCHAR,
        auth             VARCHAR,
        firstName        VARCHAR,
        gender           CHAR(1),
        itemInSession    INTEGER,
        lastName         VARCHAR,
        length           FLOAT,
        level            VARCHAR,
        location         VARCHAR,
        method           VARCHAR,
        page             VARCHAR,
        registration     VARCHAR,
        sessionID        INTEGER,
        song             VARCHAR,
        status           INTEGER,
        ts               TIMESTAMP,
        userAgent        VARCHAR,
        userid           INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs        INTEGER,
        artist_id        VARCHAR,
        artist_latitude  FLOAT,
        artist_longitude FLOAT,
        artist_location  VARCHAR,
        artist_name      VARCHAR,
        song_id          VARCHAR,
        title            VARCHAR,
        duration         FLOAT,
        year             INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id     INTEGER     IDENTITY(0,1)   PRIMARY KEY,
        start_time      TIMESTAMP   NOT NULL        DISTKEY SORTKEY,
        user_id         INTEGER     NOT NULL,
        level           VARCHAR,
        song_id         VARCHAR     NOT NULL,
        artist_id       VARCHAR     NOT NULL,
        session_id      INTEGER     NOT NULL,
        location        VARCHAR,
        user_agent      VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id         INTEGER     NOT NULL        SORTKEY PRIMARY KEY,
        first_name      VARCHAR     NOT NULL,
        last_name       VARCHAR     NOT NULL,
        gender          CHAR(1)     NOT NULL,
        level           VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id         VARCHAR     NOT NULL        SORTKEY PRIMARY KEY,
        title           VARCHAR     NOT NULL,
        artist_id       VARCHAR     NOT NULL,
        year            INTEGER     NOT NULL,
        duration        FLOAT      
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id       VARCHAR     NOT NULL        SORTKEY PRIMARY KEY,
        name            VARCHAR     NOT NULL,
        location        VARCHAR,
        latitude       FLOAT,
        longitude       FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time      TIMESTAMP   NOT NULL        SORTKEY PRIMARY KEY,
        hour            INTEGER     NOT NULL,
        day             INTEGER     NOT NULL,
        week            INTEGER     NOT NULL,
        month           INTEGER     NOT NULL,
        year            INTEGER     NOT NULL,
        weekday         INTEGER     NOT NULL
    );
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {} TIMEFORMAT AS 'epochmillisecs'
    REGION 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json 'auto' REGION 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT  DISTINCT(e.ts) AS start_time, 
                e.userId       AS user_id, 
                e.level        AS level, 
                s.song_id      AS song_id, 
                s.artist_id    AS artist_id, 
                e.sessionId    AS session_id, 
                e.location     AS location, 
                e.userAgent    AS user_agent
        FROM staging_events e JOIN staging_songs  s   
            ON (e.song = s.title AND e.artist = s.artist_name)
        WHERE e.page  =  'NextSong' 
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId) AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level 
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id) AS song_id,
        title, 
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id) AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time)                AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  as weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
