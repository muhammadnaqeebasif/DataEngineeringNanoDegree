class CreateTablesQueries:
    """Class which contains the queries for creating the tables

    Attributes
    ----------
    staging_events_table_create : str
        SQL statement for creating staging_events table
    staging_songs_table_create : str
        SQL statement for creating staging_songs table
    songplay_table_create : str
        SQL statement for creating songplays table
    user_table_create : str
        SQL statement for creating users table.
    song_table_create : str
        SQL statement for creating songs table.
    artist_table_create : str
        SQL statement for creating artists table.
    time_table_create : str
        SQL statement for creating times table.

    """
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
        ts               INT8,
        userAgent        VARCHAR,
        userid           INTEGER
    );
""")

    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs        INT4,
            artist_id        VARCHAR,
            artist_latitude  FLOAT,
            artist_longitude FLOAT,
            artist_location  VARCHAR,
            artist_name      VARCHAR,
            song_id          VARCHAR,
            title            VARCHAR,
            duration         FLOAT,
            year             INT4
        );
    """)

    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id     VARCHAR     PRIMARY KEY,
            start_time      TIMESTAMP,
            user_id         INT4,
            level           VARCHAR,
            song_id         VARCHAR,
            artist_id       VARCHAR,
            session_id      INT4,
            location        VARCHAR,
            user_agent      VARCHAR
        );
    """)

    user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users (
            user_id         INT4     NOT NULL PRIMARY KEY,
            first_name      VARCHAR,
            last_name       VARCHAR,
            gender          CHAR(1),
            level           VARCHAR
        );
    """)

    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs(
            song_id         VARCHAR     NOT NULL        PRIMARY KEY,
            title           VARCHAR ,
            artist_id       VARCHAR ,
            year            INTEGER,
            duration        FLOAT      
        );
    """)

    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists(
            artist_id       VARCHAR     NOT NULL        PRIMARY KEY,
            name            VARCHAR,
            location        VARCHAR,
            latitude        FLOAT,
            longitude       FLOAT
        );
    """)

    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time(
            start_time      TIMESTAMP   NOT NULL        PRIMARY KEY,
            hour            INTEGER     NOT NULL,
            day             INTEGER     NOT NULL,
            week            INTEGER     NOT NULL,
            month           INTEGER     NOT NULL,
            year            INTEGER     NOT NULL,
            weekday         INTEGER     NOT NULL
        );
    """)