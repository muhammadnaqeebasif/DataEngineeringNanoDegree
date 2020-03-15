class SqlQueries:
    """Class which contains the queries for extracting the data from staging tables

    Attributes
    ----------
    songplay_table_insert : str
        SQL statement for extracting the data for songplays table.
    user_table_insert : str
        SQL statement for extracting the data for users table.
    song_table_insert : str
        SQL statement for extracting the data for songs table.
    artist_table_insert : str
        SQL statement for extracting the data for artists table.
    time_table_insert : str
        SQL statement for extracting the data for time table.
    """
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid user_id, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid session_id, 
                events.location, 
                events.useragent user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT 
            distinct userid AS user_id,
            firstname AS first_name,
            lastname AS last_name,
            gender,
            level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT 
            distinct song_id,
            title,
            artist_id,
            year, 
            duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT 
            distinct artist_id, 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT 
            start_time, 
            extract(hour from start_time), 
            extract(day from start_time), 
            extract(week from start_time), 
            extract(month from start_time), 
            extract(year from start_time), 
            extract(dayofweek from start_time)
        FROM songplays
    """)