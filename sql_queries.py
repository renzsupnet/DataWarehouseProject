import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
"""
References config file (dwh.cfg) and parses required information
"""
# DROP TABLES
"""
Drops table in order to reset data, useful for troubleshooting.
"""
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
"""
Initializes the structure of all tables
"""
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                    num_events INTEGER IDENTITY(0,1),
                                    artist VARCHAR,
                                    auth VARCHAR,
                                    firstName VARCHAR,
                                    gender VARCHAR,
                                    itemInSession VARCHAR,
                                    lastName VARCHAR,
                                    length VARCHAR,
                                    level VARCHAR,
                                    location VARCHAR,
                                    method VARCHAR,
                                    page VARCHAR,
                                    registration VARCHAR,
                                    sessionId INT,
                                    song VARCHAR,
                                    status VARCHAR,
                                    ts BIGINT,
                                    userAgent VARCHAR,
                                    userId VARCHAR
                                    );
                            """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                    artist_id VARCHAR, 
                                    artist_latitude FLOAT, 
                                    artist_location VARCHAR, 
                                    artist_longitude FLOAT, 
                                    artist_name VARCHAR, 
                                    duration FLOAT, 
                                    num_songs INT, 
                                    song_id VARCHAR, 
                                    title VARCHAR, 
                                    year INT
                                    ); 
                            """)



user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                            user_id VARCHAR PRIMARY KEY, 
                            first_name VARCHAR, 
                            last_name VARCHAR, 
                            gender CHAR, 
                            level VARCHAR
                            );
                    """)



artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                            artist_id VARCHAR PRIMARY KEY, 
                            name VARCHAR, 
                            location VARCHAR, 
                            latitude FLOAT, 
                            longitude FLOAT
                            );
                      """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                            start_time TIMESTAMP PRIMARY KEY, 
                            hour INT, 
                            day INT, 
                            week INT, 
                            month INT, 
                            year INT, 
                            weekday INT
                            );
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                            song_id VARCHAR PRIMARY KEY, 
                            title VARCHAR, 
                            artist_id VARCHAR NOT NULL REFERENCES artists(artist_id), 
                            year INT, 
                            duration FLOAT
                            );
                    """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                                 songplay_id VARCHAR PRIMARY KEY, 
                                 start_time BIGINT NOT NULL REFERENCES time(start_time), 
                                 user_id VARCHAR NOT NULL REFERENCES users(user_id), 
                                 level VARCHAR, 
                                 song_id VARCHAR NOT NULL REFERENCES songs(song_id), 
                                 artist_id VARCHAR NOT NULL REFERENCES artists(artist_id), 
                                 session_id INT, 
                                 location VARCHAR, 
                                 user_agent VARCHAR
                                 );
                        """)
# STAGING TABLES
"""
References config information: S3, LOG_DATA, IAM_ROLE, etc. in order to execute command with proper credentials.
"""
staging_events_copy = ("""COPY staging_events FROM {}
                                CREDENTIALS 'aws_iam_role={}'
                                COMPUPDATE OFF region 'us-east-1'
                                TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                                TIMEFORMAT 'epochmillisecs'
                                FORMAT AS JSON {};
                        """).format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'),config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs FROM {}
                                CREDENTIALS 'aws_iam_role={}'
                                COMPUPDATE OFF region 'us-east-1'
                                TIMEFORMAT 'epochmillisecs'
                                TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                                JSON 'auto'
                        """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))


# FINAL TABLES
"""
Inserts data from staging_events and staging_songs into their appropriate tables and columns
"""
songplay_table_insert =("""INSERT INTO songplays(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                                SELECT DISTINCT md5(se.sessionId || se.ts) songplay_id, se.ts AS start_time, 
                                se.userId AS user_id, se.level, ss.song_id, ss.artist_id, se.sessionId AS session_id, se.location,
                                se.userAgent AS user_agent
                                FROM 
                                staging_events se
                                JOIN staging_songs ss ON se.artist = ss.artist_name
                                AND se.song = ss.title
                                WHERE se.page = 'NextSong';""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                            SELECT DISTINCT se.userId, se.firstName, se.lastName, se.gender, se.level
                            FROM 
                            staging_events se
                            WHERE se.page = 'NextSong' AND
                            se.userId NOT IN (SELECT DISTINCT user_id FROM users);""")

song_table_insert =("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                            SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
                            FROM 
                            staging_songs ss;""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                              SELECT DISTINCT ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
                              FROM
                              staging_songs ss;""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
                                SELECT DISTINCT start_time,
                                EXTRACT(HOUR FROM start_time) AS hour,
                                EXTRACT(DAY FROM start_time) AS day,
                                EXTRACT(WEEK FROM start_time) AS week,
                                EXTRACT(MONTH FROM start_time) AS month,
                                EXTRACT(YEAR FROM start_time) AS year,
                                EXTRACT(DOW FROM start_time) AS weekday
                                FROM (SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' AS start_time
                                FROM staging_events)""")



# QUERY LISTS
"""
References commands to be executed
"""
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, time_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]