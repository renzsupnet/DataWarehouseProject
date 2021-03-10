import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

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
                                    sessionId VARCHAR,
                                    song VARCHAR,
                                    status VARCHAR,
                                    ts TIMESTAMP,
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

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(
                                 songplay_id VARCHAR PRIMARY KEY, 
                                 start_time TIMESTAMP NOT NULL REFERENCES time(start_time), 
                                 user_id VARCHAR NOT NULL REFERENCES user(user_id), 
                                 level VARCHAR, 
                                 song_id VARCHAR, 
                                 artist_id INT, 
                                 session_id INT, 
                                 location VARCHAR, 
                                 user_agent VARCHAR
                                 );
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS user(
                            user_id VARCHAR PRIMARY KEY, 
                            first_name VARCHAR, 
                            last_name VARCHAR, 
                            gender CHAR, 
                            level VARCHAR
                            );
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXSITS song(
                            song_id VARCHAR PRIMARY KEY, 
                            title VARCHAR, 
                            artist_id VARCHAR, 
                            year INT, 
                            duration FLOAT
                            );
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist(
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

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
