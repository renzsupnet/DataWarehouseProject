import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Copies data from s3 bucket into 2 tables: staging_events and staging_songs
    
    Parameters: 
    cur (cursor), con(connection)

    Results:
    Populated staging_events and staging_songs tables

    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inserts data into users, songplays, songs, artists, time from the staging tables

    Parameters: 
    cur (cursor), con(connection)

    Results:
    Populated users, songplays, songs, artists, and time tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Calls the loading and insert functions as well as opening redshift 
    connection using credentials from dwh.cfg

    Results:
    Populated staging_events, staging_songs, users, songplays, songs, artists, and time tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()