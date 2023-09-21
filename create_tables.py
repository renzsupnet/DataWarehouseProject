import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops all tables if they exist

    Parameters:
    cur(cursor), conn(connection)

    Results:
    Dropped staging_events, staging_songs, users, songplays, songs, artists, and time tables
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
     """Initializes/ creates all tables if they have yet to exist

    Parameters:
    cur(cursor), conn(connection)

    Results:
    Created staging_events, staging_songs, users, songplays, songs, artists, and time tables
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Executes other functions and accesses dwh.cfg to establish redshift connection

    Results:
    Creates/re-creates staging_events, staging_songs, users, songplays, songs, artists, and time tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()