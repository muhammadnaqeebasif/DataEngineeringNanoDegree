import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Loads data into the staging tables

    Parameters
    ----------
    cur : psycopg2.extensions.cursor
        Cursor to the PostgreSQL database
    filepath : str
        The path of the song file

    Returns
    -------
    None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Performs ETL from staging tables to the required tables

        Parameters
        ----------
        cur : psycopg2.extensions.cursor
            Cursor to the PostgreSQL database
        filepath : str
            The path of the song file

        Returns
        -------
        None
        """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Main function to call

            Returns
            -------
            None
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