import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops staging and fact and dim tables if they exist in Redshift.
        
    Args:
        cur: Redshift cursor instance.
        conn: Redshift connection instance.
    
    Returns:
        Nothing.
    
    Raises:
        Any errors raised by psycopg2.
    """
    
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def create_tables(cur, conn):
    """
    Creates staging and fact and dim tables in Redshift.
        
    Args:
        cur: Redshift cursor instance.
        conn: Redshift connection instance.
    
    Returns:
        Nothing.
    
    Raises:
        Any errors raised by psycopg2.
    """
    
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def main():
    """
    Parses the dwh.cfg configuration file to retrieve connection parameters.
    Establishes a connection to Redshift.
    Calls function to drop staging and fact and dim tables if they exist.
    Calls function to create the fact and dim tables.
        
    Args:
        None.
    
    Returns:
        Nothing.
    
    Raises:
        Any errors raised by configparser or psycopg2.
    """
    
    try:
        config = configparser.ConfigParser()
        config.read('dwh.cfg')
        
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        
        drop_tables(cur, conn)
        create_tables(cur, conn)
        
        conn.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()