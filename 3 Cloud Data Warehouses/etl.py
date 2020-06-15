import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data files from S3 into Redshift staging tables.
        
    Args:
        cur: Redshift cursor instance.
        conn: Redshift connection instance.
    
    Returns:
        Nothing.
    
    Raises:
        Any errors raised by psycopg2.
    """
    
    try:
        for query in copy_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def insert_tables(cur, conn):
    """
    Transforms and loads data from staging tables into fact and dim tables.
        
    Args:
        cur: Redshift cursor instance.
        conn: Redshift connection instance.
    
    Returns:
        Nothing.
    
    Raises:
        Any errors raised by psycopg2.
    """
    
    try:
        for query in insert_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def main():
    """
    Parses the dwh.cfg configuration file to retrieve connection parameters.
    Establishes a connection to Redshift.
    Calls function to load data files from S3 into Redshift staging tables.
    Calls function to transform and load data from staging tables into fact and dim tables.
        
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
        
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
    
        conn.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()