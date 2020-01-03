import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data to staging tables.
    
    The function calls all queries from copy_table_queries to copy the data 
    from S3 to AWS Redshift.
    
    Parameters: 
    cur: Cursor object of a database
    conn: Connection object to a database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data to analytics tables.
    
    The function calls all queries from insert_table_queries to insert the data 
    from the staging tables into the analytics tables.
    
    Parameters: 
    cur: Cursor object of a database
    conn: Connection object to a database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function to call all functions for the etl process.
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