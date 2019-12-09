import psycopg2
import sql_queries
from importlib import reload
reload(sql_queries)
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Create and connect to the sparkify database.
    
    First the function connects to the default database and then creates the 
    sparkify database with UTF8 encoding. After that the connection to the 
    default database will be closed.
    
    Returns:
    cur: Cursor object in the created database
    conn: Connection object to the created database 
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drop all tables defined in sql_queries.py
    """
    for query in drop_table_queries:
#         print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all tables defined in sql_queries.py
    """
    for query in create_table_queries:
#         print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function to create the spotify database, drop the old tables and create 
    new tables defined in sql_queries.py.
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()