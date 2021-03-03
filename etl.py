import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Loading data from files stored in S3 to staging tables
def load_staging_tables(cur, conn):
    print('Inserting data from S3 to staging tables')
    for query in copy_table_queries:
        print('Running ' + query)
        cur.execute(query)
        conn.commit()

# Transforming data from staging tables into the dimensional tables
def insert_tables(cur, conn):
    print('Inserting data from staging tables')
    for query in insert_table_queries:
        print('Running ' + query)
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()