import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''Loads data into staging table'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''Insert data into tables'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''Configure connection to redshift cluster'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
#     try:
#         conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
#         cur = conn.cursor()
#         print('hi')
# #         return conn 
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
  
    
#     print(conn)
#     print(cur)
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()