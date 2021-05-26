import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Method loads data into staging tables used for intermediate data storage
    :param cur: cursor
    :param conn: connection
    :return:
    """
    for query in copy_table_queries:
        print("Staging tables query:", query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Method inserts data into dimension and fact tables
    :param cur: cursor
    :param conn: connection
    :return:
    """
    for query in insert_table_queries:
        print("Insert tables query:", query)
        cur.execute(query)
        conn.commit()


def main():
    """"
    ETL pipeline for processing data and storing in final dimension and fact tables
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

