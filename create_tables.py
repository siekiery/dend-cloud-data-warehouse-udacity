import configparser
import psycopg2


def drop_tables(cur, conn, drop_table_queries):
    """Drops all tables if they exists. This ensures clean start"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn, create_table_queries):
    """ Creates tables as specified in sql statements of create_table_queries"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connects to Redshift cluster and runs procedures for dropping and creating tables. Creates songs and logs staging tables and five fact and dimensional tables """
    from sql_queries import create_table_queries, drop_table_queries
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn, drop_table_queries)
    create_tables(cur, conn, create_table_queries)

    conn.close()


if __name__ == "__main__":
    main()