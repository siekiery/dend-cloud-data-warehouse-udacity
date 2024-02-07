import configparser
import psycopg2
import pandas as pd
import numpy as np


def load_staging_tables(cur, conn, copy_table_queries):
    """Loads song and log data from S3 into staging tables on Redshift cluster"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn, insert_table_queries):
    """Loads logs and songs data from staging tables and transforms them into five tables using insert_table_queries statements"""
    
    for query in insert_table_queries:
        
        # timetable is special case when values are transformed from timestamp and inserted by rows
        if "timetable" in query:
            
            # loads ts column into dataframe to easy manipulation
            sql = "SELECT ts FROM staging_events WHERE page='NextSong'"
            df = pd.read_sql_query(sql, conn)

            # convert timestamp column to datetime
            t = pd.to_datetime(df['ts'], unit='ms')

            # extract time data from timestamp and insert time data records
            time_data = np.transpose((df['ts'], t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday))
            column_labels = ('ts', 'hour', 'day', 'week', 'month',' year',' weekday')
            time_df = pd.DataFrame(time_data, columns=column_labels).astype(int).drop_duplicates('ts')
            
            # inserting values row by row
            for i, row in time_df.iterrows():
                cur.execute(query, list(row))
            
            conn.commit()
        
        # for other tables the values are inserted in bulk by selecting columns
        else:    
            cur.execute(query)
            conn.commit()


def main():
    """Connects to redshift cluster and runs ETL procedures to load logs and songs data into staging tables and transform them into the five tables"""
    from sql_queries import copy_table_queries, insert_table_queries
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn, copy_table_queries)
    insert_tables(cur, conn, insert_table_queries)

    conn.close()


if __name__ == "__main__":
    main()
                    