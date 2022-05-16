#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse

# Loading Dataset and sending it to Postgres

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    os.system(f"wget {url} -0 {csv_name}")

    # Creating engine for postgres
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Converting Parquet file into csv format
    df = pd.read_parquet("yellow_tripdata_2021-01.parquet")
    df.to_csv("ny_taxi_data.csv", index=False)

    # Loading first 100 rows of the csv-file into dataframe
    #df = pd.read_csv('ny_taxi_data.csv', nrows=100)

    # Creating a schema for SQL
    #print(pd.io.sql.get_schema(df, name='yello_taxi_data', con=engine))

    # Batching the csv-file, because it has more than 1,600,000 rows
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    # Converting string data into datetime format
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Converting the column names into sql format
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # filling the SQL table with chunkwise data from the df
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # Appending chunks of 100000 rows by looping through the interator
    while True:
        t_start = time()
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print('inserted another chunk..., took %.3f seconds' % (t_end - t_start))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # Parsing user, password, host, port, database, table name, url of csv
    parser.add_argument('user', help='user name for postgres')
    parser.add_argument('password', help='password for postgres')
    parser.add_argument('host', help='host for postgres')
    parser.add_argument('port', help='port for postgres')
    parser.add_argument('db', help='database name for postgres')
    parser.add_argument('table_name', help='name of the table where we will write the results to')
    parser.add_argument('url', help='url of the csv file')

    args = parser.parse_args()

    main(args)