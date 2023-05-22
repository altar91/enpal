import psycopg2
import pandas
import boto3
import os
import io
from io import StringIO
import sys
import warnings
from configparser import ConfigParser
from psycopg2 import extras

warnings.filterwarnings("ignore")
config = ConfigParser()
config.read('../config.ini')

USER = config['altar']['user_name']
PASSWORD = config['altar']['password']
DB_NAME = config['altar']['db_name']
HOST = config['altar']['host']
PORT = config['altar']['port']
ARN = config['altar']['arn']

FILE_NAME = ['customer_orders.csv', 'teams.csv', 'bases.csv']
TABLE_NAME = ["enpal.customer_orders", "enpal.teams", "enpal.bases"]
BUCKET_NAME = "pipeline-redshift"

params = {'host': HOST,
          'database': DB_NAME,
          'user': USER,
          'password': PASSWORD,
          'port': PORT}


def upload_to_s3(dataframe, bucket_name, file_name) -> bool:
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, file_name).put(Body=csv_buffer.getvalue())
    return True


def s3_to_RS(conn, table_name, path, columns_list) -> None:
    command = ("""
        copy """ + table_name + """ (""" + columns_list + """)
        from '""" + path + """'
        IAM_ROLE """ + ARN + """
        delimiter ','
        DATEFORMAT 'YYYY-MM-DD'
        csv
        IGNOREHEADER AS 1
        """)
    try:
        cur = conn.cursor()
        cur.execute(command)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cur.close()
        conn.commit()


def truncate(conn, tablename) -> None:
    commands = ("""
        TRUNCATE """ + tablename)
    try:
        cur = conn.cursor()
        cur.execute(commands)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cur.close()
        conn.commit()


if __name__ == '__main__':
    try:
        conn = psycopg2.connect(**params)
        s3 = boto3.client("s3")

        for i in range(len(FILE_NAME)):
            source_path = r'./data/' + FILE_NAME[i]
            destination_path = 's3://' + BUCKET_NAME + '/' + FILE_NAME[i]

            dataframe = pandas.read_csv(source_path)
            columns_list = dataframe.columns
            columns_list = ', '.join(columns_list)

            if dataframe.empty:
                print("There is no data")
            else:
                truncate(conn, TABLE_NAME[i])
                upload_to_s3(dataframe, BUCKET_NAME, FILE_NAME[i])
                s3_to_RS(conn, TABLE_NAME[i], destination_path, columns_list)
                print("Upload to " + TABLE_NAME[i] + " : Done")
    except Exception as e:
        print("Upload to " + TABLE_NAME[i] + " : FAILED, Error:" + str(e))
    finally:
        if conn is not None:
            conn.close()
