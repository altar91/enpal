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

user=config['altar']['user_name']
password=config['altar']['password']
db_name=config['altar']['db_name']
host=config['altar']['host']
port=config['altar']['port']
arn=config['altar']['arn']

params = {'host': host,
          'database': db_name,
          'user': user,
          'password': password,
          'port': port}

def upload_to_s3(dataframe, bucket_name, file_name):
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer,index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket_name, file_name).put(Body=csv_buffer.getvalue())
    return True

def s3_to_RS(conn, table_name, path, columns_list):
    command = ("""
        copy """ + table_name + """ (""" + columns_list + """)
        from '""" + path + """'
        IAM_ROLE """ + arn + """
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

def truncate(conn, tablename):
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
        filename=['customer_orders.csv', 'teams.csv', 'bases.csv']
        table_name=["enpal.customer_orders", "enpal.teams", "enpal.bases"]
        bucket_name = "pipeline-redshift"

        for i in range(len(filename)):
            source_path=r'./data/' + filename[i]
            destination_path = 's3://' + bucket_name + '/' + filename[i]

            dataframe=pandas.read_csv(source_path)
            columns_list=dataframe.columns
            columns_list = ', '.join(columns_list)

            if dataframe.empty:
                print("There is no data")
            else:
                truncate(conn, table_name[i])
                upload_to_s3(dataframe, bucket_name, filename[i])
                s3_to_RS(conn, table_name[i], destination_path, columns_list)
                print("Upload to " + table_name[i] + " : Done")
    except:
        print("Upload to " + table_name[i] + " : FAILED")
    finally:
        if conn is not None:
            conn.close()
