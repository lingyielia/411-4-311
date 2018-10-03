from setting import host, user, password, keyid, key
import boto3
import base64
import time
import json
from datetime import datetime, timedelta
import psycopg2

# set parameters for connecting with s3
client_s3 = boto3.client('s3')
client_r = boto3.client('redshift')
BUCKET = 'nyc311forinsight'

# set parameters for accessing database of redshift
host = host
database = 'db311'
user = user
password = password
port = '5439'


def lambda_handler(event, context):
    '''
    retrive the target records, format them into list of tuples,
    store the new result into redshift
    '''
    res = []
    KEY = '/newest_' + str(datetime.now().date() - timedelta(days=7)) + '.csv'
    put_data_to_redshift(host, database, user, password, port, res, KEY)
    return ''
    

def put_data_to_redshift(host, database, user, password, port, res, KEY):
    '''
    put data into database of redshift
    '''
    try:
        con = psycopg2.connect(host=host, database=database, user=user,
                               password=password, port=port)
    except Exception as err:
        print(err)
        
    with con.cursor() as cur:
        insert_query = ("truncate eventsNew;"
                        "copy eventsNew from 's3://nyc311forinsight" + str(KEY) + "' " +
                        "credentials 'aws_access_key_id=" + keyid +";aws_secret_access_key=" + key + "' " +
                        "csv " +
                        "timeformat 'YYYY-MM-DDTHH:MI:SS';")
        cur.execute(insert_query, res)
        con.commit()

    cur.close()
    con.close()