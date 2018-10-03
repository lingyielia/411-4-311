from setting import host, user, password
import boto3
import base64
import time
import json
from datetime import datetime, timedelta
from smart_open import smart_open
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
    custom_dict = event['custom']
    print(custom_dict.values())
    for i in custom_dict.values():
        res = []
        KEY = '/records_' + str(i) + '.csv'
        with smart_open('s3://' + BUCKET + KEY, 'rb') as fin:
            for line in fin:
                temp = line.decode('utf8').split(',')
                formatted = tuple(temp)
                res.append(formatted)
        print(len(res))
    
        put_data_to_redshift(host, database, user, password, port, res)
    return ''
    

def put_data_to_redshift(host, database, user, password, port, res):
    '''
    put data into database of redshift
    '''
    try:
        con = psycopg2.connect(host=host, database=database, user=user,
                               password=password, port=port)
    except Exception as err:
        print(err)
        
    with con.cursor() as cur:
        records_list_template = ','.join(['%s'] * len(res))
        insert_query = 'insert into events values {0}'.format(records_list_template)
        cur.execute(insert_query, res)
        con.commit()

    cur.close()
    con.close()