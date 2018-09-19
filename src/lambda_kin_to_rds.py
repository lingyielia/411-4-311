import json
import boto3
import base64
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime, timedelta

# set parameters for reading from kinesis
client_k = boto3.client('kinesis')
shard_id = 'shardId-000000000000'
pre_shard_it = client_k.get_shard_iterator(StreamName='data-collect7',
                     ShardId=shard_id,
                     ShardIteratorType='AT_TIMESTAMP',
                     Timestamp=(datetime.now() - timedelta(minutes=5)))
shard_it = pre_shard_it['ShardIterator']

# set parameters for accessing database
host = ''
database = ''
user = ''
password = ''


def lambda_handler(event, context):
    '''
    read events records from kinesis, extract and store then into the database
    '''
    out = client_k.get_records(ShardIterator=shard_it, Limit=10000)
    res = []
    for record in out['Records']:
        temp = json.loads(record['Data'])
        res.append(temp)

    # keep useful columns and fill null values
    df = pd.DataFrame(res)
    df = df[['agency', 'closed_date', 'complaint_type',
             'created_date', 'latitude', 'longitude', 'open_data_channel_type']]
    fill_rull = {'agency':'unknown', 'closed_date':'2050-01-10T04:08:32.000',
                 'complaint_type': 'unknown',
                 'created_date':'2000-09-14T04:08:32.000',
                 'latitude':'20.86125849849244',
                 'longitude':'-23.92566793186856',
                 'open_data_channel_type':'unknown'}
    df.fillna(value=fill_rull, inplace=True)
    args = df.transform(tuple, axis=1).tolist()

    # put data into database
    con = psycopg2.connect(host=host, database=database, user=user, password=password)
    with con.cursor() as cur:
        records_list_template = ','.join(['%s'] * len(args))
        insert_query = 'insert into events values {0}'.format(records_list_template)
        cur.execute(insert_query, args)
        con.commit()

    cur.close()
    con.close()
