import json
import boto3
import base64
import psycopg2
import time
from datetime import datetime, timedelta

# set parameters for reading from kinesis
client_k = boto3.client('kinesis')
stream = 'data-collect8'

# set parameters for accessing database
host = ''
database = 'db311'
user = ''
password = ''

# rules for filling missing values
change_ref = {'agency':'unknown', 'closed_date':'2050-01-10T04:08:32.000',
              'complaint_type':'unknown',
              'created_date':'2000-09-14T04:08:32.000',
              'latitude':'20.86125849849244',
              'longitude':'-23.92566793186856',
              'open_data_channel_type':'unknown'}


def lambda_handler(event, context):
    '''
    read events records from kinesis, extract and store then into the database
    '''
    shard_iters = get_kinesis_shards(stream)
    for i, shard in enumerate(shard_iters):
            shard_it = shard['ShardIterator']
            out = client_k.get_records(ShardIterator=shard_it, Limit=10000)
            res = []
            for record in out['Records']:
                temp = json.loads(record['Data'])
                cleaned = dict_clean(temp, change_ref)

                # filter and keep records of 7 days ago
                seven_ago = str(datetime.now().date() - timedelta(days=7))
                if cleaned['created_date'][:10] == seven_ago:
                    res.append(cleaned)

            final = [(d['agency'], d['closed_date'], d['complaint_type'], d['created_date'],
                     d['latitude'], d['longitude'], d['open_data_channel_type']) for d in res]
            put_data_to_rds(host, database, user, password, final)


def dict_clean(temp, change_ref):
    '''
    clean each record and fill missing values
    '''
    key_keep = ['agency', 'closed_date', 'complaint_type', 'created_date',
                'latitude', 'longitude', 'open_data_channel_type']

    default = ''
    dict_keep = {k: temp[k] if k in temp else default for k in key_keep}

    for key, value in dict_keep.items():
        if len(value) == 0:
            dict_keep[key] = change_ref[key]
    return dict_keep


def put_data_to_rds(host, database, user, password, final):
    '''
    put data into database
    '''
    con = psycopg2.connect(host=host, database=database, user=user, password=password)
    with con.cursor() as cur:
        records_list_template = ','.join(['%s'] * len(final))
        insert_query = 'insert into historical_for_test values {0}'.format(records_list_template)
        cur.execute(insert_query, final)
        con.commit()
    cur.close()
    con.close()


def get_kinesis_shards(stream):
    '''
    Return list of all shard iterators, one for each shard of stream
    '''
    descriptor = client_k.describe_stream(StreamName=stream)
    shards = descriptor['StreamDescription']['Shards']
    shard_ids = [shard[u"ShardId"] for shard in shards]
    shard_iters = [client_k.get_shard_iterator(
                        StreamName=stream,
                        ShardId=shard_id,
                        ShardIteratorType='AT_TIMESTAMP',
                        Timestamp=(datetime.now() - timedelta(minutes=5)))
                   for shard_id in shard_ids]
    return shard_iters
