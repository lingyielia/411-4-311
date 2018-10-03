from setting import apikey
import json
import urllib
import boto3
import time
from datetime import datetime, timedelta
from sodapy import Socrata

# set parameters for connecting
client_k = boto3.client('kinesis', region_name='us-east-1')
client_s = Socrata('data.cityofnewyork.us', apikey)
client_l = boto3.client('lambda')


def lambda_handler(event, context):
    '''
    retrieve data from api and put into kinesis stream
    '''
    my_custom_dict = event['custom']
    earliest_date = my_custom_dict['0']
    day_start = datetime.strptime(earliest_date, '%Y-%m-%d').date()
    retrive_record(client_s, client_k, day_start)
    
    invoke_next_lam(client_l)
    return ''


def retrive_record(client_s, client_k, day_start):
    '''
    retrieve records based on the start time
    '''
    time_rule = 'created_date > "' + str(day_start) + 'T00:00:12.000"'
    results = client_s.get('fhrw-4uyv', where=time_rule, limit=800000)
    print(len(results))

    # put data to kinesis stream
    count = 0
    prepared = []
    for i,rec in enumerate(results):
        partitionkey = 'nyc311' + str(i*7 + 31)
        one_rec = fmt(partitionkey, rec)
        prepared.append(one_rec)
        count += 1
        if count == 500:
            flush(prepared)
            count = 0
            prepared = []
    return ''

    
def fmt(partitionkey, record):
    '''
    format record that will be aggregated and used in put_records
    '''
    return {'PartitionKey':partitionkey, 'Data':bytes(json.dumps(record), 'utf-8')}


def flush(prepared):
    '''
    put a list of records into kinesis using put_records
    '''
    try:
        client_k.put_records(StreamName='data-collect8', Records=prepared)
    except Exception as err:
        print("err when put_record: {}".format(err))
        

def invoke_next_lam(client_l):
    '''
    invoke the lambda function that extract data kinesis and put to rds after cleaning
    '''
    client_l.invoke(FunctionName='lambda_kin_to_s3_newest', InvocationType='Event')
    client_l.invoke(FunctionName='lambda_kin_to_s3', InvocationType='Event')
    return ''