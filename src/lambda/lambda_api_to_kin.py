import json
import urllib
import boto3
import time
from datetime import datetime, timedelta
from sodapy import Socrata

client_k = boto3.client('kinesis', region_name='us-east-1')
client_s = Socrata('data.cityofnewyork.us', '')
client_l = boto3.client('lambda')


def lambda_handler(event, context):
    '''
    retrieve data from api and put into kinesis stream
    '''
    my_custom_dict = event['custom']
    print(my_custom_dict)

    for i in my_custom_dict.values():
        dt_format = "%Y-%m-%d"
        start_time = datetime.strptime(i, dt_format).date()
        print(start_time)
        retrive_record(client_s, client_k, start_time)
        time.sleep(8)
    invoke_next_lam(client_l)
    return ''


def retrive_record(client_s, client_k, start_time):
    '''
    retrieve records of 7 days based on the start time
    '''
    y_day = start_time
    t_day = (start_time + timedelta(days=7))
    time_rule = 'created_date > "' + str(y_day) + 'T00:00:12.000" and created_date < "' +\
                str(t_day) + 'T00:00:12.000"'

    results = client_s.get('fhrw-4uyv', where=time_rule, limit=80000)
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
<<<<<<< HEAD
        client_k.put_records(StreamName='data-collect8', Records=prepared)
=======
        client_k.put_records(StreamName='data-collect7', Records=prepared)
>>>>>>> 061725ba8f041f03eb66259be6b8f6c47de1bfe2
    except Exception as err:
        print("err when put_record: {}".format(err))


def invoke_next_lam(client_l):
    '''
    invoke the lambda function that extract data kinesis and put to rds after cleaning
    '''
    client_l.invoke(FunctionName='lambda_kin_to_rds',
                    InvocationType='Event')
    return ''
