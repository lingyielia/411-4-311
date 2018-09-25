import json
import urllib
import boto3
import time
from datetime import datetime, timedelta
from sodapy import Socrata

client_k = boto3.client('kinesis', region_name='us-east-1')
client_s = Socrata('data.cityofnewyork.us', '')


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
        # time.sleep(10)
    return ''


def retrive_record(client_s, client_k, start_time):
    '''
    retrieve records of 7 days based on the start time
    '''
    y_day = start_time
    t_day = (start_time + timedelta(days=7))
    time_rule = 'created_date > "' + str(y_day) + 'T00:00:12.000" and created_date < "' +\
                str(t_day) + 'T00:00:12.000"'

    results = client_s.get('fhrw-4uyv', where=time_rule, limit=600000)
    # time.sleep(10)
    print(len(results))

    # put data to kinesis stream
    for rec in results:
        try:
            client_k.put_record(StreamName = 'collectRecords',
                                Data = json.dumps(rec),
                                PartitionKey = 'nyc311')
        except Exception as err:
            print("err when put_record: {}".format(err))
    return ''
