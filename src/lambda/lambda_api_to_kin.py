import json
import urllib
import boto3
from datetime import datetime, timedelta
from sodapy import Socrata

print('Loading function')
client_k = boto3.client('kinesis', region_name='us-east-1')
client_s = Socrata('data.cityofnewyork.us', '')


def lambda_handler(event, context):
    '''
    retrieve data from api and put into kinesis stream
    '''

    # retrieve records that were created 7 days ago
    y_day = (datetime.now().date() - timedelta(days=7))
    t_day = (datetime.now().date() - timedelta(days=6))
    time_rule = 'created_date > "' + str(y_day) + 'T00:00:12.000" and created_date < "' + str(t_day) + 'T00:00:12.000"'

    results = client_s.get('fhrw-4uyv', where=time_rule, limit=2)

    # put data to kinesis stream
    for rec in results:
        try:
            client_k.put_record(
                StreamName = 'data-collect7',
                Data = json.dumps(rec),
                PartitionKey = '123'
                )
        except Exception as err:
            print("err when put_record: {}".format(err))
