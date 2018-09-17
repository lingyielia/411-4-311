import json
import urllib
import boto3
from datetime import datetime, timedelta
from sodapy import Socrata

print('Loading function')

client_k = boto3.client('kinesis', region_name='us-east-1')


def lambda_handler(event, context):
    client_s = Socrata('data.cityofnewyork.us', '')

    y_day = (datetime.now().date() - timedelta(days=2))
    t_day = datetime.now().date()
    time_rule = 'created_date > "' + str(y_day) + 'T00:00:12.000" and created_date < "' + str(t_day) + 'T00:00:12.000"'

    results = client_s.get('fhrw-4uyv', where=time_rule, limit=200)

    for rec in results:
        try:
            client_k.put_record(
                StreamName = 'data-collect2',
                Data = json.dumps(rec),
                PartitionKey = '122'
                )
        except Exception as err:
            print("err when put_record: {}".format(err))
