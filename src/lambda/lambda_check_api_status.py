import json
from datetime import datetime, timedelta
from sodapy import Socrata
import boto3
import base64

client_s = Socrata('data.cityofnewyork.us', '')
client_s3 = boto3.client('s3')
bucket = boto3.resource('s3')
client_l = boto3.client('lambda')
BUCKET = 'checknycapi'
KEY = 'apistatus.txt'

def lambda_handler(event, context):
    '''
    check the availability of api. If available, activate the daily process.
    If unavailable, record the log into s3 and stop
    '''
    y_day = (datetime.now().date() - timedelta(days=7))
    t_day = (datetime.now().date() - timedelta(days=1))
    time_rule = 'created_date > "' + str(y_day) + 'T00:00:12.000" and created_date < "' +\
                str(t_day) + 'T00:00:12.000"'

    try:
        # test if the api works today
        client_s.get('fhrw-4uyv', where=time_rule, limit=2)

        ### for test api error ###
        # results = client_s.get(where=time_rule, limit=2)

        # check if the temp log exist. If exist, means that the api is
        # unavailable before. If not exist, means the api works normal recently.
        response = client_s3.list_objects_v2(Bucket=BUCKET, Prefix=KEY)
        down_time = {}
        if response.get('Contents'):
            print('Available & File exists')
            old_res = client_s3.get_object(Bucket=BUCKET, Key=KEY)
            text = old_res['Body'].read().decode()
            for i,line in enumerate(text.split('\n')):
                res_time = line.split()[1]
                down_time[str(i)] = res_time
            # add time of today's request
            down_time[str(len(text.split('\n')))] = str(datetime.now().date() - timedelta(days=7))
            data = {'custom': down_time}
            print(data)
            key_existing_del(client_s3, BUCKET, KEY)
            invoke_next_lam(client_l, data)
        else:
            print('Available & File does not exists')
            down_time[str(0)] = str(datetime.now().date() - timedelta(days=7))
            data = {'custom': down_time}
            invoke_next_lam(client_l, data)


    except Exception as exception:
        print(exception)
        response = client_s3.list_objects_v2(Bucket=BUCKET, Prefix=KEY)
        if response.get('Contents'):
            print('Unavailable & File exists')
            old_res = client_s3.get_object(Bucket=BUCKET, Key=KEY)
            text = old_res['Body'].read().decode()
            body = text + '\nunavailable ' + str(y_day) + ' to ' + str(t_day)
        else:
            print('Unavailable & File does not exists')
            body = 'unavailable ' + str(y_day) + ' to ' + str(t_day)

        client_s3.put_object(Body=body, Bucket=BUCKET, Key=KEY)
    return ''


def invoke_next_lam(client_l, data):
    '''
    invoke the lambda function that extract data from api to kinesis
    '''
    client_l.invoke(FunctionName='lambda_api_to_kin',
                    InvocationType='Event',
                    Payload=json.dumps(data))
    return ''


def key_existing_del(client_s3, BUCKET, KEY):
    '''
    delete key if it exist, else None
    '''
    response = client_s3.list_objects_v2(Bucket=BUCKET, Prefix=KEY)
    for obj in response.get('Contents', []):
        if obj['Key'] == KEY:
            file = bucket.Object(BUCKET, KEY)
            file.delete()
    return ''
