from setting import apikey
import json
from datetime import datetime, timedelta
from sodapy import Socrata
import boto3
import base64

# set parameters for connecting
client_s = Socrata('data.cityofnewyork.us', apikey)
client_s3 = boto3.client('s3')
bucket = boto3.resource('s3')
client_l = boto3.client('lambda')
BUCKET = 'logfordailycheck'
KEY = 'apistatus.txt'

# time range setting
day_start = (datetime.now().date() - timedelta(days=7))


def lambda_handler(event, context):
    '''
    check the availability of api. If available, activate the daily process.
    If unavailable, record the log into s3 and stop
    '''
    try:
        # test if the api works today
        time_rule = 'created_date > "' + str(day_start) + 'T00:00:12.000"'
        client_s.get('fhrw-4uyv', where=time_rule, limit=2)

        # check if the log file exist. If exist, means that the api is
        # unavailable before. If not exist, means the api works normal recently
        response_from_log = check_log_file(client_s3, BUCKET, KEY)
        need_to_collect = {}
        
        if response_from_log.get('Contents'):
            print('API available & log exists')
            
            # retrieve log records
            old_res = client_s3.get_object(Bucket=BUCKET, Key=KEY)
            log_rec = old_res['Body'].read().decode()
            log_list = log_rec.strip().split('\n')
            for i,line in enumerate(log_list):
                res_time = line.split()[1]
                need_to_collect[str(i)] = res_time
            
            # add timestamp of today's request
            need_to_collect[str(len(log_rec.split('\n')))] = str(day_start)
            data = {'custom': need_to_collect}
            
            # delete log history and invoke the next lambda function
            key_existing_del(client_s3, BUCKET, KEY)
            invoke_next_lam(client_l, data)
        
        else:
            print('API available & log does not exists')
            
            # add timestamp of today's request
            need_to_collect[str(0)] = str(day_start)
            data = {'custom': need_to_collect}
            
            # invoke the next lambda function
            invoke_next_lam(client_l, data)
    
    # when the api is not working
    except Exception as exception:
        print(exception)
        
        # check if the log file exist
        response_from_log = check_log_file(client_s3, BUCKET, KEY)
        
        if response_from_log.get('Contents'):
            print('API unavailable & log exists')
            
            # retrieve log records and append new record
            old_res = client_s3.get_object(Bucket=BUCKET, Key=KEY)
            log_rec = old_res['Body'].read().decode()
            body = log_rec + '\nunavailable ' + str(day_start)
        
        else:
            print('API unavailable & log does not exists')
            body = 'unavailable ' + str(day_start)

        client_s3.put_object(Body=body, Bucket=BUCKET, Key=KEY)
    return ''


def invoke_next_lam(client_l, data):
    '''
    invoke the lambda function that extract data from api to kinesis
    '''
    print(data)
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

    
def check_log_file(client_s3, BUCKET, KEY):
    '''
    check if the log file in s3 exsits
    '''
    response = client_s3.list_objects_v2(Bucket=BUCKET, Prefix=KEY)
    return response