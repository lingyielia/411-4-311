import json
import boto3
import base64
import time
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

# set parameters for connecting with kinesis and s3
client_k = boto3.client('kinesis')
client_s3 = boto3.client('s3')
client_l = boto3.client('lambda')
stream = 'data-collect8' 
BUCKET = 'nyc311forinsight'


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
    res = []
    
    # loop through all shards to find data
    for shard in shard_iters:
        shard_it = shard['ShardIterator']
        try_count = 0
        while shard_it is not None:
            if try_count == 5:
                break
            try_count += 1
            try:
                out = client_k.get_records(ShardIterator=shard_it, Limit=10000)
            except ClientError as e:
                code = e.response['Error']['Code']
                if code != 'ProvisionedThroughputExceededException':
                    raise
                print ('Throughput exceeded!')
                time.sleep(0.2)
                break
     
            # check number of data been found    
            print(len(out['Records']))
            
            # filter and output records of 7 days ago
            one_ago = str(datetime.now().date() - timedelta(days=7))
            start = time.time()
            for record in out['Records']:
                temp = json.loads(record['Data'])
                cleaned = dict_clean(temp, change_ref)
                
                if cleaned['created_date'][:10] == one_ago:
                    res.append(cleaned)
            shard_it = out['NextShardIterator']
            delta = time.time() - start
            time.sleep(0.3 - delta)
    
    # put records of 7 days ago into s3
    print(len(res))
    final = '\n'.join([str(d['agency']) + ',' + str(d['closed_date']) + ',' +\
            str(d['complaint_type']) + ',' + str(d['created_date']) +\
            ',' + str(d['latitude']) + ',' + str(d['longitude']) + ',' +\
            str(d['open_data_channel_type']) for d in res])
    put_data_to_s3(client_s3, final, BUCKET)
    invoke_next_lam(client_l)


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
 
    
def put_data_to_s3(client_s3, final, BUCKET):
    '''
    put data into database
    '''
    KEY = 'records_' + str(datetime.now().date() - timedelta(days=7)) + '.csv'
    client_s3.put_object(Body=final, Bucket=BUCKET, Key=KEY)
    

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
    

def invoke_next_lam(client_l):
    '''
    invoke the lambda function that extract data kinesis and put to rds
    after cleaning
    '''
    client_l.invoke(FunctionName='lambda_s3_to_redshift', InvocationType='Event')
    return ''


        
        
