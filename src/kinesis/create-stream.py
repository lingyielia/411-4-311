import boto3

client = boto3.client('kinesis')
response = client.create_stream(
   StreamName='data-collect2',
   ShardCount=1
)
