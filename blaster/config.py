'''
Created on 04-Nov-2017

@author: abhinav
'''
from os import environ



secrets = {"v0": "something_random_secret_here"}

IS_DEBUG = True
if(environ.get('PROD')=="1"):
    IS_DEBUG = False



aws_config = {
    'aws_access_key_id': "A*****",
    'aws_secret_access_key': "G*****",
    'region_name': "ap-south-1"
}


ES_HTTP_HOST =  "http://SOMEIP_ADDRESESORHOST_NAME:9200" if not IS_DEBUG else "http://localhost:9200"
ES_AWS_HOST = None

SQS_URL = "https://sqs.ap-south-1.amazonaws.com/***/SOMENAME" if not IS_DEBUG else None
