'''
Created on 04-Nov-2017

@author: abhinav
'''
from os import environ

secrets = {
	"v0": "something_random_secret_here"
}

DEBUG_LEVEL = int(environ.get("DEBUG_LEVEL") or 1)
IS_DEV = 0 if environ.get("IS_PROD") == "1" else 1
#staging is for production test instances
IS_STAGING = not IS_DEV and environ.get("IS_STAGING") == "1"


aws_config = {
	# 'aws_access_key_id': "A*****",
	# 'aws_secret_access_key': "G*****",
	# 'region_name': "ap-south-1"
}

es_http_host = None
es_aws_host = None

connection_generators = {}

sqs_url = None

server_error_page = None
