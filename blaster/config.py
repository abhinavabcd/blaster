'''
Created on 04-Nov-2017

@author: abhinav
'''
from os import environ
import logging

secrets = {
	"v0": "something_random_secret_here"
}

DEBUG_LEVEL = int(environ.get("DEBUG_LEVEL") or 1) # higher implies more debug information

IS_PROD = environ.get("IS_PROD") == "1"
#staging is for production test instances
IS_STAGING = IS_PROD and environ.get("IS_STAGING") == "1"
#
IS_DEV = 1 if not (IS_PROD or IS_STAGING) else 0

# CRITICAL-50 ERROR-40  WARNING-30  INFO-20  DEBUG-10  NOTSET-0
LOG_LEVEL = logging.DEBUG if IS_DEV else (logging.INFO if IS_STAGING else logging.WARN)

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
