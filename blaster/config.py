'''
Created on 04-Nov-2017

@author: abhinav
'''
from os import environ

secrets = {
	"v0": "something_random_secret_here"
}

IS_DEBUG = True
if(environ.get('PROD') == "1"):
	IS_DEBUG = False

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
