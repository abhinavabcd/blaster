'''
Created on 19-Jan-2018

@author: abhinav
'''


import urllib3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth.aws4auth import AWS4Auth
from .config import aws_config, es_aws_host, es_http_host, IS_DEV
from .common_funcs_and_datastructures import cur_ms
urllib3.disable_warnings()

es_conn = None


if(es_http_host):
    es_conn = Elasticsearch([es_http_host])

        
elif(es_aws_host):
    es_awsauth = AWS4Auth(aws_config["aws_access_key_id"] , aws_config["aws_secret_access_key"] , aws_config["region_name"], 'es')
    es_conn = Elasticsearch(
                hosts=[{'host': es_aws_host, 'port': 443}],
                http_auth=es_awsauth,
                use_ssl=True,
                verify_certs=False,
                connection_class=RequestsHttpConnection
    )

#es_indexes_to_create = [{"index": name, "mapping": {}, "config": {}}...]
def create_indexes_and_mappings(es_indexes_to_create, recreate_indexes=False):
    #create items index
    #create communities index
    for index_to_create in es_indexes_to_create:
        index = index_to_create["index"]
        index_config = index_to_create.get("config")
        if(not index_config):
            index_config = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                    "index.requests.cache.enable": True
                }
            }

        index_config["mappings"] = {
            "properties" : index_to_create["mappings"]
        }

        if(recreate_indexes):
            if(not isinstance(recreate_indexes, list) or (index in recreate_indexes)):
                es_conn.indices.delete(index, ignore=[400, 404])


        ret = es_conn.indices.create(index=index, body=index_config, ignore=400)
        IS_DEV and print("es_index_create", cur_ms(), {"index": index, "status": ret})
