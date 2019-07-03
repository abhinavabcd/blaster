'''
Created on 19-Jan-2018

@author: abhinav
'''


from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth.aws4auth import AWS4Auth
from .config import aws_config, es_aws_host, es_http_host
import urllib3
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


def create_indexes_and_mappings(index_name, index_config=None, recreate_index=False, mappings = None):
    if recreate_index:
        es_conn.indices.delete(index_name, ignore=[400, 404])
        
        
    index_config = index_config or {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 1,
            "index.requests.cache.enable": False
        }
    }
    
    if(mappings):
        index_config["mappings"] = mappings
    
    return es_conn.indices.create(index=index_name, body=index_config, ignore=400)
