from gevent.queue import Queue, Empty
import boto3

from . import config
from .config import aws_config, IS_DEBUG
#import umysql

conn_pools = {}
boto_session = None
if(aws_config):
    boto_session = boto3.session.Session(**aws_config)

def get_dynamodb_conn():
    if(IS_DEBUG):
        return boto3.resource('dynamodb', endpoint_url='http://{endpoint}:{port}'.format(endpoint="127.0.0.1", port="8000"), aws_access_key_id=" ", aws_secret_access_key=" ", region_name="ap-south-1")
    else:
        return boto_session.resource('dynamodb')

def get_s3_conn():
    return boto_session.resource('s3')

def get_sqs_conn():
    return boto_session.client('sqs')

def get_ses_conn():
    return boto_session.client('ses', region_name="eu-west-1")


connection_generators = {
    "dynamodb": get_dynamodb_conn,
    "s3" : get_s3_conn,
    "sqs": get_sqs_conn,
    "ses": get_ses_conn
}

connection_generators.update(**config.connection_generators)



def get_from_pool(pool_id):
    conn = None

    conn_pool = conn_pools.get(pool_id, None)
    if (conn_pool == None):
        #if no conn_pool for given pool_id create and store in conn_pools
        conn_pool = Queue()
        conn_pools[pool_id] = conn_pool

    try:
        conn = conn_pool.get(block=False)
    # print "reusing from pool"
    except Empty:
        #if queue is empty create a connection for use.
        if(pool_id in connection_generators):
            conn = connection_generators[pool_id]()

            
    return conn


def release_to_pool(conn, pool_id):
    # print "releasing to pool"
    conn_pools[pool_id].put(conn)


def use_connection_pool(**pool_args):
    def use_db_connection(func):
        def func_wrap(*args, **kwargs):

            conn_args = {}
            for pool_arg, pool_id in pool_args.items():
                conn = get_from_pool(pool_id)
                conn_args[pool_arg] = conn
            ret = None
            conn_args.update(kwargs)
            ret = func(*args, **conn_args)
            for pool_arg, pool_id in pool_args.items():
                release_to_pool(conn_args[pool_arg], pool_id)
            return ret

            # TODO: try to remove connection timeout
            # retry = 0
            # while (retry < 2):
            #     conn_args = {}
            #     for pool_arg, pool_id in pool_args.iteritems():
            #         conn = get_from_pool(pool_id)
            #         conn_args[pool_arg] = conn
            #     ret = None
            #     try:
            #         conn_args.update(kwargs)
            #         ret = func(*args, **conn_args)
            #         for pool_arg, pool_id in pool_args.iteritems():
            #             release_to_pool(conn_args[pool_arg], pool_id)
            #         return ret
            #     except Exception, e:
            #         logger.error(message=e, event="message") # dont release that stupid connections to pool, instread write destroyers for each pool
            #         logger.error(message=traceback.format_exc(), event="message")
            #
            #         retry += 1
            #         if retry >= 2:
            #             raise e
        func_wrap._original = func
        return func_wrap

    return use_db_connection

'''
uses gevent.Queue to maintain pools

@use_connection_pool(arg="pool_name")
def func(arg=None):
    # arg is automatically populated from pool_name,
    # and after function is executed it is put back to pool
    pass

or

def func():
    get_from_pool("pool_name")
    release_to_pool("pool_name")


'''
