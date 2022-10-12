from gevent.queue import Queue, Empty
from .utils import events
from .config import IS_DEV, \
    AWS_CREDENTIALS, GCLOUD_CREDENTIALS
# import umysql

conn_pools = {}

# AWS session/clients
boto_session = None

@events.register_listener("CONFIG_AWS_CREDENTIALS")
def init_aws_clients(aws_credentials):
    import boto3
    global boto_session
    boto_session = boto3.session.Session(**aws_credentials)

def get_dynamodb_conn():
    if(IS_DEV):
        import boto3
        return boto3.resource(
            'dynamodb',
            endpoint_url='http://{endpoint}:{port}'.format(endpoint="127.0.0.1", port="8000"),
            aws_access_key_id="",
            aws_secret_access_key="",
            region_name="ap-south-1"
        )
    else:
        return boto_session.resource('dynamodb')

def get_s3_conn():
    return boto_session.resource('s3')

def get_sqs_conn():
    return boto_session.client('sqs')

def get_ses_conn():
    return boto_session.client('ses')


## gcloud clients
gcloud_credentials = None

@events.register_listener("CONFIG_GCLOUD_CREDENTIALS")
def init_gcloud_clients(_gcloud_credentials):
    global gcloud_credentials
    from google.oauth2 import service_account
    gcloud_credentials = service_account.Credentials.from_service_account_info(_gcloud_credentials)


def get_gcloud_tasks_client():
    from google.cloud import tasks_v2
    return tasks_v2.CloudTasksClient(credentials=gcloud_credentials)

def get_gcloud_storage():
    from google.cloud import storage
    return None


#default connection generators
_pool_item_generators = {
    "dynamodb": get_dynamodb_conn,
    "s3" : get_s3_conn,
    "sqs": get_sqs_conn,
    "ses": get_ses_conn,
    "google_cloudtasks": get_gcloud_tasks_client,
    "google_cloudstorage": get_gcloud_storage
}

def register_pool_item_generator(pool_name, func):
    _pool_item_generators[pool_name] = func


def get_from_pool(pool_id):
    conn = None
    conn_pool = conn_pools.get(pool_id, None)
    if (conn_pool == None):
        # if no conn_pool for given pool_id create and store in conn_pools
        conn_pool = Queue()
        conn_pools[pool_id] = conn_pool

    try:
        conn = conn_pool.get(block=False)
    # print "reusing from pool"
    except Empty:
        #if queue is empty create a connection for use.
        if(pool_id in _pool_item_generators):
            conn = _pool_item_generators[pool_id]()

    return conn


def release_to_pool(conn, pool_id):
    # print "releasing to pool"
    conn_pools[pool_id].put(conn)


def use_connection_pool(**pool_args):
    def use_db_connection(func):
        def wrapper(*args, **kwargs):

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
        wrapper._original = getattr(func, "_original", func)
        return wrapper

    return use_db_connection


# try autoloading if already set
AWS_CREDENTIALS and init_aws_clients(AWS_CREDENTIALS)
GCLOUD_CREDENTIALS and init_gcloud_clients(GCLOUD_CREDENTIALS)



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
