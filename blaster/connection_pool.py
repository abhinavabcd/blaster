from collections import deque
from .config import IS_DEV, \
    AWS_CREDENTIALS, GCLOUD_CREDENTIALS
# import umysql

_conn_pools = {}
_pool_item_generators = {}


def register_pool_item_generator(pool_name, func):
    _conn_pools[pool_name] = deque()
    _pool_item_generators[pool_name] = func


def get_from_pool(pool_name):
    try:
        return _conn_pools[pool_name].popleft()
    except IndexError:
        # if queue is empty create a connection for use.
        return _pool_item_generators[pool_name]()


def release_to_pool(conn, pool_name):
    # print "releasing to pool"
    _conn_pools[pool_name].append(conn)


# decorator that supplies argument from a given pool and puts back after use

def use_connection_pool(**pool_args):
    # check and register None pool at load time, 
    # instead of checking runtime, to avoid runtime crashes
    for pool_arg, pool_name in pool_args.items():
        if(pool_name not in _pool_item_generators):
            register_pool_item_generator(pool_name, lambda: None)

    def use_db_connection(func):
        def wrapper(*args, **kwargs):
            for pool_arg, pool_name in pool_args.items():
                kwargs[pool_arg] = get_from_pool(pool_name)
            ret = func(*args, **kwargs)  # if there is an exception that item is not put into pool          
            for pool_arg, pool_name in pool_args.items():
                release_to_pool(kwargs[pool_arg], pool_name)
            return ret
        wrapper._original = getattr(func, "_original", func)
        return wrapper

    return use_db_connection


# AWS session/clients
boto_session = None


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


# gcloud clients
gcloud_credentials = None


def init_gcloud_clients(_gcloud_credentials):
    global gcloud_credentials
    from google.oauth2 import service_account
    gcloud_credentials = service_account.Credentials.from_service_account_info(_gcloud_credentials)


def get_gcloud_tasks_client():
    from google.cloud import tasks_v2
    return tasks_v2.CloudTasksClient(credentials=gcloud_credentials)


def get_gcloud_storage():
    from google.cloud import storage
    return storage.Client(credentials=gcloud_credentials)


# try autoloading if already set
AWS_CREDENTIALS and init_aws_clients(AWS_CREDENTIALS)
GCLOUD_CREDENTIALS and init_gcloud_clients(GCLOUD_CREDENTIALS)

# register some default pools
register_pool_item_generator("dynamodb", get_dynamodb_conn)
register_pool_item_generator("s3", get_s3_conn)
register_pool_item_generator("sqs", get_sqs_conn)
register_pool_item_generator("ses", get_ses_conn)
register_pool_item_generator("google_cloudtasks", get_gcloud_tasks_client)
register_pool_item_generator("google_cloudstorage", get_gcloud_storage)


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
