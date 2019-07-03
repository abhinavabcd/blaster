'''
Created on 27-Feb-2017

'''
import types
from datetime import datetime

import traceback
import ujson as json
import gevent
from .. import config
from ..constants import LOG_TYPE_PROCESSED_SQS_MESSAGE, LOG_TYPE_SERVER_INFO, LOG_TYPE_EXCEPTION
from ..base import push_tasks, server_log, is_server_running
from ..common_funcs_and_datastructures import get_random_id,\
    cur_ms
from ..connection_pool import get_from_pool,\
    use_connection_pool
from ..base import base


sqs_reader_greenlets = []

@use_connection_pool(queue="sqs")
def start_boto_sqs_readers(queue=None):
    if(not config.sqs_url):
        server_log(LOG_TYPE_SERVER_INFO, data="No queue url provided, not starting readers")
        return
        
    queue_url = config.sqs_url
    
    def process_from_sqs():
        while(is_server_running()):
            try:
                response = queue.receive_message(
                        QueueUrl=queue_url,
                        MessageAttributeNames=['All'],
                        MaxNumberOfMessages=10,
                        VisibilityTimeout=60,
                        WaitTimeSeconds=20 # long polling gevent safe
                        # ,ReceiveRequestAttemptId=''   , make it unique for each instance , probably when bootup with an ip ?
                )
                    
                for sqs_message in response.get("Messages", []):
                    message_payload = json.loads(sqs_message.get("Body", "{}"))
                    kwargs = message_payload.get("kwargs", {})
                    args = message_payload.get("args", [])
                    func_name = message_payload.get("func", "")
                    task_id = message_payload.get("task_id", "")
                    #TODO use task_id for logging
                    func = push_tasks.get(func_name, None)
                    if func:
                        func(*args, **kwargs)
                    else:
                        server_log(LOG_TYPE_EXCEPTION, data="Not a push task", func=str(func))

                    _temp = queue.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=sqs_message.get("ReceiptHandle", None)
                    )
                    
                    server_log(LOG_TYPE_PROCESSED_SQS_MESSAGE , data=json.dumps(_temp))

            except Exception as ex:
                server_log(LOG_TYPE_EXCEPTION, data=str(ex), _type="sqs_exception")
                traceback.print_exc()
        
    for i in range(5):
        sqs_reader_greenlets.append(gevent.spawn(process_from_sqs))

    base.exit_listeners.append(wait_for_push_tasks_processing)


def push_task(func):
    if(isinstance(func, str)):
        #return decorator
        def decorator(func2):
            push_tasks[func] = func2
            return func2

        return decorator
    else:
        _original = getattr(func, "_original", func)
        push_tasks[_original.__name__] = func
        return func

@use_connection_pool(queue="sqs")
def post_a_task(func, *args, **kwargs):
    queue = kwargs.pop("queue")
    args = list(args)

    if isinstance(func, str) or isinstance(func, bytes):
        func_name = func
    elif isinstance(func, types.FunctionType):
        if hasattr(func, "_original"):
            func_name = func._original.__name__
        else:
            func_name = func.__name__
    else:
        return None

    if(not config.sqs_url):
        server_log(LOG_TYPE_SERVER_INFO, data="calling directly as not queue provided")
        func = push_tasks.get(func_name, None)
        func(*args, **kwargs)
        return None


    now = datetime.utcnow().isoformat()
    task_id = get_random_id()
    message_body_json = {"args": args, "kwargs": kwargs, "func": func_name, "task_id": task_id, "created_at": now}
    message_body = json.dumps(message_body_json)
    response = queue.send_message(
            QueueUrl=config.sqs_url,
            MessageBody=message_body,
            DelaySeconds=1
    )
    return response
    


def wait_for_push_tasks_processing():
    server_log(LOG_TYPE_SERVER_INFO, data="wait called")
    gevent.joinall(sqs_reader_greenlets)
    del sqs_reader_greenlets[:]

def run_later(func):
    _original = getattr(func, "_original", func)
    task_name = _original.__name__
    push_tasks[task_name] = func

    def wrapper(*args, **kwargs):
        post_a_task(
            task_name,
            *args,
            **kwargs
        )

    return wrapper
