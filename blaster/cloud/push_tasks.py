'''
Created on 27-Feb-2017

'''
import types
from datetime import datetime

import base64
import pickle
import traceback
import ujson as json
import gevent
from ..server import route, Request, is_server_running
from ..tools import background_task, hmac_hexdigest, get_random_id, retry
from ..connection_pool import use_connection_pool
from ..utils import events
from ..logging import LOG_ERROR, LOG_WARN, LOG_SERVER, LOG_DEBUG
from ..config import PUSH_TASKS_SQS_URL,\
	GCLOUD_TASKS_QUEUE_PATH, GCLOUD_TASK_RUNNER_HOST, GCLOUD_TASKS_AUTH_SECRET

push_tasks = {}
sqs_reader_greenlets = []

@retry(2)
def exec_push_task(raw_bytes_message: bytes, auth=None):
	message_payload = pickle.loads(base64.a85decode(raw_bytes_message))
	kwargs = message_payload.get("kwargs", {})
	args = message_payload.get("args", [])
	func_name = message_payload.get("func_v2", "")
	# check for authorization
	if(
		auth 
		and hmac_hexdigest(auth, func_name) != message_payload.get("signature")
	):
		LOG_ERROR("push_tasks", desc="authorization failed", func=func_name)
		
	# task_id = message_payload.get("task_id", "")
	# TODO use task_id for logging
	func = push_tasks.get(func_name, None)
	if func:
		func(*args, **kwargs)
	else:
		LOG_WARN("server_exception", data="Not a push task", func=str(func))

@use_connection_pool(sqs_client="sqs")
def process_from_sqs(queue_url, msgs_per_batch=10, sqs_client=None):
	while(is_server_running()):
		try:
			response = sqs_client.receive_message(
					QueueUrl=queue_url,
					MessageAttributeNames=['All'],
					MaxNumberOfMessages=msgs_per_batch,
					VisibilityTimeout=60,
					WaitTimeSeconds=20 # long polling gevent safe
					# ,ReceiveRequestAttemptId=''   , make it unique for each instance , probably when bootup with an ip ?
			)

			for sqs_message in response.get("Messages", []):

				exec_push_task(sqs_message.get("Body").encode())

				_temp = sqs_client.delete_message(
					QueueUrl=queue_url,
					ReceiptHandle=sqs_message.get("ReceiptHandle", None)
				)

				LOG_SERVER("sqs_processed", data=json.dumps(_temp))

		except Exception:
			LOG_WARN("sqs_exception", stack_trace=traceback.format_exc())


@route("/gcloudtask")
def gcloud_task(request:Request):
	exec_push_task(request._post_data, auth=GCLOUD_TASKS_AUTH_SECRET)
	return "OK"


@use_connection_pool(sqs_client="sqs")
def post_task_to_sqs(push_tasks_sqs_url, message_body, sqs_client=None):
	message_body = pickle.dumps(message_body)
	return sqs_client.send_message(
		QueueUrl=push_tasks_sqs_url,
		MessageBody=base64.a85encode(message_body).decode(),  # to utf-8 string
		DelaySeconds=1
	)

@use_connection_pool(gcloudtasks_client="google_cloudtasks")
def post_task_to_gcloud_tasks(queue_path, host, message_body: dict, gcloudtasks_client=None):
	if(GCLOUD_TASKS_AUTH_SECRET):
		message_body["signature"] = hmac_hexdigest(
			GCLOUD_TASKS_AUTH_SECRET, message_body["func_v2"]
		)

	message_body = base64.a85encode(pickle.dumps(message_body)) # bytes
	task = {
		"http_request": {  # Specify the type of request.
			"http_method": 1, #tasks_v2.HttpMethod.POST,
			"url": host + "/gcloudtask",  # The full url path that the task will be sent to.
			"headers": {"Content-type": "text/plain"},
			"body": message_body
		}
	}

	return gcloudtasks_client.create_task(
		request={
			"parent": queue_path,
			"task": task
		}
	)

@background_task
def post_a_task(func, *args, **kwargs):
	args = list(args)

	if isinstance(func, str) or isinstance(func, bytes):
		func_name = func
	elif isinstance(func, types.FunctionType):
		func_name = func.__name__
	else:
		return None

	now = datetime.utcnow().isoformat()
	task_id = get_random_id()
	message_body = {
		"args": args,
		"kwargs": kwargs,
		"func_v2": func_name,
		"task_id": task_id,
		"created_at": now
	}

	if(sqs_url := PUSH_TASKS_SQS_URL):
		return post_task_to_sqs(sqs_url, message_body)
	elif(
		(gcloud_tasks_queue_path := GCLOUD_TASKS_QUEUE_PATH)
		and (gcloud_task_runner_host := GCLOUD_TASK_RUNNER_HOST)
	):
		#ex: queue_path: projects/<PROJECT_NAME>/locations/europe-west3/queues/<QUEUE_NAME>
		return post_task_to_gcloud_tasks(
			gcloud_tasks_queue_path, gcloud_task_runner_host, message_body
		)
	else:
		LOG_WARN("server_info", data="calling directly as not queue provided")
		# test pickling 
		# exec_push_task(base64.a85encode(pickle.dumps(message_body)))
		func = push_tasks.get(
			message_body.get("func_v2", ""),
			None
		)
		func and func(*message_body.get("args", []), **message_body.get("kwargs", {}))
		return None


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
	wrapper._original = getattr(func, "_original", func)
	return wrapper


def init_push_task_handlers(parallel=1):
	if(PUSH_TASKS_SQS_URL):
		for _ in range(parallel):
			sqs_reader_greenlets.append(gevent.spawn(process_from_sqs, PUSH_TASKS_SQS_URL))
		


# cleanup
@events.register_listener("blaster_exit0")
def wait_for_push_tasks_processing():
	if(sqs_reader_greenlets):
		LOG_WARN("server_info", data="finishing pending push tasks via SQS")
		gevent.joinall(sqs_reader_greenlets)
		del sqs_reader_greenlets[:]
