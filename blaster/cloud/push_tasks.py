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
from ..server import route, Request
from ..tools import background_task, hmac_hexdigest, get_random_id, retry
from ..connection_pool import use_connection_pool, get_gcloud_pubsub_subscriber
from ..utils import events
from ..logging import LOG_ERROR, LOG_WARN, LOG_SERVER
from ..config import RUN_LATER_TASKS_SQS_URL, \
	RUN_LATER_TASKS_GCLOUD_PUBSUB_SUBSCRIPTION_TOPIC, RUN_LATER_TASKS_GCLOUD_PUBSUB_TOPIC, \
	GCLOUD_TASKS_QUEUE_PATH, GCLOUD_TASK_RUNNER_HOST, GCLOUD_TASKS_AUTH_SECRET

push_tasks = {}
server_threads = []
_is_processing = True

@retry(2)
def exec_push_task(raw_bytes_message: bytes, auth=None):
	message_payload = pickle.loads(base64.a85decode(raw_bytes_message))
	print("received", message_payload)
	kwargs = message_payload.get("kwargs", {})
	args = message_payload.get("args", [])
	func_name = message_payload.get("func", "")
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


def process_from_cloud_pubsub(subscription_path):

	gcloud_pubsub_subscriber = get_gcloud_pubsub_subscriber()
	while(_is_processing):
		response = gcloud_pubsub_subscriber.pull(
			subscription=subscription_path, max_messages=50
		)
		if(not response.received_messages):
			gevent.sleep(5)
			continue
		for received_message in response.received_messages:
			try:
				exec_push_task(
					received_message.message.data,
					auth=GCLOUD_TASKS_AUTH_SECRET
				)
				gcloud_pubsub_subscriber.acknowledge(
					subscription=subscription_path,
					ack_ids=[received_message.ack_id]
				)
			except Exception:
				LOG_WARN("cloud_pubsub_exception", stack_trace=traceback.format_exc())

	gcloud_pubsub_subscriber.close()


@use_connection_pool(gcloud_pubsub_publisher="gcloud_pubsub")
def run_later_via_gcloud_pubsub(topic, message_body: dict, gcloud_pubsub_publisher=None):
	print("posting to pubslisher", message_body)
	message_body = base64.a85encode(pickle.dumps(message_body))  # bytes
	return gcloud_pubsub_publisher.publish(
		topic=topic,
		data=message_body
	)


@use_connection_pool(sqs_client="sqs")
def process_from_sqs(queue_url, msgs_per_batch=10, sqs_client=None):
	while(_is_processing):
		try:
			response = sqs_client.receive_message(
				QueueUrl=queue_url,
				MessageAttributeNames=['All'],
				MaxNumberOfMessages=msgs_per_batch,
				VisibilityTimeout=60,
				WaitTimeSeconds=20  # long polling gevent safe
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


@use_connection_pool(sqs_client="sqs")
def run_later_via_sqs(push_tasks_sqs_url, message_body, sqs_client=None):
	message_body = pickle.dumps(message_body)
	return sqs_client.send_message(
		QueueUrl=push_tasks_sqs_url,
		MessageBody=base64.a85encode(message_body).decode(),  # to utf-8 string
		DelaySeconds=1
	)


@use_connection_pool(gcloudtasks_client="gcloud_tasks")
def run_later_via_gcloud_tasks(queue_path, host, message_body: dict, gcloudtasks_client=None):
	if(GCLOUD_TASKS_AUTH_SECRET):
		message_body["signature"] = hmac_hexdigest(
			GCLOUD_TASKS_AUTH_SECRET, message_body["func"]
		)

	message_body = base64.a85encode(pickle.dumps(message_body))  # bytes
	task = {
		"http_request": {  # Specify the type of request.
			"http_method": 1,  # tasks_v2.HttpMethod.POST,
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


if(GCLOUD_TASKS_QUEUE_PATH and GCLOUD_TASK_RUNNER_HOST):
	@route("/gcloudtask")
	def gcloud_task(request: Request):
		exec_push_task(request._post_data, auth=GCLOUD_TASKS_AUTH_SECRET)
		return "OK"


@background_task
def _run_later(func, *args, **kwargs):
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
		"func": func_name,
		"task_id": task_id,
		"created_at": now
	}

	if(sqs_url := RUN_LATER_TASKS_SQS_URL):
		return run_later_via_sqs(sqs_url, message_body)
	elif(gcloud_pubsub_topic := RUN_LATER_TASKS_GCLOUD_PUBSUB_TOPIC):
		return run_later_via_gcloud_pubsub(gcloud_pubsub_topic, message_body)
	elif(
		(gcloud_tasks_queue_path := GCLOUD_TASKS_QUEUE_PATH)
		and (gcloud_task_runner_host := GCLOUD_TASK_RUNNER_HOST)
	):
		return run_later_via_gcloud_tasks(
			gcloud_tasks_queue_path, gcloud_task_runner_host, message_body
		)
	else:
		LOG_WARN("server_info", data="calling directly as not queue provided")
		# test pickling
		# exec_push_task(base64.a85encode(pickle.dumps(message_body)))
		func = push_tasks.get(
			message_body.get("func", ""),
			None
		)
		func and func(*message_body.get("args", []), **message_body.get("kwargs", {}))
		return None


def run_later(func):
	_original = getattr(func, "_original", func)
	task_name = _original.__name__
	push_tasks[task_name] = func

	def wrapper(*args, **kwargs):
		_run_later(
			task_name,
			*args,
			**kwargs
		)
	wrapper._original = getattr(func, "_original", func)
	return wrapper


def process_run_later_tasks():
	global _is_processing
	_is_processing = True
	if(RUN_LATER_TASKS_SQS_URL):
		server_threads.append(gevent.spawn(process_from_sqs, RUN_LATER_TASKS_SQS_URL))
	elif(RUN_LATER_TASKS_GCLOUD_PUBSUB_TOPIC):
		subscription_path = RUN_LATER_TASKS_GCLOUD_PUBSUB_SUBSCRIPTION_TOPIC
		if(not subscription_path):
			subscription_path = RUN_LATER_TASKS_GCLOUD_PUBSUB_TOPIC.replace("topics/", "subscriptions/") + "-sub"
		server_threads.append(
			gevent.spawn(process_from_cloud_pubsub, subscription_path)
		)


# cleanup
@events.register_listener("blaster_exit0")
def wait_for_push_tasks_processing():
	global _is_processing
	_is_processing = False
	if(server_threads):
		LOG_WARN("server_info", data="finishing pending push tasks via SQS")
		gevent.joinall(server_threads)
		del server_threads[:]
