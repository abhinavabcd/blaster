'''
Created on 27-Feb-2017

'''
import types
from datetime import datetime
from functools import partial
import base64
import pickle
import traceback
import ujson as json
import gevent
from ..server import route, Request
from ..tools import background_task, hmac_hexdigest, get_random_id, \
	PartitionedTasksRunner
from ..connection_pool import use_connection_pool, get_gcloud_pubsub_subscriber
from ..utils import events
from ..logging import LOG_ERROR, LOG_WARN, LOG_SERVER
from ..config import RUN_LATER_TASKS_SQS_URL, \
	RUN_LATER_TASKS_GCLOUD_PUBSUB_SUBSCRIPTION_TOPIC, RUN_LATER_TASKS_GCLOUD_PUBSUB_TOPIC, \
	GCLOUD_TASKS_QUEUE_PATH, GCLOUD_TASK_RUNNER_HOST, GCLOUD_TASKS_AUTH_SECRET

run_later_tasks = {}  # tracks all push tasks

_joinables = []  # all long running threads, that need to be stopped on exit
partitioned_tasks_runner = PartitionedTasksRunner(max_parallel=100)


# just submit to background wait a max of 10 seconds
# careful: raises an exception if queue limits reaches, should try to reprocess in that case
def exec_push_task(raw_bytes_message: bytes, verify_secret=None):
	message_payload = pickle.loads(base64.a85decode(raw_bytes_message))
	kwargs = message_payload.get("kwargs") or {}
	args = message_payload.get("args") or []
	func_name = message_payload.get("func", "")
	parition_key = message_payload.get("partition")
	max_backlog = message_payload.get("max_backlog", 50)
	# check for authorization
	if(
		verify_secret
		and hmac_hexdigest(verify_secret, func_name) != message_payload.get("signature")
	):
		LOG_ERROR("run_later", desc="authorization failed", func=func_name)
		return

	# task_id = message_payload.get("task_id", "")
	# TODO use task_id for logging
	if(func := run_later_tasks.get(func_name, None)):
		LOG_SERVER("run_later", desc="executing push task", func=func_name)
		partitioned_tasks_runner.submit_task(
			parition_key, func, args, kwargs,
			max_backlog=max_backlog, timeout=10
		)
		return

	LOG_WARN("run_later_exception", data="Not a push task", func=str(func))


def process_from_cloud_pubsub(subscription_path):
	LOG_SERVER("run_later", desc="starting pubsub subscriber", subscription_path=subscription_path)

	def callback(message):
		exec_push_task(message.data)
		message.ack()

	pull_future = None

	@events.register_listener("blaster_exit1")
	def _stop():  # stop the pull_future on event
		pull_future and pull_future.cancel()
		events.remove_listener("blaster_exit1", _stop)  # remove the event reference

	while(_is_processing):
		try:
			with get_gcloud_pubsub_subscriber() as subscriber:
				pull_future = subscriber.subscribe(
					subscription_path, callback=callback
				)
				try:
					pull_future.result()  # Block until the feature is completed.
				except Exception:
					LOG_WARN("gcloud_pubsub_exception", stack_trace=traceback.format_exc())
					pull_future.cancel()
		except Exception as ex:
			LOG_WARN("gcloud_pubsub_exception", stack_trace=traceback.format_exc())
			gevent.sleep(10)

	partitioned_tasks_runner.stop()
	LOG_SERVER("run_later", data="stopping run later tasks for gcloud pubsub")


@use_connection_pool(gcloud_pubsub_publisher="gcloud_pubsub_publisher")
def run_later_via_gcloud_pubsub(topic, message_body: dict, gcloud_pubsub_publisher=None):
	message_body = base64.a85encode(pickle.dumps(message_body))  # bytes
	ret = gcloud_pubsub_publisher.publish(topic, message_body)
	ret.result()  # wait for it to be published


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
def run_later_via_sqs(run_later_sqs_url, message_body, sqs_client=None):
	message_body = pickle.dumps(message_body)
	return sqs_client.send_message(
		QueueUrl=run_later_sqs_url,
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
		exec_push_task(request._post_data, verify_secret=GCLOUD_TASKS_AUTH_SECRET)
		return "OK"


@background_task  # background so as to not block the main thread
def _run_later(partition_key, func, args, kwargs):
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
	if(partition_key):
		message_body["partition"] = partition_key

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
		func = run_later_tasks.get(
			message_body.get("func", ""),
			None
		)
		func and func(*message_body.get("args", []), **message_body.get("kwargs", {}))
		return None


def run_later(func, partition_key_func=None):
	_original = getattr(func, "_original", func)
	task_name = _original.__name__  # f"{_original.__module__}{_original.__name__}"
	run_later_tasks[task_name] = func

	def wrapper(*args, **kwargs):
		_run_later(
			partition_key_func(args, kwargs) if partition_key_func else None,
			task_name,
			args,
			kwargs
		)
	wrapper._original = getattr(func, "_original", func)
	return wrapper


def run_later_partitioned(partition_key):
	partition_key_func = partition_key

	def _simple_partition_key_func(args, kwargs):
		return str(partition_key)

	if(not callable(partition_key)):
		partition_key_func = _simple_partition_key_func

	return lambda func: run_later(func, partition_key_func=partition_key_func)


def process_run_later_tasks():
	global _is_processing
	_is_processing = True

	if(RUN_LATER_TASKS_SQS_URL):
		_joinables.append(gevent.spawn(process_from_sqs, RUN_LATER_TASKS_SQS_URL))
	elif(RUN_LATER_TASKS_GCLOUD_PUBSUB_TOPIC):
		subscription_path = RUN_LATER_TASKS_GCLOUD_PUBSUB_SUBSCRIPTION_TOPIC
		if(not subscription_path):
			subscription_path = RUN_LATER_TASKS_GCLOUD_PUBSUB_TOPIC.replace("topics/", "subscriptions/") + "-sub"
		_joinables.append(
			gevent.spawn(process_from_cloud_pubsub, subscription_path)
		)


# cleanup
@events.register_listener("blaster_exit0")
def _exit0():
	global _is_processing
	_is_processing = False
	partitioned_tasks_runner.stop()


@events.register_listener("blaster_exit2")
def _exit2():
	for _joinable in _joinables:
		_joinable.join()
