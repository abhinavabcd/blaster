import os
# import socket
import sys
import time
from datetime import datetime
import ujson as json
from gevent.queue import Queue
from gevent.threading import Thread
from logging import DEBUG, INFO, WARN, ERROR
from .utils import events
from .env import LOG_LEVEL, LOG_APP_NAME, LOG_APP_VERSION,\
	CONSOLE_LOG_RAW_JSON
from . import req_ctx

# CRITICAL-50 ERROR-40  WARNING-30  INFO-20  DEBUG-10  NOTSET-0

_1_DAY_MILLIS = 24 * 60 * 60 * 1000
_this_ = sys.modules[__name__]

# more levels
SERVER_INFO = 31
APP_INFO = 32

log_level_to_names = {
	DEBUG: "DEBUG",
	INFO: "INFO",
	WARN: "WARNING",
	ERROR: "ERROR",
	SERVER_INFO: "SERVER",
	APP_INFO: "APP"
}


log_streaming_thread = None
log_queue = Queue()


# Specific configs
log_handlers = []


# starts a loop to stream logs to sinks
def stream_logs_loop():
	# create index if not exist
	while stream_logs_loop.can_run or not log_queue.empty():
		log_item = log_queue.get()
		for _handler in log_handlers:
			try:
				_handler(log_item)
			except Exception as ex:
				print("Exception in log handler: {ex}")


def create_es_log_handler(es_config):
	from elasticsearch import Elasticsearch
	es_conn = Elasticsearch(
		hosts=es_config.get("hosts") or [es_config["host"]]
	)
	es_base_index_name = es_config.get("index", "blaster_app_logs")
	if(not es_base_index_name.endswith("_logs")):
		es_base_index_name += "_logs"

	# try creating index if not already
	es_index_name = None
	start_of_today_timestamp = 0

	def log_handler(log_item):
		nonlocal es_index_name
		nonlocal start_of_today_timestamp
		log_timestamp = log_item.get("timestamp") or int(time.time() * 1000)
		# create a new index if the log belongs to next day
		if(log_timestamp > start_of_today_timestamp + _1_DAY_MILLIS):  # next day
			_d = datetime.now()
			start_of_today \
				= datetime(year=_d.year, month=_d.month, day=_d.day)

			start_of_today_timestamp = start_of_today.timestamp()

			es_index_name = "{}_{}".format(
				es_base_index_name, start_of_today.strftime("%Y-%m-%d")
			)
			es_conn.indices.create(
				index=es_index_name,
				body={
					"aliases": {
						es_base_index_name: {}
					},
					"mappings": {
						"dynamic": False,
						"properties": {
							"log_type": {"type": "keyword"},
							"log_level": {"type": "keyword"},
							"app": {"type": "keyword"},
							"version": {"type": "keyword"},
							"timestamp": {"type": "date", "format": "epoch_millis||yyyy-MM-dd HH:mm:ss"},
							"payload": {"dynamic": True, "properties": {}}
						}
					}
				},
				ignore=400
			)

		es_conn.index(es_index_name, log_item)

	return log_handler


stream_logs_loop.can_run = False


def start_log_streaming(es_config=None, udp_config=None, log_handlers=None):
	if(_this_.log_streaming_thread):
		# already started
		raise Exception("Already started log streaming")

	if(es_config):
		_this_.log_handlers.append(create_es_log_handler(es_config))

	log_handlers and _this_.log_handlers.extend(log_handlers)

	# start the thread
	_this_.log_streaming_thread = Thread(
		target=stream_logs_loop,
		args=(),
		name="log_streamer"
	)
	stream_logs_loop.can_run = True
	_this_.log_streaming_thread.start()


@events.register_listener(["blaster_exit0"])
def stop_log_streaming():
	# cannot start log streaming anymore
	if(not stream_logs_loop.can_run):
		return  # double calling function

	stream_logs_loop.can_run = False


@events.register_listener(["blaster_exit1"])
def flush_and_exit_log_streaming():
	# don't remove it , it will push an empty function to queues to flush them off
	LOG(DEBUG, "log_flushing", msg="flushing logs and exiting")

	log_streaming_thread and log_streaming_thread.join()


class PrintColors:
	HEADER = '\033[95m'
	OKBLUE = '\033[94m'
	OKGREEN = '\033[92m'
	WARNING = '\033[93m'
	FAIL = '\033[91m'
	ENDC = '\033[0m'


log_level_colors = {
	WARN: PrintColors.WARNING,
	ERROR: PrintColors.FAIL,
}


# LOGGING functions start
def LOG(level, log_type, **kwargs):

	if(level < LOG_LEVEL):
		return

	log_level_name = log_level_to_names.get(level, "SERVER")
	# print to stdout

	_log = {
		"log_level": log_level_name,
		"log_type": log_type,
		"app": LOG_APP_NAME,
		"version": LOG_APP_VERSION,
		"timestamp": req_ctx.timestamp or int(1000 * time.time()),
		"payload": kwargs
	}
	# blaster request
	if(req := req_ctx.req):
		_log["ip"] = req.ip_port[0]
		if(client_name := req.client_name):
			_log["client"] = client_name

	if(CONSOLE_LOG_RAW_JSON):
		print(json.dumps(_log))
	else:
		print(
			"%s%s [%s]"%(
				log_level_colors.get(level, PrintColors.OKGREEN),
				datetime.now(),
				log_level_name
			),
			log_type, json.dumps(kwargs),
			PrintColors.ENDC
		)

	# put this on quue if a log_streaming thread is processing it
	if(log_streaming_thread):
		log_queue.put(_log)


def LOG_SERVER(log_type, **kwargs):
	LOG(SERVER_INFO, log_type, **kwargs)


def LOG_APP_INFO(log_type, **kwargs):
	LOG(APP_INFO, log_type, **kwargs)


def LOG_DEBUG(log_type, **kwargs):
	LOG(DEBUG, log_type, **kwargs)


def LOG_INFO(log_type, **kwargs):
	LOG(INFO, log_type, **kwargs)


def LOG_WARN(log_type, **kwargs):
	LOG(WARN, log_type, **kwargs)


def LOG_ERROR(log_type, **kwargs):
	LOG(ERROR, log_type, **kwargs)
