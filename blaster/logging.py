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
from .config import LOG_LEVEL
# CRITICAL-50 ERROR-40  WARNING-30  INFO-20  DEBUG-10  NOTSET-0

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


deployment_config = json.loads(open(".deploy").read()) if os.path.isfile(".deploy") else {}

# some constants
_app_name = deployment_config.get("app", "BlasterApp")
_app_version = deployment_config.get("version", "unknown")


log_streaming_thread = None
log_queue = Queue()


# Specific configs
es_config = None
udp_config = None
custom_log_handlers = None


# starts a loop to stream logs to sinks
def stream_logs_loop():

	timestamp = -24 * 60 * 60

	# elasticsearch specific
	es_conn = None
	es_index_name = None
	es_base_index_name = None
	if(es_config):
		from elasticsearch import Elasticsearch
		es_conn = Elasticsearch(
			hosts=es_config.get("hosts") or [es_config["host"]]
		)

		es_base_index_name = es_config.get("index", "blaster_app_logs") + "_"

	# create index if not exist
	while stream_logs_loop.can_run or not log_queue.empty():
		log_item = log_queue.get()

		# check and stream to elasticsearch
		if(es_conn):
			try:
				# if time elapsed since last new index creation
				if(time.time() > timestamp + 24 * 60 * 60):
					_d = datetime.now()
					start_of_today = datetime(year=_d.year, month=_d.month, day=_d.day)
					timestamp = start_of_today.timestamp()
					es_index_name = es_base_index_name + start_of_today.strftime("%d-%m-%Y")
					# try creating index if not already
					es_conn.indices.create(
						index=es_index_name,
						body={
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
			except Exception as ex:
				print("Exception streaming logs to elasticsearch:", str(ex), es_index_name)

		if(custom_log_handlers):
			for _handler in custom_log_handlers:
				_handler(log_item)


stream_logs_loop.can_run = False


def start_log_streaming(es_config=None, udp_config=None, log_handlers=None):
	if(_this_.log_streaming_thread):
		# already started
		raise Exception("Already started log streaming")

	_this_.es_config = es_config
	_this_.udp_config = udp_config
	_this_.custom_log_handlers = log_handlers

	# start the thread
	_this_.log_streaming_thread = Thread(
		target=stream_logs_loop,
		args=(),
		name="log_streamer"
	)
	_this_.log_streaming_thread.start()
	stream_logs_loop.can_run = True


@events.register_listener(["blaster_exit0"])
def stop_log_streaming():
	# cannot start log streaming anymore
	if(not stream_logs_loop.can_run):
		return  # double calling function

	stream_logs_loop.can_run = False


@events.register_listener(["blaster_exit1"])
def flush_and_exit_log_streaming():
	# don't remove it , it will push an empty function to queues to flush them off
	LOG(LOG_LEVEL, "log_flushing", msg="flushing logs and exiting")

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
	global udp_log_listeners_i

	if(level < LOG_LEVEL):
		return

	log_level_name = log_level_to_names.get(level, "SERVER")
	# print to stdout

	print("%s%s [%s]"%(log_level_colors.get(level, PrintColors.OKGREEN), datetime.now(), log_level_name),
		log_type, json.dumps(kwargs),
		PrintColors.ENDC
	)

	if(log_streaming_thread):
		log_queue.put({
			"log_type": log_type,
			"log_level": log_level_name,
			"app": _app_name,
			"version": _app_version,
			"timestamp": int(1000 * time.time()),
			"payload": kwargs
		})


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
