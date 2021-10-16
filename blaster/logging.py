import os
import socket
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

#more levels
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

#some constants
_app_name = deployment_config.get("app", "BlasterApp")
_app_version = deployment_config.get("version", "unknown")


logging_es_conn = None
es_log_streaming_thread = None
log_queue = Queue()

#starts a loop to stream logs to sinks
def stream_logs_loop(es_config):

	index_name = es_config.get("index", "blaster_app_logs")
	#create index if not exist
	logging_es_conn.indices.create(index=index_name, ignore=400)
	while stream_logs_loop.can_run or not log_queue.empty():
		try:
			log_item = log_queue.get()
			logging_es_conn.index(index_name, log_item)
		except Exception as ex:
			print("Exception streaming logs to elasticsearch :", str(ex))
			

stream_logs_loop.can_run = False


#######this is the exposed function ########
#logging.stream_logs_to_es -> to stream logs to elasticsearch
def stream_logs_to_es(es_config): # {"index": , "host": } #new index per day
	from elasticsearch import Elasticsearch
	_this_.logging_es_conn = Elasticsearch(
		hosts=es_config.get("hosts") or [es_config["host"]]
	)

	_this_.es_log_streaming_thread = Thread(
		target=stream_logs_loop,
		args=(es_config,)
	)
	_this_.es_log_streaming_thread.start()
	stream_logs_loop.can_run = True


@events.register_as_listener(["sigint", "blaster_atexit", "blaster_after_shutdown"])
def flush_and_exit_log_streaming():

	if(not stream_logs_loop.can_run):
		return # double calling function

	stream_logs_loop.can_run = False

	#push an empty function to queues to flush them off
	LOG_SERVER("server_log_streaming_exit", msg="flushing logs and exiting")

	es_log_streaming_thread and es_log_streaming_thread.join()


#LOGGING functions start
def LOG(level, log_type, **kwargs):
	global udp_log_listeners_i

	if(level < LOG_LEVEL):
		return

	log_level_name = log_level_to_names.get(level, "SERVER")
	print(log_level_name, log_type, datetime.now(), json.dumps(kwargs)) # stdout

	if(logging_es_conn):
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
