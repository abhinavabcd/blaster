from logging import DEBUG, INFO, WARN, ERROR
from ..config import LOG_LEVEL
from datetime import datetime
import ujson as json

# CRITICAL-50 ERROR-40  WARNING-30  INFO-20  DEBUG-10  NOTSET-0

#more levels
SERVER_INFO = 21
APP_INFO = 22

log_level_to_names = {
	DEBUG: "DEBUG",
	INFO: "INFO",
	WARN: "WARNING",
	ERROR: "ERROR",
	SERVER_INFO: "SERVER",
	APP_INFO: "APP"
}

def LOG(level, log_type, **kwargs):
	if(level < LOG_LEVEL):
		return
	#TODO: send to UDP listener
	print(log_level_to_names.get(level, "SERVER"), log_type, datetime.now(), json.dumps(kwargs))

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
