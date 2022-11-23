'''
Created on 29-Nov-2017

@author: abhinav
'''
from gevent import monkey
monkey.patch_all()  # replaces read , write, send , sleep ...
from gevent import local, signal_handler

# gevent local with some default
class __ReqCtx(local.local):
	def __init__(self, **kwargs):
		self.__dict__.update({"req": None, "timestamp": None, "client_name": None})
req_ctx = __ReqCtx()
# END gevent local


import sys

## override config module, hack
from .config import config
sys.modules["blaster.config"] = config

## cleanup handlers
import signal
from .utils import events

def blaster_exit():
	# send exit signals
	# stage 0 -> stop creating new things
	# stage 1 -> initiate closing all connections, waiting things
	# ..
	# stage 5 -> all things cleanedup and done
	for i in range(6):
		events.broadcast_event("blaster_exit" + str(i))

# sigint event broadcast
signal_handler(signal.SIGINT, blaster_exit)
