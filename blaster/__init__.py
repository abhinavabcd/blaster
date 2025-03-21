'''
Created on 29-Nov-2017

@author: abhinav
'''
from gevent import monkey; monkey.patch_all()  # replaces read , write, send , sleep ...
from gevent import local, signal_handler, signal
# some other dependencies to patch
try:
	import grpc.experimental.gevent
	grpc.experimental.gevent.init_gevent()
except Exception:
	pass

import sys
import os
import inspect
# override config module, hack
from .config import config
from .utils import events


sys.modules["blaster.config"] = config  # hack


# gevent local with some default
class __ReqCtx(local.local):
	def __init__(self, **kwargs):
		self.__dict__.update({"req": None, "timestamp": None, "user": None, "cache": None})


req_ctx = __ReqCtx()
# END gevent local


def blaster_exit():
	# send exit signals
	# stage 0 -> stop creating new things
	# stage 1 -> initiate closing all connections, waiting things
	# ..
	# stage 5 -> all things cleanedup and done
	for i in range(6):
		events.broadcast_event("blaster_exit" + str(i))


def handle_signal(signum):
	# remove this signal handler and reraise
	blaster_exit()
	signal.signal(signum, signal.SIG_DFL)
	signal.raise_signal(signum)


# sigint event broadcast
signal_handler(signal.SIGINT, handle_signal, signal.SIGINT)
signal_handler(signal.SIGTERM, handle_signal, signal.SIGTERM)

# load default config, scan the stack and load
stack = inspect.stack()
for i in range(1, len(stack)):
	if(file := inspect.getfile(stack[i].frame)):
		if(file.startswith("/")):
			config.load(os.path.dirname(file))
			break
