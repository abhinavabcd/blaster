'''
Created on 29-Nov-2017

@author: abhinav
'''
from gevent import monkey
from gevent import local
monkey.patch_all()  # replaces read , write, send , sleep ...

# gevent local with some default
class __ReqCtx(local.local):
    def __init__(self, **kwargs):
        self.__dict__.update({"req": None, "timestamp": None, "client_name": None})
req_ctx = __ReqCtx()
# END gevent local


import sys
from .config import config
# override config
sys.modules["blaster.config"] = config

