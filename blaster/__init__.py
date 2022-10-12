'''
Created on 29-Nov-2017

@author: abhinav
'''
from gevent import monkey
monkey.patch_all()  # replaces read , write, send , sleep ...

import sys
from .config import config
# override config
sys.modules["blaster.config"] = config
