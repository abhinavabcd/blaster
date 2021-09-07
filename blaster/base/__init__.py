import signal
import gevent
from ..utils import events

#.base is the main module
from .base import *


gevent.signal_handler(signal.SIGINT, lambda : events.broadcast_event("sigint"))
