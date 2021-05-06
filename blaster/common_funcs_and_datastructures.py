'''
Created on 04-Nov-2017

@author: abhinav
'''

import os
import sys
import subprocess
import shlex
import collections
import random
import string
import socket
import struct
import gevent
import fcntl
import html

from functools import reduce as _reduce
from gevent.lock import BoundedSemaphore
from gevent.fileobject import FileObject
from gevent.socket import wait_read
from collections import namedtuple
from datetime import timezone
from datetime import datetime
import logging
import time
import hmac
import base64
import hashlib
from gevent.threading import Lock
import re
import six
from urllib.parse import urlencode

from queue import Queue
from threading import Thread
import traceback

from . import config
from .websocket._core import WebSocket
from .config import IS_DEV
from .utils.xss_html import XssHtml
from .utils.phone_number_utils import PhoneNumberObj


SOME_OBJ = object()

def cur_ms():
	return int(1000 * time.time())
#random id

#genetic LRU cache
class LRUCache:
	cache = None
	capacity = None

	def __init__(self, capacity, items=None):
		self.capacity = capacity
		self.cache = collections.OrderedDict(items or [])
		#in addition to recentness of use

	def exists(self, key):
		return self.cache.get(key, None)
		
	def get(self, key, default=None):
		try:
			#remove and reinsert into
			#ordered dict to move to recent
			value = self.cache.pop(key)
			self.cache[key] = value
			return value
		except KeyError:
			return default

	def set(self, key, value):
		removed_entries = []
		try:
			self.cache.pop(key)
		except KeyError:
			#new entry so cleanup space if it's beyond capacity
			while(len(self.cache) >= self.capacity):
				removed_entries.append(self.cache.popitem(last=False))
		self.cache[key] = value
		
		return removed_entries
		
	def delete(self, key):
		return self.cache.pop(key, None)
	
	def clear(self):
		return self.cache.clear()
	
	def to_son(self):
		ret = {}
		for key, val in self.cache:
			ret[key] = val.to_son() if hasattr(val, "to_son") else val
		return ret

class ExpiringCache:
	cache = None
	capacity = None
	ttl = None

	#ttl in millis, 5 minutes default
	def __init__(self, capacity, items=None, ttl=5 * 60 * 1000):
		self.capacity = capacity
		self.cache = collections.OrderedDict()
		self.ttl = ttl
		if(items):
			for k, v in items:
				self.set(k, v)

	def get(self, key, default=None):
		try:
			#remove and reinsert into
			#ordered dict to move to recent
			cur_timestamp = cur_ms()
			timestamp_and_value = self.cache.get(key)
			if(not timestamp_and_value):
				return default

			timestamp, value = timestamp_and_value
			if(cur_timestamp < timestamp + self.ttl):
				return value
			else:
				#remove if expired
				self.cache.pop(key, None)

			return default
		except KeyError:
			return default


	def set(self, key, value):
		removed_entries = []
		try:
			self.cache.pop(key)
		except KeyError:
			#new entry so cleanup space if it's beyond capacity
			cur_timestamp = cur_ms()
			while(len(self.cache) >= self.capacity):
				_key, (timestamp, _value) = self.cache.popitem(last=False)
				if(cur_timestamp < timestamp + self.ttl):
					removed_entries.append((_key, _value))

		self.cache[key] = (cur_ms(), value)
		
		return removed_entries

	def exists(self, key):
		return self.cache.get(key, SOME_OBJ) != SOME_OBJ
		
	def delete(self, key):
		return self.cache.pop(key, None)
	
	def clear(self):
		return self.cache.clear()
	
	def to_son(self):
		ret = {}
		cur_timestamp = cur_ms()
		for key, _val in self.cache.items():
			timestamp, val = _val
			if(cur_timestamp < timestamp + self.ttl):
				ret[key] = val.to_son() if hasattr(val, "to_son") else val
		return ret


#get SON of the fields from any generic python object
def to_son(obj):
	ret = obj.__dict__
	for k in ret.keys():
		if(ret[k] == None):
			del ret[k]
	return ret


def from_kwargs(cls, **kwargs):
	ret = cls()
	for key in kwargs:
		setattr(ret, key, kwargs[key])
	return ret

def get_by_key_list(d , *keyList):
	for key in keyList:
		if(not d):
			return None
		d = d.get(key, None)
	return d

def date2string(date):
	return date.isoformat()

def date2timestamp(dt):
	# datetime to timestamp
	if not isinstance(dt, datetime):
		return dt
	if(six.PY34):
		return dt.replace(tzinfo=timezone.utc).timestamp()
	else:
		timestamp = time.mktime(dt.timetuple()) + dt.microsecond / 1e6
		return timestamp

def timestamp2date(timestamp):
	if not isinstance(timestamp, (int, float)):
		return timestamp
	date = datetime.utcfromtimestamp(timestamp)
	return date


###############EPOCH
EPOCH = timestamp2date(0)

def find_index(a_list, value):
	try:
		return a_list.index(value)
	except ValueError:
		return -1

def find_nth(haystack, needle, n):
	''' Find position of nth occurance in a string'''
	if(not haystack):
		return -1
	start = haystack.find(needle)
	while start >= 0 and n > 1:
		start = haystack.find(needle, start + len(needle))
		n -= 1
	return start


SOME_TIME_WHEN_WE_STARTED_THIS = 1471504855
SOME_TIME_WHEN_WE_STARTED_THIS_MILLIS_WITH_10 = 16111506425808

### this function is inspired from instagram engineering post on generating 64bit keys with 12 bit shard_id inside it
# __thread_data = LRUCache(10000)
def generate_64bit_key(shard_id): # shard id should be a 12 bit number
	# may be later use dattime
	millis_elapsed = int((time.time() - SOME_TIME_WHEN_WE_STARTED_THIS) * 1000)
	
	_id = (((1 << 41) - 1) & millis_elapsed) << 23 # 41 bits , clear 22 places for random id and shard_id
	
	_id |= (((1 << 12) - 1) & shard_id) << 11 # 12 bit shard id, on top
	

	#increment per thread number
	# thread_id = threading.current_thread().ident
	# thread_data = __thread_data.get(thread_id, None)
	# if(not thread_data):
	#     thread_data = [0]
	#     __thread_data.set(thread_id , thread_data)
	# thread_data[0] = (thread_data[0] + 1)%(1 << 11)
	random_11_bits = random.randrange(0, 1 << 11)
	_id |= (((1 << 11) - 1) & random_11_bits) # clear 12 places for random

	#shard_id|timestmap|random_number
	
	return _id

def int_to_bytes(number: int) -> bytes:
	return number.to_bytes(length=(8 + (number + (number < 0)).bit_length()) // 8, byteorder='big', signed=True)


BASE_62_LIST = string.ascii_uppercase + string.digits + string.ascii_lowercase
BASE_62_DICT = dict((c, i) for i, c in enumerate(BASE_62_LIST))

def str_to_int(string, reverse_base=BASE_62_DICT):
	length = len(reverse_base)
	ret = 0
	for i, c in enumerate(string[::-1]):
		ret += (length ** i) * reverse_base[c]

	return ret

def int_to_str(integer, base=BASE_62_LIST):
	if integer == 0:
		return base[0]

	length = len(base)
	ret = ''
	while integer != 0:
		ret = base[integer % length] + ret
		integer /= length

	return ret

def get_int_id():
	return int(time.time() * 10000 - SOME_TIME_WHEN_WE_STARTED_THIS_MILLIS_WITH_10)

def get_str_id():
	return int_to_str(get_int_id())



##### protobuf + mysql utility functions
def list_to_protobuf(values, message):
	'''parse list to protobuf message'''
	if not values:
		return
	if isinstance(values[0], dict): # value needs to be further parsed
		for v in values:
			cmd = message.add()
			dict_to_protobuf(v, cmd)
	else: # value can be set
		message.extend(values)


#just like move constructor ;) , move values much faster than copies
def dict_to_protobuf(values, obj, transformations=None, excludes=None, preserve_values=True):
	if(not preserve_values):
		if(transformations):
			for k, func in transformations.items():
				values[k] = func(values.get(k, None))
				
		if(excludes):
			for exclude, flag in excludes.items():
				if(hasattr(values, exclude)):
					del values[exclude]
			
	for k, v in values.items():
		if(preserve_values):
			if(transformations and k in transformations):
				v = transformations[k](v)
							
			if(excludes and k in excludes):
				continue

		if hasattr(obj, k):
			if isinstance(v, dict): # value needs to be further parsed
				dict_to_protobuf(v, getattr(obj, k))
			elif isinstance(v, list):
				list_to_protobuf(v, getattr(obj, k))
			else: # value can be set
				if v: # otherwise default
					setattr(obj, k, v)

def get_mysql_rows_as_dict(res):
	
	rows_as_dict = []
	for row in res.rows:
		as_dict = {}
		for field, val in zip(res.fields, row):
			as_dict[field[0]] = val
		
		
		rows_as_dict.append(as_dict)
	
	return rows_as_dict

########################

def remove_duplicates(lst):
	exists = {}
	ret = []
	for i in lst:
		if(i in exists):
			continue
		exists[i] = True
		ret.append(i)
	return ret


def get_shard_id(_id):
	return (int(_id) & (((1 << 12) - 1) << 11)) >> 11
	
	
## move it to seperate module ?
## copied from internet sha1 token encode - decode module

if hasattr(hmac, 'compare_digest'):  # python 3.3
	_time_independent_equals = hmac.compare_digest
else:
	def _time_independent_equals(a, b):
		if len(a) != len(b):
			return False
		result = 0
		if isinstance(a[0], int):  # python3 byte strings
			for x, y in zip(a, b):
				result |= x ^ y
		else:  # python2
			for x, y in zip(a, b):
				result |= ord(x) ^ ord(y)
		return result == 0


def utf8(value) -> bytes:
	if(isinstance(value, bytes)):
		return value
	return value.encode("utf-8")

def create_signed_value(name, value, secret_key_version="v0", secret=None):
	timestamp = utf8(str(int(time.time())))
	value = base64.b64encode(utf8(value))
	secret = secret or config.secrets.get(secret_key_version)
	signature = _create_signature(secret, name, value, timestamp)
	value = b"|".join([value, timestamp, signature, utf8(secret_key_version)])
	return value

def decode_signed_value(name, value, max_age_days=-1, secret=None):
	if not value:
		return None
	parts = utf8(value).split(b"|")
	secret_key_version = b"v0"
	if len(parts) == 4:
		secret_key_version = parts[3]
	if(len(parts) < 3):
		return None
	secret = secret or config.secrets.get(secret_key_version.decode())
	signature = _create_signature(secret, name, parts[0], parts[1])
	if not _time_independent_equals(parts[2], signature):
		return None
	if(max_age_days > 0):
		timestamp = int(parts[1])
		if timestamp < time.time() - max_age_days * 86400:
			print(-1, cur_ms(), "Expired cookie %s"%value)
			return None
		if timestamp > time.time() + 31 * 86400:
			# _cookie_signature does not hash a delimiter between the
			# parts of the cookie, so an attacker could transfer trailing
			# digits from the payload to the timestamp without altering the
			# signature.  For backwards compatibility, sanity-check timestamp
			# here instead of modifying _cookie_signature.
			print(-1, cur_ms(), "Cookie timestamp in future; possible tampering %s"%value)
			return None
	if parts[1].startswith(b"0"):
		logging.warning("Tampered cookie %r", value)
		return None
	try:
		return base64.b64decode(parts[0])
	except Exception:
		return None


def _create_signature(secret, *parts):
	hash = hmac.new(utf8(secret), digestmod=hashlib.sha1)
	for part in parts:
		hash.update(utf8(part))
	return utf8(hash.hexdigest())


"""Dependencies are expressed as a dictionary whose keys are items
	and whose values are a set of dependent items. Output is a list of
	sets in topological order. The first set consists of items with no
	dependences, each subsequent set consists of items that depend upon
	items in the preceeding sets.
"""
def toposort(data):

	# Special case empty input.
	if len(data) == 0:
		return

	# Copy the input so as to leave it unmodified.
	data = data.copy()

	# Ignore self dependencies.
	for k, v in data.items():
		v.discard(k)
	# Find all items that don't depend on anything.
	extra_items_in_deps = _reduce(set.union, data.values()) - set(data.keys())
	# Add empty dependences where needed.
	data.update({item: set() for item in extra_items_in_deps})
	while True:
		ordered = set(item for item, dep in data.items() if len(dep) == 0)
		if not ordered:
			break
		yield ordered
		data = {item: (dep - ordered)
			for item, dep in data.items() if item not in ordered
		}
	if len(data) != 0:
		exception_string = 'Circular dependencies exist among these items: {{{}}}'.format(
				', '.join([
					'{!r}:{!r}'.format(key, value) for key, value in sorted(data.items())
				])
		)
		raise Exception(exception_string)




# Console or Cloud Console.
def get_random_id(length=10, include_timestamp=True):
	'''returns a 10 character random string containing numbers lowercase upper case'''
	'''http://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits-in-python'''

	key_str = ''.join(random.SystemRandom().choice(BASE_62_LIST) for _ in range(length)) 
	if(include_timestamp):
		key_str = key_str + ("%d" % time.time())
	#key_str = hashlib.md5(key_str).hexdigest()
	return key_str

# jump consistent hash function , jump_hash("Asdasd", 100) assigns to a bucket
def jump_hash(key, num_buckets):
	b, j = -1, 0
	key = int(hashlib.md5(key).hexdigest(), 16)
	while j < num_buckets:
		b = int(j)
		key = ((key * int(2862933555777941757)) + 1) & 0xFFFFFFFFFFFFFFFF
		j = float(b + 1) * (float(1 << 31) / float((key >> 33) + 1))
	return int(b)


number_regex = re.compile(r"([0-9\.]+)")
non_alpha_regex = re.compile("[^0-9a-zA-Z \.]", re.DOTALL)
def sanitize_string(text):
	return non_alpha_regex.sub("", text)


non_alpha_regex_2 = re.compile("[^0-9a-zA-Z]", re.DOTALL)
def sanitize_to_id(text):
	return non_alpha_regex_2.sub("", text.lower())


EMAIL_REGEX = re.compile('^[_a-z0-9-]+(\.[_a-z0-9-]+)*(\+[_a-z0-9-]+)?\@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$')
def is_valid_email(email):
	return EMAIL_REGEX.match(email)
	

SANITIZE_EMAIL_REGEX_NO_PLUS_ALLOWED = re.compile("\+[^@]*")
def sanitize_email_id(email_id, plus_allowed=True):
	if(not email_id):
		return None
	email_id = email_id.strip().lower()
	if(not EMAIL_REGEX.match(email_id)):
		return None

	if(not plus_allowed):
		email_id = SANITIZE_EMAIL_REGEX_NO_PLUS_ALLOWED.sub("", email_id)

	return email_id


PHONE_NUMBER_REGEX = re.compile("^\+?[0-9]+")
def sanitize_phone_number(phone_number, country_phone_code=None, iso2_country_code=None):
	if(not phone_number):
		return None
	phone_number = phone_number.replace(" ", "").strip().lower()
	if(not PHONE_NUMBER_REGEX.match(phone_number)):
		return None

	try:
		phone_number = PhoneNumberObj.parse_phone_number(
			phone_number,
			country_phone_code=country_phone_code,
			iso2_country_code=iso2_country_code
		).phone_number
	except Exception as ex:
		return None

	return phone_number


# first minus second
def list_diff(first, second):
	if(not first or not second):
		return first
	second = set(second)
	return [item for item in first if item not in second]

# what are added vs what need to be deleted from first
#no order
def list_diff2(first, second):
	if(not first and second):
		return [], second
	if(second == None):
		return first, []
	second = set(second)
	first = set(first)
	# to add and delete
	return [item for item in first if item not in second],\
		[item for item in second if item not in first]

# [TODO:] returns least operations to modify first-second
# hamiltonian distance
def list_diff3(first, second):
	if(not first and second):
		return [], second
	if(second == None):
		return first, []
	second = set(second)
	first = set(first)
	# to add and delete
	return [item for item in first if item not in second],\
		[item for item in second if item not in first]



# a dummy object with given keys,values
class DummyObject:
	entries = None

	def __init__(self, entries=None, **kwargs):
		self.entries = entries or {}
		self.entries.update(kwargs)

	def __setattr__(self, key, val):
		if(self.entries):
			self.entries[key] = val
		else:
			super().__setattr__(key, val)

	def __getattr__(self, key):
		return self.entries.get(key)

	def get(self, key, default=None):
		return self.entries.get(key, default)

	def __getitem__(self, key):
		return self.entries.get(key)

	def __setitem__(self, key, val):
		self.entries[key] = val

	def to_dict(self):
		return self.entries


# fully qualified name of the object
def object_name(o):
	# o.__module__ + "." + o.__class__.__qualname__ is an example in
	# this context of H.L. Mencken's "neat, plausible, and wrong."
	# Python makes no guarantees as to whether the __module__ special
	# attribute is defined, so we take a more circumspect approach.
	# Alas, the module name is explicitly excluded from __qualname__
	# in Python 3.

	module = o.__class__.__module__
	if module is None or module == str.__class__.__module__:
		return o.__class__.__name__  # Avoid reporting __builtin__
	else:
		return module + '.' + o.__class__.__name__


# normalize a int/str to n number of characters
def normalize(val, n=13):
	val = str(val)
	length = len(val)
	if(n >= length):
		return ("0" * (n - length)) + val

	raise Exception('cannot have greater length, ensure about this')

# normalize a int/str to n number of characters 
# and clips of any characters more than n characters
def normalize_no_warn(val, n=13):
	val = str(val)
	length = len(val)
	if(n >= length):
		return ("0" * (n - length)) + val
	return val



def ltrim(_str, str_to_remove):
	if(str_to_remove and _str.startswith(str_to_remove)):
		return _str[len(str_to_remove):]
	return _str

def rtrim(_str, str_to_remove):
	if(str_to_remove and _str.endswith(str_to_remove)):
		return _str[:len(_str) - len(str_to_remove)]
	return _str

def trim(_str, str_to_remove):
	return rtrim(ltrim(_str, str_to_remove), str_to_remove)


#remove zeros in the front
#returns 0 if it's all zeroes
def de_normalize(id1):
	i = 0
	length = len(id1)
	while(i < length):
		if(id1[i] != '0'):
			break
		i = i + 1
	if(i >= length):
		return '0'
	return id1[i:]

def set_non_blocking(fd):
	"""
	Set the file description of the given file descriptor to non-blocking.
	"""
	flags = fcntl.fcntl(fd, fcntl.F_GETFL)
	flags = flags | os.O_NONBLOCK
	fcntl.fcntl(fd, fcntl.F_SETFL, flags)


#generic thread pool to submit tasks which will be
#picked up wokers and processed
class Worker(Thread):
	""" Thread executing tasks from a given tasks queue """
	def __init__(self, tasks):
		Thread.__init__(self)
		self.tasks = tasks
		self.daemon = True
		self.start()

	def run(self):
		while True:
			func, args, kargs = self.tasks.get()
			try:
				func(*args, **kargs)
			except Exception as e:
				# An exception happened in this thread
				traceback.print_exc()
			finally:
				# Mark this task as done, whether an exception happened or not
				self.tasks.task_done()

class ThreadPool:
	""" Pool of threads consuming tasks from a queue """
	def __init__(self, num_threads):
		self.tasks = Queue(num_threads)
		for _ in range(num_threads):
			Worker(self.tasks)

	def add_task(self, func, *args, **kargs):
		""" Add a task to the queue """
		self.tasks.put((func, args, kargs))

	def map(self, func, args_list):
		""" Add a list of tasks to the queue """
		for args in args_list:
			self.add_task(func, args)

	def wait_completion(self):
		""" Wait for completion of all the tasks in the queue """
		self.tasks.join()

def make_xss_safe(_html):
	if(not _html):
		return _html
	parser = XssHtml()
	parser.feed(_html)
	parser.close()
	return parser.getHtml()

##### custom containers ##########
#SanitizedList and SanitizedDict are used for HTML safe operation
#the idea is to wrap them to sanitizeContainers while inserting
#rather than retrieving

class SanitizedSetterGetter(object):
	def __getitem__(self, k, escape_quotes=True, escape_html=True):
		val = super().__getitem__(k)
		if(isinstance(val, str)):
			if(escape_html):
				return html.escape(val, quote=escape_quotes)
		return val

	def __setitem__(self, key, val):
		if(isinstance(val, dict)):
			new_val = SanitizedDict()
			for k, v in val.items():
				new_val[k] = v # calls __setitem__ nested way
			super().__setitem__(key, new_val)

		elif(isinstance(val, list)):
			new_val = SanitizedList()
			for i in val:
				new_val.append(i)
			super().__setitem__(key, new_val)
		else:
			super().__setitem__(key, val)

class SanitizedList(SanitizedSetterGetter, list):

	def __iter__(self):
		#unoptimized but for this it's okay, always returns sanitized one
		def sanitized(val):
			if(isinstance(val, str)):
				return html.escape(val, quote=True)
			return val
		return map(sanitized, list.__iter__(self))

	def at(self, k, escape_quotes=True, escape_html=True):
		self.__getitem__(k,
			escape_quotes=escape_quotes,
			escape_html=escape_html
		)

	def extend(self, _list):
		for val in _list:
			#calls __setitem__ again
			self.append(val)
		#allow chaining
		return self

	def append(self, val):
		if(isinstance(val, dict)):
			new_val = SanitizedDict()
			for k, v in val.items():
				new_val[k] = v # calls __setitem__ nested way
			super().append(new_val)

		elif(isinstance(val, list)):
			new_val = SanitizedList()
			for i in val:
				new_val.append(i)
			super().append(new_val)
		else:
			super().append(val)
		#allow chaining
		return self

#intercepts all values setting and
class SanitizedDict(SanitizedSetterGetter, dict):
	#can pass escape_html=false if you want raw data
	def get(self, key, default=None, escape_html=True, escape_quotes=True):
		try:
			val = self.__getitem__(
				key,
				escape_html=escape_html,
				escape_quotes=escape_quotes
			)
			return val
		except KeyError:
			return default

	def items(self):
		#unoptimized but for this it's okay, always returns sanitized one
		def sanitized(key_val):
			key, val = key_val
			if(isinstance(val, str)):
				return (key, html.escape(val, quote=True))
			return key_val
		return map(sanitized, dict.items(self))

	def update(self, another):
		for k, v in another.items():
			#calls __setitem__ again
			self[k] = v
		#allow chaining
		return self


class LowerKeyDict(dict):
	def __getitem__(self, k):
		return super().__getitem__(k.lower())

	def get(self, k, default=None):
		return super().get(k.lower(), default)


# milli seconds
# TCP_USER_TIMEOUT kernel setting
_tcp_user_timeout = 30 * 1000

def set_socket_options(sock):
	l_onoff = 1
	l_linger = 10 # seconds,
	sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
				struct.pack('ii', l_onoff, l_linger))# close means a close understand ?
	
	
	
	timeval = struct.pack('ll', 100, 0)
	sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, timeval)

	TCP_USER_TIMEOUT = 18
	sock.setsockopt(socket.SOL_TCP, TCP_USER_TIMEOUT, _tcp_user_timeout) # close means a close understand ?

#wraps send method of websocket which keeps a buffer of messages for 20 seconds
#if the connection closes
class WebsocketConnection(WebSocket):
	queue = None# to keep track of how many greenlets are waiting on semaphore to send 
	msg_assumed_sent = None# queue for older sent messages in case of reset we try to retransmit
	ws = None
	lock = BoundedSemaphore()
	is_stale = False
	last_msg_recv_timestamp = None
	last_msg_sent_timestamp = None
	
	
	user_id = None
	is_viewer_list = False
	
	def __init__(self, ws, user_id):
		self.ws = ws
		self.queue = collections.deque()# to keep track of how many greenlets are waiting on semaphore to send
		self.msg_assumed_sent = collections.deque()# queue for older sent messages in case of reset we try to retransmit
		self.user_id = user_id

		self.last_msg_recv_timestamp = self.last_msg_sent_timestamp = time.time() * 1000
		
		
	def send(self, msg, ref=None, add_to_assumend_sent=True): # msg is only string data , #ref is used , just in case an exception occurs , we pass that ref 
		if(self.is_stale):
			raise Exception("stale connection")
		
		self.queue.append((ref, msg))
		if(self.lock.locked()):
			return
		
		self.lock.acquire()
		data_ref = None
		data = None
		try:
			while(not self.is_stale and len(self.queue) > 0):
				data_ref, data = self.queue.popleft() # peek
				self.ws.send(data) # msg objects only
				current_timestamp = time.time() * 1000
				self.last_msg_sent_timestamp = current_timestamp
				if(add_to_assumend_sent):
					while(len(self.msg_assumed_sent) > 0 and self.msg_assumed_sent[0][0] < current_timestamp - _tcp_user_timeout):
						#keep inly 100 seconds of previous data
						self.msg_assumed_sent.popleft()
					
					self.msg_assumed_sent.append((current_timestamp , data_ref, data))
				
		except Exception as ex:
			err_msg = "Exception while sending message to %s might be closed "%(self.user_id)
			self.is_stale = True
			raise Exception(err_msg)
			
		finally:
			self.lock.release()
		return
	
	
#upload related mime_types
TypeDescriptor = namedtuple('TypeDescriptor', ['mime_type'])
MIME_TYPE_MAP = dict(
	gif=TypeDescriptor(mime_type='image/gif'),
	webp=TypeDescriptor(mime_type='image/webp'),
	mp4=TypeDescriptor(mime_type='video/mp4'),

	jpeg=TypeDescriptor(mime_type='image/jpeg'),
	jpg=TypeDescriptor(mime_type='image/jpeg'),
	png=TypeDescriptor(mime_type='image/png'),
	json=TypeDescriptor(mime_type='application/json'),
	zip=TypeDescriptor(mime_type='application/zip'),
	mp3=TypeDescriptor(mime_type='audio/mpeg'),
	ogg=TypeDescriptor(mime_type='audio/ogg'),
	opus=TypeDescriptor(mime_type='audio/ogg'),
	wav=TypeDescriptor(mime_type='audio/wav'),
	m4a=TypeDescriptor(mime_type='audio/mp4'),
	aac=TypeDescriptor(mime_type='audio/aac'),
	docx=TypeDescriptor(mime_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'),
	pdf=TypeDescriptor(mime_type='application/pdf'),

	# variables should not start numerals
	gp=TypeDescriptor(mime_type='audio/3gp'),
	amr=TypeDescriptor(mime_type='audio/amr'),
	wma=TypeDescriptor(mime_type='audio/wma'),
	gpp=TypeDescriptor(mime_type='audio/3gpp'),
	flac=TypeDescriptor(mime_type='audio/flac'),
	
	css=TypeDescriptor(mime_type='text/css'),
	js=TypeDescriptor(mime_type='text/javascript')
	
)
#mime_type -> file extension map
MIME_TYPE_TO_EXTENSION = {v.mime_type: k for k, v in MIME_TYPE_MAP.items()}


def get_mime_type_from_filename(file_name):
	ext = os.path.splitext(file_name)[1][1:]
	return MIME_TYPE_MAP.get(ext, None)






# File handler for blaster server
def static_file_handler(_base_folder_path_, default_file_path="index.html", file_not_found_page_cb=None):
	cached_file_data = {}
	gevent_lock = Lock()

	def file_handler(sock, path, request_params=None, headers=None, post_data=None):
		if(not path):
			path = default_file_path
		#from given base_path
		path = _base_folder_path_ + str(path)
		file_data = cached_file_data.get(path, None)
		resp_headers = None

		if(not file_data or time.time() * 1000 - file_data[2] > 1000 if IS_DEV else 2 * 60 * 1000): # 1 millis
			gevent_lock.acquire()
			file_hash_key = path + urlencode(request_params._get)[:400]
			file_data = cached_file_data.get(file_hash_key, None)
			if(not file_data or time.time() * 1000 - file_data[2] > 1000): # 1 millis
				#put a lock here
				try:
					data = open(path, "rb").read()
					mime_type_headers = get_mime_type_from_filename(path)
					if(mime_type_headers):
						resp_headers = {
											'Content-Type': mime_type_headers.mime_type,
											'Cache-Control': 'max-age=31536000'
									}
					
					file_data = (data, resp_headers, time.time() * 1000)
					cached_file_data[file_hash_key] = file_data
				except Exception as ex:
					if(file_not_found_page_cb):
						file_data = (
										file_not_found_page_cb(path, request_params=None, headers=None, post_data=None),
										None,
										time.time() * 1000
									)
					else:
						file_data = ("--NO-FILE--", None, time.time() * 1000)
					
			gevent_lock.release()
					
		data, resp_headers, last_updated = file_data
		return resp_headers, data
	
	return file_handler

def run_shell(cmd, output_parser=None, shell=False, max_buf=5000):

	state = DummyObject()
	state.total_output = ""
	state.total_err = ""

	#keep parsing output
	def process_output(proc_out, proc_in):
		while(state.is_running):
			_out = proc_out.read(1)
			if(not _out):
				break
			_out = _out.decode('utf-8', 'ignore')
			#add to our input
			state.total_output += _out
			if(len(state.total_output) > 2 * max_buf):
				state.total_output = state.total_output[-max_buf:]

			if(output_parser):
				#parse the output and if it returns something
				#we write that to input file(generally stdin)
				_inp = output_parser(state.total_output, state.total_err)
				if(_inp):
					proc_in.write(_inp)
			else:
				print(_out, end="", flush=True)

	def process_error(proc_err, proc_in):
		while(state.is_running):
			_err = proc_err.read(1)
			if(not _err):
				break
			_err = _err.decode('utf-8', 'ignore')
			#add to our input
			state.total_err += _err
			if(len(state.total_err) > 2 * max_buf):
				state.total_err = state.total_err[-max_buf:]
			if(output_parser):
				#parse the output and if it returns something
				#we write that to input file(generally stdin)
				_inp = output_parser(state.total_output, state.total_err)
				if(_inp):
					proc_in.write(_inp)
			else:
				print(_err, end="", flush=True)

	if(isinstance(cmd, str) and not shell):
		cmd = shlex.split(cmd)

	dup_stdin = os.dup(sys.stdin.fileno()) if shell else subprocess.PIPE
	proc = subprocess.Popen(
		cmd,
		stdin=dup_stdin,
		stdout=subprocess.PIPE,
		stderr=subprocess.PIPE,
		shell=shell,
		env=os.environ.copy()
	)
	state.is_running = True

	#process output reader
	output_parser_thread = Thread(
		target=process_output,
		args=(
			proc.stdout,
			proc.stdin
		)
	)
	#process err reader
	err_parser_thread = Thread(
		target=process_error,
		args=(
			proc.stderr,
			proc.stdin
		)
	)
	output_parser_thread.start()
	err_parser_thread.start()

	os.close(dup_stdin) if dup_stdin != subprocess.PIPE else None

	#just keep printing error
	#wait for process to terminate
	ret_code = proc.wait()
	state.return_code = ret_code
	state.is_running = False
	
	output_parser_thread.join()
	err_parser_thread.join()
	return state
